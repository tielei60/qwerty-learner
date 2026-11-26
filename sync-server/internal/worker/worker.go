package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"qwerty-learner/sync-server/internal/broker"
	"qwerty-learner/sync-server/internal/protocol"
	"qwerty-learner/sync-server/internal/store"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Worker struct {
	broker  *broker.EmbeddedBroker
	store   *store.Store
	consume jetstream.ConsumeContext
	snapSub *nats.Subscription
}

func New(b *broker.EmbeddedBroker, s *store.Store) *Worker {
	return &Worker{broker: b, store: s}
}

func (w *Worker) Start(ctx context.Context) error {
	cons, err := w.broker.JetStream().CreateOrUpdateConsumer(ctx, broker.StreamUp, jetstream.ConsumerConfig{
		Durable:       "sync-worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    -1,
		FilterSubject: "sync.up.*",
	})
	if err != nil {
		return fmt.Errorf("create sync-worker consumer: %w", err)
	}

	cc, err := cons.Consume(w.handleUpMessage)
	if err != nil {
		return fmt.Errorf("start consuming SYNC_UP: %w", err)
	}
	w.consume = cc

	snapSub, err := w.broker.Conn().Subscribe("sync.req.snapshot.*", w.handleSnapshotRequest)
	if err != nil {
		w.consume.Stop()
		return fmt.Errorf("subscribe snapshot requests: %w", err)
	}
	w.snapSub = snapSub
	return nil
}

func (w *Worker) Stop() {
	if w.consume != nil {
		w.consume.Stop()
	}
	if w.snapSub != nil {
		_ = w.snapSub.Unsubscribe()
	}
}

func (w *Worker) handleUpMessage(msg jetstream.Msg) {
	var ev protocol.SyncEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		log.Printf("[worker] invalid JSON payload: %v", err)
		_ = msg.Term()
		return
	}
	if err := ev.Validate(); err != nil {
		log.Printf("[worker] invalid event %q: %v", ev.EventID, err)
		_ = msg.Term()
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := w.store.ApplyEvent(ctx, ev)
	if err != nil {
		log.Printf("[worker] sqlite apply failed for %q: %v", ev.EventID, err)
		_ = msg.NakWithDelay(time.Second)
		return
	}
	if res.Duplicate && !res.Pending {
		_ = msg.Ack()
		return
	}

	outBytes, err := json.Marshal(res.Event)
	if err != nil {
		_ = msg.Term()
		return
	}

	downSubject := "sync.down." + res.Event.UserID
	if _, err := w.broker.JetStream().Publish(ctx, downSubject, outBytes, jetstream.WithMsgID(res.Event.EventID)); err != nil {
		log.Printf("[worker] publish downstream %s failed: %v", downSubject, err)
		_ = msg.NakWithDelay(time.Second)
		return
	}

	if err := w.store.MarkBroadcast(ctx, res.Event.EventID); err != nil {
		_ = msg.NakWithDelay(time.Second)
		return
	}
	_ = msg.Ack()
	log.Printf("[worker] synced user=%s entity=%s op=%s seq=%d from=%s",
		res.Event.UserID, res.Event.Entity, res.Event.Op, res.Event.ServerSeq, res.Event.SourceDeviceID)
}

func (w *Worker) handleSnapshotRequest(msg *nats.Msg) {
	var req protocol.SnapshotRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil || req.UserID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	snap, err := w.store.GetSnapshot(ctx, req.UserID, req.AfterSeq, req.AfterRow)
	if err != nil {
		log.Printf("[worker] query snapshot failed: %v", err)
		return
	}
	out, err := json.Marshal(snap)
	if err == nil {
		err = msg.Respond(out)
	}
	if err != nil {
		log.Printf("[worker] reply to snapshot failed: %v", err)
	}
}
