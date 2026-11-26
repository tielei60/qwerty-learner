package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"qwerty-learner/sync-server/internal/broker"
	"qwerty-learner/sync-server/internal/config"
	"qwerty-learner/sync-server/internal/protocol"
	"qwerty-learner/sync-server/internal/store"

	"github.com/nats-io/nats.go"
)

func TestSyncStability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	start := func() (*nats.Conn, func()) {
		t.Helper()
		db, err := store.Open(filepath.Join(dir, "shared.db"))
		if err != nil {
			t.Fatal(err)
		}
		b, err := broker.Start(ctx, config.Config{NATSHost: "127.0.0.1", NATSPort: -1, NATSStoreDir: filepath.Join(dir, "jetstream")})
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		w := New(b, db)
		if err := w.Start(ctx); err != nil {
			b.Close()
			db.Close()
			t.Fatal(err)
		}
		nc, err := nats.Connect(b.ClientURL())
		if err != nil {
			w.Stop()
			b.Close()
			db.Close()
			t.Fatal(err)
		}
		return nc, func() { nc.Close(); w.Stop(); b.Close(); db.Close() }
	}
	nc, stop := start()
	defer func() {
		if stop != nil {
			stop()
		}
	}()
	snapshot := func(after int64) protocol.SnapshotResponse {
		t.Helper()
		raw, err := nc.RequestWithContext(ctx, "sync.req.snapshot.stability", []byte(fmt.Sprintf(`{"userId":"stability","afterSeq":%d}`, after)))
		if err != nil {
			t.Fatal(err)
		}
		var snap protocol.SnapshotResponse
		if err := json.Unmarshal(raw.Data, &snap); err != nil {
			t.Fatal(err)
		}
		return snap
	}
	publish := func(ev protocol.SyncEvent) {
		t.Helper()
		raw, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nc.RequestWithContext(ctx, "sync.up.stability", raw); err != nil {
			t.Fatal(err)
		}
	}
	await := func(seq int64) protocol.SnapshotResponse {
		t.Helper()
		for ctx.Err() == nil {
			if snap := snapshot(0); snap.LatestServerSeq >= seq {
				return snap
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("server did not reach sequence %d", seq)
		return protocol.SnapshotResponse{}
	}
	cp := func(index int) json.RawMessage {
		raw, err := json.Marshal(map[string]string{"key": "unfinishedPractice", "value": fmt.Sprintf(`{"dict":"cet4","chapter":0,"progress":{"chapterData":{"index":%d}}}`, index)})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	first := protocol.SyncEvent{EventID: "first", UserID: "stability", SourceDeviceID: "desktop", Entity: protocol.EntityConfig, Op: protocol.OpUpdate, SyncUID: "unfinishedPractice", Payload: cp(4), ClientTs: 100}
	publish(first)
	await(1)
	stale := first
	stale.EventID, stale.SourceDeviceID, stale.Payload, stale.ClientTs = "stale", "web", cp(2), 200
	publish(stale)
	latest := await(2)
	if len(latest.Records) != 1 || string(latest.Records[0].Payload) != string(cp(4)) {
		t.Fatalf("checkpoint regressed: %+v", latest)
	}
	publish(first)
	word := protocol.SyncEvent{EventID: "word", UserID: "stability", SourceDeviceID: "web", Entity: protocol.EntityWordRecords, Op: protocol.OpCreate, SyncUID: "word-1", Payload: json.RawMessage(`{"word":"hello"}`), ClientTs: 300}
	publish(word)
	await(3)
	if snap := snapshot(2); snap.LatestServerSeq != 3 || len(snap.Records) != 2 {
		t.Fatalf("incremental catch-up or duplicate handling failed: %+v", snap)
	}
	stop()
	stop = nil
	nc, stop = start()
	if snap := snapshot(0); snap.LatestServerSeq != 3 || len(snap.Records) != 2 {
		t.Fatalf("restart lost data: %+v", snap)
	}
	deleted := word
	deleted.EventID, deleted.Op, deleted.ClientTs = "delete", protocol.OpDelete, 400
	publish(deleted)
	await(4)
	t.Run("offline_delete", func(t *testing.T) {
		for _, rec := range snapshot(3).Records {
			if rec.SyncUID == word.SyncUID {
				return
			}
		}
		t.Fatal("catch-up omitted the deleted record, so the offline client cannot remove its local copy")
	})
}
