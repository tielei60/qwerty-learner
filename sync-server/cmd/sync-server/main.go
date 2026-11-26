package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"qwerty-learner/sync-server/internal/broker"
	"qwerty-learner/sync-server/internal/config"
	"qwerty-learner/sync-server/internal/store"
	"qwerty-learner/sync-server/internal/worker"
)

func main() {
	cfg := config.Load()

	st, err := store.Open(cfg.SQLitePath)
	if err != nil {
		log.Fatalf("open sqlite store: %v", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.Printf("close sqlite store: %v", err)
		}
	}()

	ctx := context.Background()
	brk, err := broker.Start(ctx, cfg)
	if err != nil {
		log.Fatalf("start embedded nats broker: %v", err)
	}
	defer brk.Close()

	w := worker.New(brk, st)
	if err := w.Start(ctx); err != nil {
		log.Fatalf("start sync worker: %v", err)
	}
	defer w.Stop()

	log.Printf("qwerty-learner sync-server started")
	log.Printf("  NATS TCP : %s", brk.ClientURL())
	log.Printf("  NATS WS  : ws://%s:%d", cfg.WSHost, cfg.WSPort)
	log.Printf("  SQLite   : %s", cfg.SQLitePath)
	log.Printf("  JetStream: %s", cfg.NATSStoreDir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received signal %s, shutting down...", sig)
}
