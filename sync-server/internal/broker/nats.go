package broker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"qwerty-learner/sync-server/internal/config"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	StreamUp   = "SYNC_UP"
	StreamDown = "SYNC_DOWN"
)

type EmbeddedBroker struct {
	ns       *server.Server
	nc       *nats.Conn
	js       jetstream.JetStream
	wsServer *http.Server
}

func Start(ctx context.Context, cfg config.Config) (*EmbeddedBroker, error) {
	for _, host := range []string{cfg.NATSHost, cfg.WSHost} {
		if host == "" && cfg.WSPort == 0 {
			continue
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return nil, fmt.Errorf("sync listeners must bind to a loopback IP: %q", host)
		}
	}
	if err := os.MkdirAll(cfg.NATSStoreDir, 0o755); err != nil {
		return nil, fmt.Errorf("create nats store dir: %w", err)
	}

	opts := &server.Options{
		ServerName:    "qwerty-sync-nats",
		Host:          cfg.NATSHost,
		Port:          cfg.NATSPort,
		JetStream:     true,
		StoreDir:      cfg.NATSStoreDir,
		Authorization: cfg.AuthToken,
		Websocket: server.WebsocketOpts{
			Host:           cfg.WSHost,
			Port:           cfg.WSPort,
			NoTLS:          true,
			AllowedOrigins: []string{proxyOrigin},
		},
	}
	if cfg.WSPort != 0 {
		// NATS only supports HTTP origins. A local proxy also checks Tauri's
		// tauri://localhost origin before forwarding to this private listener.
		opts.Websocket.Port = -1
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("init embedded nats server: %w", err)
	}

	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		return nil, fmt.Errorf("embedded nats server failed to become ready in time")
	}

	nc, err := nats.Connect(ns.ClientURL(), nats.Token(cfg.AuthToken), nats.Name("sync-worker"))
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("connect internal nats client: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("init jetstream context: %w", err)
	}

	streams := []jetstream.StreamConfig{
		{
			Name:       StreamUp,
			Subjects:   []string{"sync.up.*"},
			Storage:    jetstream.FileStorage,
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     7 * 24 * time.Hour,
			Duplicates: 2 * time.Minute,
		},
		{
			Name:       StreamDown,
			Subjects:   []string{"sync.down.*"},
			Storage:    jetstream.FileStorage,
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     30 * 24 * time.Hour,
			Duplicates: 2 * time.Minute,
		},
	}
	for _, sc := range streams {
		if _, err := js.CreateOrUpdateStream(ctx, sc); err != nil {
			nc.Close()
			ns.Shutdown()
			return nil, fmt.Errorf("ensure stream %s: %w", sc.Name, err)
		}
	}

	b := &EmbeddedBroker{ns: ns, nc: nc, js: js}
	if cfg.WSPort != 0 {
		b.wsServer, err = startWebsocketProxy(cfg, ns.WebsocketURL())
		if err != nil {
			b.Close()
			return nil, err
		}
	}
	return b, nil
}

func (b *EmbeddedBroker) Conn() *nats.Conn               { return b.nc }
func (b *EmbeddedBroker) JetStream() jetstream.JetStream { return b.js }
func (b *EmbeddedBroker) ClientURL() string              { return b.ns.ClientURL() }

func (b *EmbeddedBroker) Close() {
	if b.wsServer != nil {
		_ = b.wsServer.Close()
	}
	if b.nc != nil {
		_ = b.nc.Drain()
	}
	if b.ns != nil {
		b.ns.Shutdown()
		b.ns.WaitForShutdown()
	}
}
