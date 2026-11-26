package broker

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"qwerty-learner/sync-server/internal/config"

	"github.com/nats-io/nats.go"
)

func TestWebsocketOrigins(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	b, err := Start(context.Background(), config.Config{
		NATSHost: "127.0.0.1", NATSPort: -1,
		WSHost: "127.0.0.1", WSPort: port,
		NATSStoreDir: filepath.Join(t.TempDir(), "jetstream"), AuthToken: "test-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, test := range []struct {
		origin string
		status int
	}{
		{"http://evil.example", http.StatusForbidden},
		{"http://localhost:5173.evil.example", http.StatusForbidden},
		{"null", http.StatusForbidden},
		{"http://127.0.0.1:5173", http.StatusSwitchingProtocols},
		{"tauri://localhost", http.StatusSwitchingProtocols},
		{"http://tauri.localhost", http.StatusSwitchingProtocols},
	} {
		t.Run(test.origin, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(2 * time.Second))
			fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nOrigin: %s\r\n\r\n", port, test.origin)
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != test.status {
				t.Fatalf("origin %q: status %d, want %d", test.origin, resp.StatusCode, test.status)
			}
		})
	}
	// Origin checks must not bypass NATS authentication or break native clients.
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d", port)
	if conn, err := nats.Connect(wsURL, nats.Token("wrong"), nats.NoReconnect()); err == nil {
		conn.Close()
		t.Fatal("accepted an invalid token")
	}
	conn, err := nats.Connect(wsURL, nats.Token("test-token"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
}
