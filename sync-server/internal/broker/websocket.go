package broker

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"qwerty-learner/sync-server/internal/config"
)

const proxyOrigin = "http://qwerty-sync.internal"

func startWebsocketProxy(cfg config.Config, target string) (*http.Server, error) {
	origins := cfg.WSAllowedOrigins
	if len(origins) == 0 {
		origins = config.DefaultWSAllowedOrigins()
	}
	allowed := make(map[string]bool)
	for _, origin := range origins {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		u, err := url.ParseRequestURI(origin)
		if err != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" ||
			(u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "tauri") {
			return nil, fmt.Errorf("invalid allowed WebSocket origin: %q", origin)
		}
		allowed[origin] = true
	}
	if len(allowed) == 0 {
		return nil, fmt.Errorf("at least one allowed WebSocket origin is required")
	}
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	u.Scheme = "http"
	proxy := httputil.NewSingleHostReverseProxy(u)
	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				origin = r.Header.Get("Sec-WebSocket-Origin")
			}
			// Native clients without an Origin still authenticate with NATS.
			if origin != "" && !allowed[origin] {
				http.Error(w, "WebSocket origin is not allowed", http.StatusForbidden)
				return
			}
			r.Header.Set("Origin", proxyOrigin)
			proxy.ServeHTTP(w, r)
		}),
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(cfg.WSHost, strconv.Itoa(cfg.WSPort)))
	if err != nil {
		return nil, fmt.Errorf("listen for WebSocket connections: %w", err)
	}
	go func() { _ = server.Serve(listener) }()
	return server, nil
}
