#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SYNC_DIR="$ROOT_DIR/sync-server"
BIN_PATH="$SYNC_DIR/sync-server"
DATA_DIR="${SYNC_DATA_DIR:-$SYNC_DIR/data}"
WS_PORT="${SYNC_WS_PORT:-9322}"
LOG_FILE="$DATA_DIR/sync-server.log"
PID_FILE="$DATA_DIR/sync-server.pid"

# 1. If port is already listening, reuse the running sync-server instance
if command -v nc >/dev/null 2>&1 && nc -z 127.0.0.1 "$WS_PORT" >/dev/null 2>&1; then
  echo "[sync-server] already listening on ws://127.0.0.1:$WS_PORT"
  exit 0
fi

mkdir -p "$DATA_DIR"

# 2. Build binary automatically if missing and Go is available
if [ ! -x "$BIN_PATH" ]; then
  if command -v go >/dev/null 2>&1; then
    echo "[sync-server] building single binary..."
    (cd "$SYNC_DIR" && go mod tidy && CGO_ENABLED=0 go build -ldflags="-s -w" -o "$BIN_PATH" ./cmd/sync-server)
    if [ $? -ne 0 ]; then
      echo "[sync-server] build failed (check sync-server/internal/broker/nats.go and worker/worker.go); continuing frontend startup."
      exit 0
    fi
  else
    echo "[sync-server] binary not found and 'go' is not in PATH; skipping auto-start."
    exit 0
  fi
fi

# 3. Start sync-server in background
echo "[sync-server] starting on ws://127.0.0.1:$WS_PORT ..."
SYNC_DATA_DIR="$DATA_DIR" \
SYNC_WS_PORT="$WS_PORT" \
SYNC_AUTH_TOKEN="${SYNC_AUTH_TOKEN:-qwerty-dev-token}" \
nohup "$BIN_PATH" >>"$LOG_FILE" 2>&1 &

echo $! > "$PID_FILE"
echo "[sync-server] started (pid $!, log: $LOG_FILE)"
exit 0
