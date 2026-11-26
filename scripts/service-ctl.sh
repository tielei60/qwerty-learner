#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run"
SYNC_DIR="$ROOT_DIR/sync-server"
SYNC_BIN="$SYNC_DIR/sync-server"
SYNC_DATA_DIR="${SYNC_DATA_DIR:-$SYNC_DIR/data}"
SYNC_WS_PORT="${SYNC_WS_PORT:-9322}"
SYNC_AUTH_TOKEN="${SYNC_AUTH_TOKEN:-qwerty-dev-token}"
WEB_PORT="${WEB_PORT:-5173}"
TAURI_APP="$ROOT_DIR/src-tauri/target/release/bundle/macos/背单词.app"

mkdir -p "$RUN_DIR" "$SYNC_DATA_DIR"

port_open() {
  local port="$1"
  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$port" >/dev/null 2>&1
  else
    lsof -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1
  fi
}

stop_managed() {
  local pid_file="$1" executable="$2" pid command
  [ -f "$pid_file" ] || return 0
  pid="$(cat "$pid_file")"
  case "$pid" in ''|*[!0-9]*|0|1) return 1 ;; esac
  command="$(ps -p "$pid" -o command= || true)"
  if [ -n "$command" ]; then
    case "$command" in
      "$executable"|"$executable "*|*"node $executable preview "*) kill "$pid" ;;
      *) echo "PID $pid does not belong to this service" >&2; return 1 ;;
    esac
  fi
  rm -f "$pid_file"
}

start_sync() {
  if port_open "$SYNC_WS_PORT"; then
    echo "[sync-server] 已在运行 (ws://127.0.0.1:$SYNC_WS_PORT)"
    return 0
  fi
  if [ ! -x "$SYNC_BIN" ]; then
    echo "[sync-server] 未找到可执行文件 $SYNC_BIN，请先执行 make build-sync"
    return 1
  fi
  echo "[sync-server] 启动常驻同步服务 (ws://127.0.0.1:$SYNC_WS_PORT)..."
  SYNC_DATA_DIR="$SYNC_DATA_DIR" \
  SYNC_WS_PORT="$SYNC_WS_PORT" \
  SYNC_AUTH_TOKEN="$SYNC_AUTH_TOKEN" \
  nohup "$SYNC_BIN" >>"$RUN_DIR/sync-server.log" 2>&1 < /dev/null &
  echo $! > "$RUN_DIR/sync-server.pid"
  sleep 1
  port_open "$SYNC_WS_PORT" && echo "[sync-server] 启动成功 (pid $(cat "$RUN_DIR/sync-server.pid"))" || {
    echo "[sync-server] 启动异常，请查看日志: $RUN_DIR/sync-server.log"; return 1;
  }
}

stop_sync() {
  stop_managed "$RUN_DIR/sync-server.pid" "$SYNC_BIN"
}

start_web() {
  if port_open "$WEB_PORT"; then
    echo "[web] 已在运行 (http://127.0.0.1:$WEB_PORT)"
    return 0
  fi
  if [ ! -d "$ROOT_DIR/build" ]; then
    echo "[web] 未找到 build/，正在执行 yarn build..."
    (cd "$ROOT_DIR" && yarn build)
  fi
  echo "[web] 启动常驻网页端 (http://127.0.0.1:$WEB_PORT)..."
  (
    cd "$ROOT_DIR"
    nohup node "$ROOT_DIR/node_modules/vite/bin/vite.js" preview --host 127.0.0.1 --port "$WEB_PORT" --strictPort \
      >>"$RUN_DIR/web.log" 2>&1 < /dev/null &
    echo $! > "$RUN_DIR/web.pid"
  )
  sleep 1
  port_open "$WEB_PORT" && echo "[web] 启动成功 (pid $(cat "$RUN_DIR/web.pid")) -> http://127.0.0.1:$WEB_PORT" || {
    echo "[web] 启动异常，请查看日志: $RUN_DIR/web.log"; return 1;
  }
}

stop_web() {
  stop_managed "$RUN_DIR/web.pid" "$ROOT_DIR/node_modules/vite/bin/vite.js"
}

start_desktop() {
  if [ -d "$TAURI_APP" ]; then
    echo "[desktop] 启动 Tauri 客户端..."
    open "$TAURI_APP"
  elif [ -d "/Applications/背单词.app" ]; then
    echo "[desktop] 启动 /Applications/背单词.app ..."
    open "/Applications/背单词.app"
  else
    echo "[desktop] 尚未打包 .app，可执行 make build-desktop 打包，或 make dev-desktop 启动开发版"
  fi
}

show_status() {
  echo "=== qwerty-learner 服务状态 ==="
  port_open "$SYNC_WS_PORT" && echo "  sync-server : RUNNING (ws://127.0.0.1:$SYNC_WS_PORT)" || echo "  sync-server : STOPPED"
  port_open "$WEB_PORT"     && echo "  web (prod)  : RUNNING (http://127.0.0.1:$WEB_PORT)"    || echo "  web (prod)  : STOPPED"
  pgrep -f "/背单词.app/Contents/MacOS/背单词" >/dev/null 2>&1 && echo "  desktop app : RUNNING" || echo "  desktop app : STOPPED"
}

case "${1:-}" in
  start-sync)    start_sync ;;
  stop-sync)     stop_sync ;;
  start-web)     start_web ;;
  stop-web)      stop_web ;;
  start-desktop) start_desktop ;;
  start-all)     start_sync && start_web && start_desktop ;;
  stop-all)      stop_web && stop_sync ;;
  status)        show_status ;;
  logs)          tail -n 50 -f "$RUN_DIR/sync-server.log" "$RUN_DIR/web.log" ;;
  *) echo "Usage: $0 {start-sync|stop-sync|start-web|stop-web|start-desktop|start-all|stop-all|status|logs}"; exit 1 ;;
esac
