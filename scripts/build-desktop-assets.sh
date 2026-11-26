#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
target="${CARGO_BUILD_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}"
case "$target" in
  aarch64-*) arch=arm64 ;;
  x86_64-*) arch=amd64 ;;
  *) echo "Unsupported sync sidecar architecture: $target" >&2; exit 1 ;;
esac
case "$target" in
  *-apple-darwin) os=darwin; extension='' ;;
  *-linux-*) os=linux; extension='' ;;
  *-windows-*) os=windows; extension=.exe ;;
  *) echo "Unsupported sync sidecar platform: $target" >&2; exit 1 ;;
esac
mkdir -p "$root/src-tauri/binaries"
(
  cd "$root/sync-server"
  GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build -ldflags="-s -w" \
    -o "$root/src-tauri/binaries/sync-server-$target$extension" ./cmd/sync-server
)
cd "$root"
yarn build
