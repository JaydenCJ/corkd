#!/usr/bin/env bash
# mutex.sh — three workers race for one lock; exactly one wins.
#
# Demonstrates the corkd locking idiom:
#   acquire: set --if-absent --ttl 30s lock/<name> <owner>
#   release: del lock/<name>
#   queue:   wait --gone lock/<name>   (block until the holder releases)
# The TTL is the crash insurance: a worker that dies while holding the
# lock frees it automatically when the lease runs out.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d)"
SOCK="$WORKDIR/board.sock"
BIN="$WORKDIR/corkd"
SERVER_PID=""
cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

(cd "$ROOT" && go build -o "$BIN" ./cmd/corkd)
"$BIN" serve --socket "$SOCK" --quiet &
SERVER_PID=$!
for _ in $(seq 1 50); do
  "$BIN" ping --socket "$SOCK" >/dev/null 2>&1 && break
  sleep 0.1
done

B() { local c="$1"; shift; "$BIN" "$c" --socket "$SOCK" "$@"; }

echo "three workers race for lock/build ..."
for w in worker-1 worker-2 worker-3; do
  if B set --if-absent --ttl 30s lock/build "$w" >/dev/null 2>&1; then
    echo "  $w: acquired"
  else
    echo "  $w: busy (held by $(B get lock/build))"
  fi
done

HOLDER="$(B get lock/build)"
echo "winner: $HOLDER"

echo "a queued worker blocks on wait --gone ..."
(
  B wait --gone --timeout 10s lock/build >/dev/null
  B set --if-absent --ttl 30s lock/build worker-9 >/dev/null
  echo "  worker-9: lock was released, acquired it"
) &
QUEUED=$!

sleep 0.3
echo "  $HOLDER: done, releasing"
B del lock/build >/dev/null
wait "$QUEUED"

[ "$(B get lock/build)" = "worker-9" ] || { echo "handoff failed" >&2; exit 1; }
echo "handoff verified: lock/build -> worker-9"
