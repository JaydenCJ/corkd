#!/usr/bin/env bash
# pipeline.sh — producer/consumer coordination without polling.
#
# The consumer arms `corkd wait` for each result key; the producer
# writes results as they finish. A prefix watch logs every board
# mutation as NDJSON while the pipeline runs — a free audit trail.
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

# Audit trail: record every task/* mutation while the pipeline runs.
B watch task/ > "$WORKDIR/audit.ndjson" &
WATCH_PID=$!

echo "producer: working through 3 tasks in the background ..."
(
  for i in 1 2 3; do
    sleep 0.2 # simulated work
    B set "task/$i" "result-$i" >/dev/null
    B incr tasks/completed >/dev/null
  done
) &
PRODUCER=$!

echo "consumer: blocking on each result as it is needed ..."
for i in 1 2 3; do
  RESULT="$(B wait --timeout 10s "task/$i")"
  echo "  consumed task/$i = $RESULT"
done
wait "$PRODUCER"

[ "$(B get tasks/completed)" = "3" ] || { echo "counter wrong" >&2; exit 1; }
echo "progress counter: $(B get tasks/completed)/3"

kill "$WATCH_PID" 2>/dev/null || true
wait "$WATCH_PID" 2>/dev/null || true
EVENTS="$(grep -c '"event":"put"' "$WORKDIR/audit.ndjson")"
echo "audit trail captured $EVENTS put events:"
sed 's/^/  /' "$WORKDIR/audit.ndjson"
