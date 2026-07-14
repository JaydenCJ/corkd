#!/usr/bin/env bash
# End-to-end smoke test for corkd: builds the binary, starts a real board
# on a temp socket, and drives the full CLI surface — set/get, CAS, TTL,
# incr, keys/dump, wait, watch, stats, shutdown. No network, idempotent,
# finishes in seconds.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d)"
SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

fail() {
  echo "SMOKE FAIL: $*" >&2
  exit 1
}

BIN="$WORKDIR/corkd"
SOCK="$WORKDIR/board.sock"
# B <cmd> [flags] [args…] — flags come before positional args (Go style).
B() {
  local cmd="$1"
  shift
  "$BIN" "$cmd" --socket "$SOCK" "$@"
}

echo "1. build"
(cd "$ROOT" && go build -o "$BIN" ./cmd/corkd) || fail "go build failed"

echo "2. version matches manifest"
"$BIN" version | grep -qx "corkd 0.1.0" || fail "version mismatch"

echo "3. serve on a temp socket"
"$BIN" serve --socket "$SOCK" --quiet &
SERVER_PID=$!
for _ in $(seq 1 50); do
  B ping >/dev/null 2>&1 && break
  sleep 0.1
done
B ping | grep -q "pong corkd 0.1.0" || fail "server did not come up"

echo "4. set/get round trip"
B set build/status green | grep -q "version=1" || fail "set output wrong"
[ "$(B get build/status)" = "green" ] || fail "get returned wrong value"

echo "5. CAS: if-absent lock, exactly one winner"
B set --if-absent lock agent-a >/dev/null || fail "first if-absent set failed"
if B set --if-absent lock agent-b 2>/dev/null; then
  fail "second if-absent set should exit 1"
fi
[ "$(B get lock)" = "agent-a" ] || fail "lock value clobbered"

echo "6. CAS: stale if-version is refused with the current version"
B set build/status yellow >/dev/null
set +e
CONFLICT="$(B set --if-version 1 build/status red 2>&1)"
CODE=$?
set -e
[ "$CODE" -eq 1 ] || fail "stale CAS should exit 1, got $CODE"
echo "$CONFLICT" | grep -q "version_conflict" || fail "conflict code missing"
echo "$CONFLICT" | grep -q "current is" || fail "conflict message lacks current version"
[ "$(B get build/status)" = "yellow" ] || fail "stale CAS mutated the value"

echo "7. incr is a counter"
[ "$(B incr done-count)" = "1" ] || fail "incr from zero"
[ "$(B incr --by 4 done-count)" = "5" ] || fail "incr by 4"

echo "8. keys and dump list sorted prefix matches"
B set job/2 running >/dev/null
B set job/1 done >/dev/null
[ "$(B keys job/)" = "$(printf 'job/1\njob/2')" ] || fail "keys listing wrong"
B dump job/ | grep -q "running" || fail "dump lacks values"

echo "9. wait blocks until another client writes"
(
  sleep 0.3
  B set go now >/dev/null
) &
WRITER_PID=$!
[ "$(B wait --timeout 10s go)" = "now" ] || fail "wait did not observe the set"
wait "$WRITER_PID"

echo "10. wait --timeout expires with exit code 1"
set +e
B wait --timeout 50ms never-set 2>/dev/null
CODE=$?
set -e
[ "$CODE" -eq 1 ] || fail "wait timeout should exit 1, got $CODE"

echo "11. watch --state replays the board as NDJSON"
WATCH="$(B watch --state --count 3 job/)"
echo "$WATCH" | head -1 | grep -q '"event":"put".*"key":"job/1"' || fail "watch replay wrong"
echo "$WATCH" | tail -1 | grep -q '"event":"sync"' || fail "watch sync marker missing"

echo "12. TTL entries expire"
B set --ttl 100ms lease held >/dev/null
B get lease >/dev/null || fail "lease should exist right after set"
sleep 0.4
if B get lease 2>/dev/null; then
  fail "lease should have expired"
fi

echo "13. stats reflect the session"
B stats | grep -q "expires=1" || fail "expiry not counted"

echo "14. usage errors exit 2"
set +e
B set only-a-key 2>/dev/null
CODE=$?
set -e
[ "$CODE" -eq 2 ] || fail "bad arg count should exit 2, got $CODE"

echo "15. clean shutdown removes the socket"
kill -TERM "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""
[ ! -e "$SOCK" ] || fail "socket file left behind after SIGTERM"

echo "SMOKE OK"
