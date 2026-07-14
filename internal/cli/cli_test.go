// End-to-end CLI tests: every command runs in-process against a real
// server on a temp socket, asserting on stdout/stderr text and exit
// codes — the same surface a shell script sees.
package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JaydenCJ/corkd/internal/proto"
	"github.com/JaydenCJ/corkd/internal/server"
	"github.com/JaydenCJ/corkd/internal/version"
)

// startBoard runs a server for the duration of the test and returns its
// socket path.
func startBoard(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "corkd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "b.sock")
	srv := server.New(server.Config{Socket: sock})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return sock
}

// run executes the CLI and captures both streams.
func run(args ...string) (code int, stdout, stderr string) {
	var out, errb bytes.Buffer
	code = Run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestVersionOutputs(t *testing.T) {
	for _, arg := range []string{"version", "--version", "-v"} {
		code, out, _ := run(arg)
		if code != ExitOK || out != "corkd "+version.Version+"\n" {
			t.Fatalf("%s: code = %d, out = %q", arg, code, out)
		}
	}
}

func TestHelpExitsZero(t *testing.T) {
	code, out, _ := run("help")
	if code != ExitOK || !strings.Contains(out, "corkd wait") {
		t.Fatalf("code = %d, out = %q", code, out)
	}
}

func TestUsageAndUnknownCommandsExit2(t *testing.T) {
	code, _, errb := run()
	if code != ExitUsage || !strings.Contains(errb, "Usage:") {
		t.Fatalf("no args: code = %d, stderr = %q", code, errb)
	}
	code, _, errb = run("frobnicate")
	if code != ExitUsage || !strings.Contains(errb, "unknown command") {
		t.Fatalf("unknown cmd: code = %d, stderr = %q", code, errb)
	}
	if code, _, _ := run("get", "--frob", "k"); code != ExitUsage {
		t.Fatalf("unknown flag: code = %d, want %d", code, ExitUsage)
	}
}

func TestSetThenGetTextOutput(t *testing.T) {
	sock := startBoard(t)
	code, out, _ := run("set", "--socket", sock, "build/status", "green")
	if code != ExitOK || out != "ok key=build/status version=1\n" {
		t.Fatalf("set: code = %d, out = %q", code, out)
	}
	code, out, _ = run("get", "--socket", sock, "build/status")
	if code != ExitOK || out != "green\n" {
		t.Fatalf("get: code = %d, out = %q", code, out)
	}
	// A TTL is reported back on the set line.
	_, out, _ = run("set", "--socket", sock, "--ttl", "30s", "lease", "me")
	if !strings.Contains(out, "ttl_ms=30000") {
		t.Fatalf("set --ttl out = %q", out)
	}
}

func TestGetJSONHasKeyValueVersion(t *testing.T) {
	sock := startBoard(t)
	run("set", "--socket", sock, "k", "v")
	code, out, _ := run("get", "--socket", sock, "--json", "k")
	if code != ExitOK {
		t.Fatalf("code = %d", code)
	}
	var resp proto.Response
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		t.Fatalf("output is not JSON: %q", out)
	}
	if !resp.OK || resp.Key != "k" || *resp.Value != "v" || resp.Version != 1 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestGetMissingKeyExits1(t *testing.T) {
	sock := startBoard(t)
	code, _, errb := run("get", "--socket", sock, "ghost")
	if code != ExitCond || !strings.Contains(errb, "not_found") {
		t.Fatalf("code = %d, stderr = %q", code, errb)
	}
}

func TestSetIfAbsentSecondWriterLoses(t *testing.T) {
	sock := startBoard(t)
	if code, _, _ := run("set", "--socket", sock, "--if-absent", "lock", "agent-a"); code != ExitOK {
		t.Fatalf("first if-absent set failed: %d", code)
	}
	code, _, errb := run("set", "--socket", sock, "--if-absent", "lock", "agent-b")
	if code != ExitCond || !strings.Contains(errb, "exists") {
		t.Fatalf("code = %d, stderr = %q", code, errb)
	}
	// The board still holds the winner's value.
	if _, out, _ := run("get", "--socket", sock, "lock"); out != "agent-a\n" {
		t.Fatalf("lock = %q", out)
	}
}

func TestSetIfVersionCAS(t *testing.T) {
	sock := startBoard(t)
	// 0 is create-only: works once, conflicts the second time.
	if code, _, _ := run("set", "--socket", sock, "--if-version", "0", "k", "v1"); code != ExitOK {
		t.Fatalf("create-only set on fresh key failed")
	}
	if code, _, _ := run("set", "--socket", sock, "--if-version", "0", "k", "again"); code != ExitCond {
		t.Fatalf("create-only set on existing key should exit %d", ExitCond)
	}
	run("set", "--socket", sock, "k", "v2")
	code, _, errb := run("set", "--socket", sock, "--if-version", "1", "k", "stale")
	if code != ExitCond || !strings.Contains(errb, "current is 2") {
		t.Fatalf("code = %d, stderr = %q", code, errb)
	}
}

func TestSetValueFromStdin(t *testing.T) {
	sock := startBoard(t)
	// Substitute stdin for the "-" value convention.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = old })
	w.WriteString("multi\nline payload")
	w.Close()
	if code, _, errb := run("set", "--socket", sock, "doc", "-"); code != ExitOK {
		t.Fatalf("code = %d, stderr = %q", code, errb)
	}
	_, out, _ := run("get", "--socket", sock, "doc")
	if out != "multi\nline payload\n" {
		t.Fatalf("round-trip = %q", out)
	}
}

func TestFlagValidationErrorsExit2(t *testing.T) {
	sock := startBoard(t)
	cases := [][]string{
		{"set", "--socket", sock, "--if-version", "banana", "k", "v"},
		{"set", "--socket", sock, "only-key"},
		{"set", "--socket", sock, "--ttl", "100us", "k", "v"},
		{"wait", "--socket", sock, "--equals", "x", "--gone", "k"},
		{"serve", "--sweep-interval", "-1s"},
		{"keys", "--socket", sock, "a", "b"},
	}
	for _, args := range cases {
		if code, _, _ := run(args...); code != ExitUsage {
			t.Fatalf("%v: code = %d, want %d", args, code, ExitUsage)
		}
	}
}

func TestDelSemantics(t *testing.T) {
	sock := startBoard(t)
	run("set", "--socket", sock, "k", "v")
	code, out, _ := run("del", "--socket", sock, "k")
	if code != ExitOK || out != "deleted key=k\n" {
		t.Fatalf("del: code = %d, out = %q", code, out)
	}
	if code, _, _ := run("get", "--socket", sock, "k"); code != ExitCond {
		t.Fatalf("get after del: code = %d", code)
	}
	if code, _, _ := run("del", "--socket", sock, "k"); code != ExitCond {
		t.Fatalf("del missing: code = %d, want %d", code, ExitCond)
	}
}

func TestIncrPrintsNewValue(t *testing.T) {
	sock := startBoard(t)
	if _, out, _ := run("incr", "--socket", sock, "n"); out != "1\n" {
		t.Fatalf("first incr = %q", out)
	}
	if _, out, _ := run("incr", "--socket", sock, "--by", "-3", "n"); out != "-2\n" {
		t.Fatalf("negative incr = %q", out)
	}
	run("set", "--socket", sock, "words", "sentence")
	code, _, errb := run("incr", "--socket", sock, "words")
	if code != ExitCond || !strings.Contains(errb, "not_number") {
		t.Fatalf("non-numeric incr: code = %d, stderr = %q", code, errb)
	}
}

func TestKeysListsSortedPrefixMatches(t *testing.T) {
	sock := startBoard(t)
	run("set", "--socket", sock, "job/2", "b")
	run("set", "--socket", sock, "job/1", "a")
	run("set", "--socket", sock, "lock", "x")
	_, out, _ := run("keys", "--socket", sock, "job/")
	if out != "job/1\njob/2\n" {
		t.Fatalf("keys = %q", out)
	}
}

func TestKeysJSONIsBareArray(t *testing.T) {
	sock := startBoard(t)
	// Empty board: an empty array, not null — jq-friendly.
	_, out, _ := run("keys", "--socket", sock, "--json")
	if strings.TrimSpace(out) != "[]" {
		t.Fatalf("empty board = %q, want []", out)
	}
	run("set", "--socket", sock, "k", "v")
	_, out, _ = run("keys", "--socket", sock, "--json")
	var arr []proto.KeyInfo
	if err := json.Unmarshal([]byte(out), &arr); err != nil {
		t.Fatalf("not a JSON array: %q", out)
	}
	if len(arr) != 1 || arr[0].Key != "k" {
		t.Fatalf("arr = %+v", arr)
	}
}

func TestDumpShowsValuesAndTTL(t *testing.T) {
	sock := startBoard(t)
	run("set", "--socket", sock, "--ttl", "1m", "lease", "held")
	_, out, _ := run("dump", "--socket", sock)
	if !strings.Contains(out, "lease") || !strings.Contains(out, "held") || !strings.Contains(out, "ttl=") {
		t.Fatalf("dump = %q", out)
	}
}

func TestWaitSatisfiedImmediatelyPrintsValue(t *testing.T) {
	sock := startBoard(t)
	run("set", "--socket", sock, "ready", "yes")
	code, out, _ := run("wait", "--socket", sock, "--timeout", "1ms", "ready")
	if code != ExitOK || out != "yes\n" {
		t.Fatalf("code = %d, out = %q", code, out)
	}
}

func TestWaitTimeoutAndGoneSemantics(t *testing.T) {
	sock := startBoard(t)
	code, _, errb := run("wait", "--socket", sock, "--timeout", "1ms", "never")
	if code != ExitCond || !strings.Contains(errb, "timeout") {
		t.Fatalf("timeout: code = %d, stderr = %q", code, errb)
	}
	// --gone on a key that was never set is satisfied immediately.
	code, out, _ := run("wait", "--socket", sock, "--gone", "--timeout", "1ms", "ghost")
	if code != ExitOK || out != "gone key=ghost\n" {
		t.Fatalf("gone: code = %d, out = %q", code, out)
	}
}

func TestWaitEqualsEmptyStringIsAValidCondition(t *testing.T) {
	sock := startBoard(t)
	run("set", "--socket", sock, "flag", "")
	code, out, _ := run("wait", "--socket", sock, "--equals", "", "--timeout", "1ms", "flag")
	if code != ExitOK || out != "\n" {
		t.Fatalf("code = %d, out = %q", code, out)
	}
}

func TestWatchCountStreamsStateThenExits(t *testing.T) {
	sock := startBoard(t)
	run("set", "--socket", sock, "job/1", "queued")
	run("set", "--socket", sock, "job/2", "running")
	code, out, _ := run("watch", "--socket", sock, "--state", "--count", "3", "job/")
	if code != ExitOK {
		t.Fatalf("code = %d", code)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines: %q", len(lines), out)
	}
	var evs []proto.Event
	for _, l := range lines {
		var ev proto.Event
		if err := json.Unmarshal([]byte(l), &ev); err != nil {
			t.Fatalf("line %q is not JSON", l)
		}
		evs = append(evs, ev)
	}
	if evs[0].Key != "job/1" || evs[1].Key != "job/2" || evs[2].Event != "sync" {
		t.Fatalf("events = %+v", evs)
	}
}

func TestStatsOutputs(t *testing.T) {
	sock := startBoard(t)
	run("set", "--socket", sock, "a", "1")
	code, out, _ := run("stats", "--socket", sock)
	if code != ExitOK || !strings.Contains(out, "keys=1") || !strings.Contains(out, "puts=1") {
		t.Fatalf("text: code = %d, out = %q", code, out)
	}
	_, out, _ = run("stats", "--socket", sock, "--json")
	var st proto.Stats
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		t.Fatalf("not JSON: %q", out)
	}
	if st.Keys != 1 || st.Puts != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestPingPrintsPong(t *testing.T) {
	sock := startBoard(t)
	code, out, _ := run("ping", "--socket", sock)
	if code != ExitOK || out != "pong corkd "+version.Version+"\n" {
		t.Fatalf("code = %d, out = %q", code, out)
	}
}

func TestNoServerExits3(t *testing.T) {
	dir, err := os.MkdirTemp("", "corkd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	code, _, errb := run("get", "--socket", filepath.Join(dir, "none.sock"), "k")
	if code != ExitRuntime || !strings.Contains(errb, "corkd serve") {
		t.Fatalf("code = %d, stderr = %q", code, errb)
	}
}

func TestServeRefusesBusySocketExits3(t *testing.T) {
	sock := startBoard(t)
	code, _, errb := run("serve", "--socket", sock, "--quiet")
	if code != ExitRuntime || !strings.Contains(errb, "already has a live server") {
		t.Fatalf("code = %d, stderr = %q", code, errb)
	}
}

func TestBadKeyOverWireExits3(t *testing.T) {
	// A key the server refuses (control character) is a bad_request,
	// which is a caller bug — distinct from an unmet condition.
	sock := startBoard(t)
	code, _, errb := run("get", "--socket", sock, "bad\tkey")
	if code != ExitRuntime || !strings.Contains(errb, "bad_request") {
		t.Fatalf("code = %d, stderr = %q", code, errb)
	}
}

func TestDefaultSocketResolutionOrder(t *testing.T) {
	t.Setenv("CORKD_SOCKET", "/tmp/x/custom.sock")
	t.Setenv("XDG_RUNTIME_DIR", "/tmp/run-x")
	if got := DefaultSocket(); got != "/tmp/x/custom.sock" {
		t.Fatalf("env override: DefaultSocket = %q", got)
	}
	t.Setenv("CORKD_SOCKET", "")
	if got := DefaultSocket(); got != filepath.Join("/tmp/run-x", "corkd.sock") {
		t.Fatalf("runtime dir: DefaultSocket = %q", got)
	}
	t.Setenv("XDG_RUNTIME_DIR", "")
	if got := DefaultSocket(); !strings.HasPrefix(filepath.Base(got), "corkd-") {
		t.Fatalf("uid fallback: DefaultSocket = %q", got)
	}
}

func TestWaitBlocksAcrossCLIInvocations(t *testing.T) {
	// Full CLI wait/set handshake: one goroutine runs `corkd wait`, the
	// main test writes once the waiter's watcher is registered.
	sock := startBoard(t)
	type res struct {
		code int
		out  string
	}
	done := make(chan res, 1)
	go func() {
		code, out, _ := run("wait", "--socket", sock, "--timeout", "60s", "go")
		done <- res{code, out}
	}()
	// The board reports a watcher as soon as the wait is armed.
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, out, _ := run("stats", "--socket", sock)
		if strings.Contains(out, "watchers=1") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never registered")
		}
		time.Sleep(time.Millisecond)
	}
	run("set", "--socket", sock, "go", "now")
	r := <-done
	if r.code != ExitOK || r.out != "now\n" {
		t.Fatalf("wait: code = %d, out = %q", r.code, r.out)
	}
}
