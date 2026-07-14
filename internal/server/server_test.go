// End-to-end protocol tests: a real server on a real unix socket in a
// temp dir, exercised through the client package. TTLs are driven by a
// fake clock, so nothing here depends on wall-clock timing.
package server

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/JaydenCJ/corkd/internal/client"
	"github.com/JaydenCJ/corkd/internal/proto"
	"github.com/JaydenCJ/corkd/internal/version"
)

// clock is a manually advanced, concurrency-safe time source.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// tempSocket returns a socket path short enough for the sun_path limit;
// t.TempDir can exceed it on deeply nested runners.
func tempSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "corkd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "b.sock")
}

// startServer boots a server (no sweeper — tests sweep explicitly) and
// tears it down with the test.
func startServer(t *testing.T) (*Server, *clock) {
	t.Helper()
	ck := &clock{t: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)}
	srv := New(Config{Socket: tempSocket(t), Now: ck.now})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close() })
	return srv, ck
}

func dialT(t *testing.T, srv *Server) *client.Client {
	t.Helper()
	c, err := client.Dial(srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func do(t *testing.T, c *client.Client, req proto.Request) proto.Response {
	t.Helper()
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do(%+v): %v", req, err)
	}
	return resp
}

func set(t *testing.T, c *client.Client, key, value string) proto.Response {
	t.Helper()
	resp := do(t, c, proto.Request{Op: proto.OpSet, Key: key, Value: &value})
	if !resp.OK {
		t.Fatalf("set %q failed: %+v", key, resp)
	}
	return resp
}

// waitForWatchers blocks until the board has n live watchers; used to
// order "waiter registered" before "value written" without guessing at a
// fixed delay.
func waitForWatchers(t *testing.T, srv *Server, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for srv.Store().WatcherCount() < n {
		if time.Now().After(deadline) {
			t.Fatalf("watchers never reached %d", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPingReportsServerVersion(t *testing.T) {
	srv, _ := startServer(t)
	resp := do(t, dialT(t, srv), proto.Request{Op: proto.OpPing})
	if !resp.OK || resp.Server != version.Version {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestSetGetRoundTripOverSocket(t *testing.T) {
	srv, _ := startServer(t)
	c := dialT(t, srv)
	set(t, c, "review/pr-42", "approved")
	resp := do(t, c, proto.Request{Op: proto.OpGet, Key: "review/pr-42"})
	if !resp.OK || *resp.Value != "approved" || resp.Version != 1 {
		t.Fatalf("resp = %+v", resp)
	}
	miss := do(t, c, proto.Request{Op: proto.OpGet, Key: "nope"})
	if miss.OK || miss.Error != "not_found" {
		t.Fatalf("miss = %+v", miss)
	}
}

func TestCASConflictOverWireCarriesCurrentVersion(t *testing.T) {
	srv, _ := startServer(t)
	c := dialT(t, srv)
	set(t, c, "k", "v1")
	v2 := set(t, c, "k", "v2")
	stale := uint64(1)
	resp := do(t, c, proto.Request{Op: proto.OpSet, Key: "k", Value: proto.StringPtr("x"), IfVersion: &stale})
	if resp.OK || resp.Error != "version_conflict" || resp.Version != v2.Version {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestIfAbsentOverWire(t *testing.T) {
	srv, _ := startServer(t)
	c := dialT(t, srv)
	r1 := do(t, c, proto.Request{Op: proto.OpSet, Key: "lock", Value: proto.StringPtr("a"), IfAbsent: true})
	r2 := do(t, c, proto.Request{Op: proto.OpSet, Key: "lock", Value: proto.StringPtr("b"), IfAbsent: true})
	if !r1.OK || r2.OK || r2.Error != "exists" {
		t.Fatalf("r1 = %+v, r2 = %+v", r1, r2)
	}
}

func TestProtocolErrorsKeepConnectionUsable(t *testing.T) {
	srv, _ := startServer(t)
	conn, err := net.Dial("unix", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	dec := json.NewDecoder(conn)
	// 1. Not JSON at all.
	if _, err := conn.Write([]byte("this is not json\n")); err != nil {
		t.Fatal(err)
	}
	var r1 proto.Response
	if err := dec.Decode(&r1); err != nil {
		t.Fatal(err)
	}
	if r1.OK || r1.Error != proto.CodeBadRequest {
		t.Fatalf("r1 = %+v", r1)
	}
	// 2. Valid JSON, unknown op.
	if _, err := conn.Write([]byte(`{"op":"shrug"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	var r2 proto.Response
	if err := dec.Decode(&r2); err != nil {
		t.Fatal(err)
	}
	if r2.OK || r2.Error != proto.CodeBadRequest {
		t.Fatalf("r2 = %+v", r2)
	}
	// 3. The same connection still serves valid requests.
	if _, err := conn.Write([]byte(`{"op":"ping"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	var r3 proto.Response
	if err := dec.Decode(&r3); err != nil {
		t.Fatal(err)
	}
	if !r3.OK {
		t.Fatalf("r3 = %+v", r3)
	}
}

func TestOversizedValueRejectedBeforeStore(t *testing.T) {
	srv, _ := startServer(t)
	c := dialT(t, srv)
	big := make([]byte, proto.DefaultLimits().MaxValueBytes+1)
	for i := range big {
		big[i] = 'x'
	}
	v := string(big)
	resp := do(t, c, proto.Request{Op: proto.OpSet, Key: "k", Value: &v})
	if resp.OK || resp.Error != proto.CodeBadRequest {
		t.Fatalf("resp = %+v", resp)
	}
	// Nothing was written.
	if r := do(t, c, proto.Request{Op: proto.OpGet, Key: "k"}); r.OK {
		t.Fatalf("key exists after rejected set: %+v", r)
	}
}

// A request line over the protocol limit cannot be parsed, so the
// connection must die — but with an actionable bad_request response
// first, not a bare EOF.
func TestOversizedRequestLineGetsBadRequestBeforeHangup(t *testing.T) {
	srv, _ := startServer(t)
	conn, err := net.Dial("unix", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	line := make([]byte, proto.MaxLineBytes+2)
	for i := range line {
		line[i] = 'a'
	}
	line[len(line)-1] = '\n'
	if _, err := conn.Write(line); err != nil {
		t.Fatalf("writing oversized line: %v", err)
	}
	var resp proto.Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("reading response: %v", err)
	}
	if resp.OK || resp.Error != proto.CodeBadRequest {
		t.Fatalf("want bad_request, got %+v", resp)
	}
}

func TestKeysAndDumpOverWire(t *testing.T) {
	srv, _ := startServer(t)
	c := dialT(t, srv)
	set(t, c, "job/2", "running")
	set(t, c, "job/1", "done")
	set(t, c, "other", "x")
	keys := do(t, c, proto.Request{Op: proto.OpKeys, Prefix: "job/"})
	if len(keys.Keys) != 2 || keys.Keys[0].Key != "job/1" || keys.Keys[0].Value != nil {
		t.Fatalf("keys = %+v", keys.Keys)
	}
	dump := do(t, c, proto.Request{Op: proto.OpDump, Prefix: "job/"})
	if len(dump.Keys) != 2 || dump.Keys[1].Value == nil || *dump.Keys[1].Value != "running" {
		t.Fatalf("dump = %+v", dump.Keys)
	}
}

func TestIncrOverWire(t *testing.T) {
	srv, _ := startServer(t)
	c := dialT(t, srv)
	by := int64(3)
	r1 := do(t, c, proto.Request{Op: proto.OpIncr, Key: "n", By: &by})
	r2 := do(t, c, proto.Request{Op: proto.OpIncr, Key: "n"}) // default by=1
	if !r1.OK || *r1.Num != 3 || !r2.OK || *r2.Num != 4 {
		t.Fatalf("r1 = %+v, r2 = %+v", r1, r2)
	}
	set(t, c, "words", "banana")
	r3 := do(t, c, proto.Request{Op: proto.OpIncr, Key: "words"})
	if r3.OK || r3.Error != "not_number" {
		t.Fatalf("r3 = %+v", r3)
	}
}

func TestTTLExpiryVisibleOverWire(t *testing.T) {
	srv, ck := startServer(t)
	c := dialT(t, srv)
	ttl := int64(5000)
	do(t, c, proto.Request{Op: proto.OpSet, Key: "lease", Value: proto.StringPtr("a"), TTLMS: &ttl})
	r1 := do(t, c, proto.Request{Op: proto.OpGet, Key: "lease"})
	if !r1.OK || r1.TTLMS == nil || *r1.TTLMS != 5000 {
		t.Fatalf("r1 = %+v", r1)
	}
	ck.advance(6 * time.Second)
	r2 := do(t, c, proto.Request{Op: proto.OpGet, Key: "lease"})
	if r2.OK || r2.Error != "not_found" {
		t.Fatalf("r2 = %+v", r2)
	}
}

func TestStatsOverWire(t *testing.T) {
	srv, _ := startServer(t)
	c := dialT(t, srv)
	set(t, c, "a", "1")
	set(t, c, "b", "2")
	do(t, c, proto.Request{Op: proto.OpDel, Key: "a"})
	resp := do(t, c, proto.Request{Op: proto.OpStats})
	if !resp.OK || resp.Stats == nil {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.Stats.Keys != 1 || resp.Stats.Puts != 2 || resp.Stats.Dels != 1 {
		t.Fatalf("stats = %+v", resp.Stats)
	}
}

func TestWatchStreamsLiveEvents(t *testing.T) {
	srv, _ := startServer(t)
	watcher := dialT(t, srv)
	if err := watcher.StartWatch("job/", false); err != nil {
		t.Fatal(err)
	}
	waitForWatchers(t, srv, 1)
	writer := dialT(t, srv)
	set(t, writer, "job/1", "queued")
	set(t, writer, "other", "ignored")
	do(t, writer, proto.Request{Op: proto.OpDel, Key: "job/1"})
	ev1, err := watcher.NextEvent()
	if err != nil {
		t.Fatal(err)
	}
	if ev1.Event != "put" || ev1.Key != "job/1" || *ev1.Value != "queued" {
		t.Fatalf("ev1 = %+v", ev1)
	}
	ev2, err := watcher.NextEvent()
	if err != nil {
		t.Fatal(err)
	}
	// The out-of-prefix write must have been filtered out.
	if ev2.Event != "del" || ev2.Key != "job/1" {
		t.Fatalf("ev2 = %+v", ev2)
	}
}

func TestWatchWithStateReplaysSnapshotThenSync(t *testing.T) {
	srv, _ := startServer(t)
	writer := dialT(t, srv)
	set(t, writer, "b", "2")
	set(t, writer, "a", "1")
	watcher := dialT(t, srv)
	if err := watcher.StartWatch("", true); err != nil {
		t.Fatal(err)
	}
	var kinds, keys []string
	for i := 0; i < 3; i++ {
		ev, err := watcher.NextEvent()
		if err != nil {
			t.Fatal(err)
		}
		kinds = append(kinds, ev.Event)
		keys = append(keys, ev.Key)
	}
	if kinds[0] != "put" || keys[0] != "a" || kinds[1] != "put" || keys[1] != "b" || kinds[2] != "sync" {
		t.Fatalf("replay = %v %v", kinds, keys)
	}
}

func TestWaitAlreadySatisfiedReturnsImmediately(t *testing.T) {
	srv, _ := startServer(t)
	c := dialT(t, srv)
	set(t, c, "ready", "yes")
	resp := do(t, c, proto.Request{Op: proto.OpWait, Key: "ready", TimeoutMS: proto.Int64Ptr(1)})
	// Timeout 1ms + immediate satisfaction: this can only pass if wait
	// consults current state before listening for changes.
	if !resp.OK || *resp.Value != "yes" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestWaitBlocksUntilPut(t *testing.T) {
	srv, _ := startServer(t)
	waiter := dialT(t, srv)
	type result struct {
		resp proto.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := waiter.Do(proto.Request{Op: proto.OpWait, Key: "go", TimeoutMS: proto.Int64Ptr(60000)})
		done <- result{resp, err}
	}()
	waitForWatchers(t, srv, 1) // the waiter is registered before we write
	set(t, dialT(t, srv), "go", "now")
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	if !r.resp.OK || *r.resp.Value != "now" {
		t.Fatalf("resp = %+v", r.resp)
	}
}

func TestWaitEqualsSkipsNonMatchingValues(t *testing.T) {
	srv, _ := startServer(t)
	waiter := dialT(t, srv)
	done := make(chan proto.Response, 1)
	go func() {
		resp, _ := waiter.Do(proto.Request{
			Op: proto.OpWait, Key: "phase", Equals: proto.StringPtr("done"),
			TimeoutMS: proto.Int64Ptr(60000),
		})
		done <- resp
	}()
	waitForWatchers(t, srv, 1)
	writer := dialT(t, srv)
	set(t, writer, "phase", "running") // must not satisfy the wait
	set(t, writer, "phase", "done")
	resp := <-done
	if !resp.OK || *resp.Value != "done" || resp.Version != 2 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestWaitGoneSemantics(t *testing.T) {
	srv, _ := startServer(t)
	writer := dialT(t, srv)
	// Missing key: satisfied immediately.
	r0 := do(t, writer, proto.Request{Op: proto.OpWait, Key: "never-set", Gone: true, TimeoutMS: proto.Int64Ptr(1)})
	if !r0.OK {
		t.Fatalf("r0 = %+v", r0)
	}
	// Held key: satisfied by the delete.
	set(t, writer, "lock", "held")
	waiter := dialT(t, srv)
	done := make(chan proto.Response, 1)
	go func() {
		resp, _ := waiter.Do(proto.Request{Op: proto.OpWait, Key: "lock", Gone: true, TimeoutMS: proto.Int64Ptr(60000)})
		done <- resp
	}()
	waitForWatchers(t, srv, 1)
	do(t, writer, proto.Request{Op: proto.OpDel, Key: "lock"})
	if resp := <-done; !resp.OK {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestWaitTimesOut(t *testing.T) {
	srv, _ := startServer(t)
	resp := do(t, dialT(t, srv), proto.Request{Op: proto.OpWait, Key: "never", TimeoutMS: proto.Int64Ptr(1)})
	if resp.OK || resp.Error != proto.CodeTimeout {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestWaitIgnoresSiblingKeysSharingThePrefix(t *testing.T) {
	srv, _ := startServer(t)
	waiter := dialT(t, srv)
	done := make(chan proto.Response, 1)
	go func() {
		resp, _ := waiter.Do(proto.Request{Op: proto.OpWait, Key: "job/1", TimeoutMS: proto.Int64Ptr(60000)})
		done <- resp
	}()
	waitForWatchers(t, srv, 1)
	writer := dialT(t, srv)
	set(t, writer, "job/10", "red herring") // prefix-matches "job/1" internally
	set(t, writer, "job/1", "real")
	resp := <-done
	if !resp.OK || *resp.Value != "real" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestConcurrentIncrIsAtomic(t *testing.T) {
	srv, _ := startServer(t)
	const workers, perWorker = 8, 25
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := client.Dial(srv.Addr())
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			for j := 0; j < perWorker; j++ {
				if _, err := c.Do(proto.Request{Op: proto.OpIncr, Key: "n"}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	resp := do(t, dialT(t, srv), proto.Request{Op: proto.OpGet, Key: "n"})
	if *resp.Value != "200" {
		t.Fatalf("final counter = %q, want 200", *resp.Value)
	}
}

func TestListenHandlesStaleAndLiveSockets(t *testing.T) {
	// A crash leftover — a socket file with nothing behind it — is
	// silently taken over.
	sock := tempSocket(t)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	ln.Close()
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("stale socket file missing: %v", err)
	}
	srv := New(Config{Socket: sock})
	if err := srv.Listen(); err != nil {
		t.Fatalf("takeover of stale socket failed: %v", err)
	}
	go srv.Serve()
	defer srv.Close()
	// A socket with a live server must be refused — two boards on one
	// path would split the agents' shared memory.
	second := New(Config{Socket: sock})
	if err := second.Listen(); err == nil {
		second.Close()
		t.Fatal("second server bound the same live socket")
	}
	// The live server is unharmed by the failed takeover.
	c, err := client.Dial(sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if resp := do(t, c, proto.Request{Op: proto.OpPing}); !resp.OK {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestSocketPermsAndCleanupOnClose(t *testing.T) {
	sock := tempSocket(t)
	srv := New(Config{Socket: sock})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perm = %o, want 600", perm)
	}
	srv.Close()
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Fatalf("socket file survives Close: %v", err)
	}
}
