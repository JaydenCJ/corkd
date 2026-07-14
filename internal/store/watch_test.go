// Tests for watchers: event delivery, prefix scoping, state replay,
// atomic snapshot+subscribe, and slow-consumer drop. Delivery happens
// under the store lock, so every expectation here is deterministic —
// events are read from buffered channels that are already filled.
package store

import (
	"testing"
	"time"
)

// drain reads exactly n buffered events, failing fast if fewer arrived.
func drain(t *testing.T, w *Watcher, n int) []Event {
	t.Helper()
	evs := make([]Event, 0, n)
	for i := 0; i < n; i++ {
		select {
		case ev, ok := <-w.Events():
			if !ok {
				t.Fatalf("channel closed after %d of %d events", i, n)
			}
			evs = append(evs, ev)
		default:
			t.Fatalf("only %d of %d events buffered", i, n)
		}
	}
	return evs
}

func TestWatcherReceivesPutEvent(t *testing.T) {
	s, _ := newTestStore()
	w := s.Watch("", false, 0)
	defer w.Close()
	v := mustPut(t, s, "job/1", "queued", 0)
	ev := drain(t, w, 1)[0]
	if ev.Kind != EventPut || ev.Key != "job/1" || ev.Value != "queued" {
		t.Fatalf("event = %+v", ev)
	}
	if ev.Version != v.Version || ev.Seq != v.Version {
		t.Fatalf("event version/seq = %d/%d, want %d", ev.Version, ev.Seq, v.Version)
	}
}

func TestWatcherPrefixFiltersEvents(t *testing.T) {
	s, _ := newTestStore()
	w := s.Watch("job/", false, 0)
	defer w.Close()
	mustPut(t, s, "job/1", "a", 0)
	mustPut(t, s, "lock/x", "b", 0) // outside the prefix
	mustPut(t, s, "job/2", "c", 0)
	evs := drain(t, w, 2)
	if evs[0].Key != "job/1" || evs[1].Key != "job/2" {
		t.Fatalf("events = %+v", evs)
	}
	select {
	case ev := <-w.Events():
		t.Fatalf("unexpected extra event: %+v", ev)
	default:
	}
}

func TestWatcherSeesDelAndLazyExpireEvents(t *testing.T) {
	s, c := newTestStore()
	v := mustPut(t, s, "k", "v", 0)
	mustPut(t, s, "lease", "v", time.Second)
	w := s.Watch("", false, 0)
	defer w.Close()
	s.Del("k", Cond{})
	c.advance(2 * time.Second)
	s.Get("lease") // lazy expiry triggered by a read
	evs := drain(t, w, 2)
	if evs[0].Kind != EventDel || evs[0].Key != "k" {
		t.Fatalf("evs[0] = %+v", evs[0])
	}
	if evs[0].Version != v.Version {
		t.Fatalf("del event should carry last version %d, got %d", v.Version, evs[0].Version)
	}
	if evs[1].Kind != EventExpire || evs[1].Key != "lease" {
		t.Fatalf("evs[1] = %+v", evs[1])
	}
}

func TestSweepEmitsExpireEventsInKeyOrder(t *testing.T) {
	s, c := newTestStore()
	mustPut(t, s, "b-lease", "v", time.Second)
	mustPut(t, s, "a-lease", "v", time.Second)
	w := s.Watch("", false, 0)
	defer w.Close()
	c.advance(2 * time.Second)
	s.Sweep()
	evs := drain(t, w, 2)
	// Sorted emission keeps event streams reproducible run to run.
	if evs[0].Key != "a-lease" || evs[1].Key != "b-lease" {
		t.Fatalf("sweep order = %q, %q", evs[0].Key, evs[1].Key)
	}
}

func TestStateReplaySendsSnapshotThenSync(t *testing.T) {
	s, _ := newTestStore()
	mustPut(t, s, "job/b", "2", 0)
	va := mustPut(t, s, "job/a", "1", 0)
	mustPut(t, s, "lock/x", "out of prefix", 0)
	w := s.Watch("job/", true, 0)
	defer w.Close()
	evs := drain(t, w, 3)
	if evs[0].Key != "job/a" || evs[0].Kind != EventPut || evs[0].Value != "1" {
		t.Fatalf("evs[0] = %+v", evs[0])
	}
	if evs[1].Key != "job/b" {
		t.Fatalf("evs[1] = %+v", evs[1])
	}
	if evs[2].Kind != EventSync {
		t.Fatalf("evs[2] = %+v, want sync", evs[2])
	}
	if evs[2].Seq <= va.Version {
		t.Fatalf("sync seq = %d, want the board's current seq", evs[2].Seq)
	}
	// On an empty board the replay is just the sync marker at seq 0.
	s2, _ := newTestStore()
	w2 := s2.Watch("", true, 0)
	defer w2.Close()
	if ev := drain(t, w2, 1)[0]; ev.Kind != EventSync || ev.Seq != 0 {
		t.Fatalf("empty-board replay = %+v", ev)
	}
}

func TestLiveEventsContinueAfterSync(t *testing.T) {
	s, _ := newTestStore()
	mustPut(t, s, "k", "old", 0)
	w := s.Watch("", true, 0)
	defer w.Close()
	mustPut(t, s, "k", "new", 0)
	evs := drain(t, w, 3) // snapshot put, sync, live put
	if evs[2].Kind != EventPut || evs[2].Value != "new" {
		t.Fatalf("live event after sync = %+v", evs[2])
	}
	if evs[2].Seq <= evs[1].Seq {
		t.Fatalf("live seq %d not after sync seq %d", evs[2].Seq, evs[1].Seq)
	}
}

func TestMultipleWatchersEachReceiveEvents(t *testing.T) {
	s, _ := newTestStore()
	w1 := s.Watch("", false, 0)
	w2 := s.Watch("", false, 0)
	defer w1.Close()
	defer w2.Close()
	mustPut(t, s, "k", "v", 0)
	if ev := drain(t, w1, 1)[0]; ev.Key != "k" {
		t.Fatalf("w1 event = %+v", ev)
	}
	if ev := drain(t, w2, 1)[0]; ev.Key != "k" {
		t.Fatalf("w2 event = %+v", ev)
	}
}

func TestCloseUnsubscribesAndIsIdempotent(t *testing.T) {
	s, _ := newTestStore()
	w := s.Watch("", false, 0)
	if got := s.WatcherCount(); got != 1 {
		t.Fatalf("WatcherCount = %d, want 1", got)
	}
	w.Close()
	w.Close() // second close must not panic
	if got := s.WatcherCount(); got != 0 {
		t.Fatalf("WatcherCount after close = %d, want 0", got)
	}
	mustPut(t, s, "k", "v", 0) // must not panic on closed channel
}

func TestSlowWatcherIsDroppedFastWatcherSurvives(t *testing.T) {
	s, _ := newTestStore()
	slow := s.Watch("", false, 2)
	fast := s.Watch("", false, 3)
	defer fast.Close()
	mustPut(t, s, "k", "1", 0)
	mustPut(t, s, "k", "2", 0)
	mustPut(t, s, "k", "3", 0) // overflows slow (buffer 2), exactly fills fast
	if !slow.Lagged() {
		t.Fatal("slow watcher should be marked lagged")
	}
	if fast.Lagged() {
		t.Fatal("fast watcher dropped although its buffer was sufficient")
	}
	if got := s.WatcherCount(); got != 1 {
		t.Fatalf("WatcherCount = %d, want 1 after drop", got)
	}
	// The slow watcher keeps what it had buffered, then sees a close.
	evs := drain(t, slow, 2)
	if evs[0].Value != "1" || evs[1].Value != "2" {
		t.Fatalf("buffered events = %+v", evs)
	}
	if _, open := <-slow.Events(); open {
		t.Fatal("channel should be closed after drop")
	}
	slow.Close() // close after drop must not panic
	drain(t, fast, 3)
}

func TestEventSeqIsStrictlyIncreasing(t *testing.T) {
	s, c := newTestStore()
	w := s.Watch("", false, 0)
	defer w.Close()
	mustPut(t, s, "a", "1", 0)
	mustPut(t, s, "b", "2", time.Second)
	s.Incr("a", 1, 0, false)
	s.Del("a", Cond{})
	c.advance(2 * time.Second)
	s.Sweep()
	evs := drain(t, w, 5)
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq != evs[i-1].Seq+1 {
			t.Fatalf("seq gap: %d -> %d", evs[i-1].Seq, evs[i].Seq)
		}
	}
}

func TestFailedCASEmitsNoEvent(t *testing.T) {
	s, _ := newTestStore()
	mustPut(t, s, "k", "v", 0)
	w := s.Watch("", false, 0)
	defer w.Close()
	s.Put("k", "x", 0, Cond{IfAbsent: true})       // exists → rejected
	s.Put("k", "x", 0, Cond{IfVersion: uptr(999)}) // conflict → rejected
	select {
	case ev := <-w.Events():
		t.Fatalf("failed writes must not emit events, got %+v", ev)
	default:
	}
}

func TestGetAndWatchSnapshotsAtomically(t *testing.T) {
	s, _ := newTestStore()
	v0 := mustPut(t, s, "k", "current", 0)
	view, ok, w := s.GetAndWatch("k", 0)
	defer w.Close()
	if !ok || view.Value != "current" || view.Version != v0.Version {
		t.Fatalf("snapshot = %+v (ok=%v)", view, ok)
	}
	// Every mutation after the snapshot is observed — no gap.
	mustPut(t, s, "k", "next", 0)
	if ev := drain(t, w, 1)[0]; ev.Kind != EventPut || ev.Value != "next" {
		t.Fatalf("event = %+v", ev)
	}
	// A missing key reports absent but still subscribes.
	_, ok2, w2 := s.GetAndWatch("missing", 0)
	defer w2.Close()
	if ok2 {
		t.Fatal("ok = true for a missing key")
	}
	mustPut(t, s, "missing", "arrived", 0)
	if ev := drain(t, w2, 1)[0]; ev.Value != "arrived" {
		t.Fatalf("event = %+v", ev)
	}
}
