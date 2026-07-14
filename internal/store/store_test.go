// Tests for the board core: versions, CAS conditions, TTL expiry, incr,
// listing, sweeping, and stats. Time is a hand-cranked fake clock, so
// every TTL case is exact — nothing here sleeps.
package store

import (
	"errors"
	"testing"
	"time"
)

// clock is a manually advanced time source.
type clock struct{ t time.Time }

func newClock() *clock {
	return &clock{t: time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)}
}

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestStore() (*Store, *clock) {
	c := newClock()
	return New(c.now), c
}

func mustPut(t *testing.T, s *Store, key, value string, ttl time.Duration) EntryView {
	t.Helper()
	v, err := s.Put(key, value, ttl, Cond{})
	if err != nil {
		t.Fatalf("Put(%q) failed: %v", key, err)
	}
	return v
}

func code(err error) string {
	var se *Error
	if errors.As(err, &se) {
		return se.Code
	}
	return ""
}

func current(err error) uint64 {
	var se *Error
	if errors.As(err, &se) {
		return se.Current
	}
	return 0
}

func uptr(n uint64) *uint64 { return &n }

func TestVersionsAreGlobalUniqueAndMonotonic(t *testing.T) {
	s, _ := newTestStore()
	if v := mustPut(t, s, "job/1", "queued", 0); v.Version != 1 {
		t.Fatalf("first write version = %d, want 1", v.Version)
	}
	mustPut(t, s, "b", "1", 0)
	// Versions are global: the third write is version 3 even though it is
	// only the second write to "job/1" — this is what defeats ABA in CAS.
	if v := mustPut(t, s, "job/1", "running", 0); v.Version != 3 {
		t.Fatalf("version = %d, want 3 (global sequence)", v.Version)
	}
	seen := map[uint64]bool{1: true, 2: true, 3: true}
	for _, k := range []string{"c", "b", "job/1"} {
		v := mustPut(t, s, k, "x", 0)
		if seen[v.Version] {
			t.Fatalf("version %d assigned twice", v.Version)
		}
		seen[v.Version] = true
	}
}

func TestGetReturnsSnapshotNotLiveState(t *testing.T) {
	s, _ := newTestStore()
	mustPut(t, s, "k", "v1", 0)
	v, err := s.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "k", "v2", 0)
	if v.Value != "v1" {
		t.Fatalf("snapshot mutated: %q", v.Value)
	}
}

func TestPutIfAbsentSemantics(t *testing.T) {
	s, _ := newTestStore()
	v, err := s.Put("lock", "agent-a", 0, Cond{IfAbsent: true})
	if err != nil {
		t.Fatalf("if-absent create failed: %v", err)
	}
	_, err = s.Put("lock", "agent-b", 0, Cond{IfAbsent: true})
	if code(err) != CodeExists {
		t.Fatalf("err = %v, want exists", err)
	}
	// The loser learns the holder's version without a second round trip.
	if current(err) != v.Version {
		t.Fatalf("Current = %d, want %d", current(err), v.Version)
	}
	if got, _ := s.Get("lock"); got.Value != "agent-a" {
		t.Fatalf("winner's value overwritten: %q", got.Value)
	}
}

func TestPutIfVersionCAS(t *testing.T) {
	s, _ := newTestStore()
	v1 := mustPut(t, s, "k", "old", 0)
	v2, err := s.Put("k", "new", 0, Cond{IfVersion: uptr(v1.Version)})
	if err != nil {
		t.Fatalf("CAS with matching version failed: %v", err)
	}
	if v2.Version <= v1.Version {
		t.Fatalf("version did not advance: %d -> %d", v1.Version, v2.Version)
	}
	// The stale writer must be refused and told the current version.
	_, err = s.Put("k", "stale write", 0, Cond{IfVersion: uptr(v1.Version)})
	if code(err) != CodeVersionConflict {
		t.Fatalf("err = %v, want version_conflict", err)
	}
	if current(err) != v2.Version {
		t.Fatalf("Current = %d, want %d", current(err), v2.Version)
	}
}

func TestPutIfVersionZeroMeansCreateOnly(t *testing.T) {
	s, _ := newTestStore()
	if _, err := s.Put("k", "v", 0, Cond{IfVersion: uptr(0)}); err != nil {
		t.Fatalf("if_version=0 on missing key should create: %v", err)
	}
	_, err := s.Put("k", "v2", 0, Cond{IfVersion: uptr(0)})
	if code(err) != CodeVersionConflict {
		t.Fatalf("if_version=0 on existing key: err = %v, want version_conflict", err)
	}
}

func TestGetAndDelOnMissingKeysReportNotFound(t *testing.T) {
	s, _ := newTestStore()
	if _, err := s.Get("nope"); code(err) != CodeNotFound {
		t.Fatalf("get missing: err = %v, want not_found", err)
	}
	if err := s.Del("nope", Cond{}); code(err) != CodeNotFound {
		t.Fatalf("del missing: err = %v, want not_found", err)
	}
	mustPut(t, s, "k", "v", 0)
	if err := s.Del("k", Cond{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("k"); code(err) != CodeNotFound {
		t.Fatalf("key still readable after del: %v", err)
	}
}

func TestDelIfVersionGuardsAgainstStaleDeletes(t *testing.T) {
	s, _ := newTestStore()
	v1 := mustPut(t, s, "k", "v1", 0)
	v2 := mustPut(t, s, "k", "v2", 0)
	if err := s.Del("k", Cond{IfVersion: uptr(v1.Version)}); code(err) != CodeVersionConflict {
		t.Fatalf("stale del: err = %v, want version_conflict", err)
	}
	if err := s.Del("k", Cond{IfVersion: uptr(v2.Version)}); err != nil {
		t.Fatalf("current del failed: %v", err)
	}
}

func TestTTLBoundaryIsExact(t *testing.T) {
	s, c := newTestStore()
	mustPut(t, s, "lease", "agent-a", 10*time.Second)
	c.advance(9999 * time.Millisecond)
	v, err := s.Get("lease")
	if err != nil {
		t.Fatalf("entry expired early: %v", err)
	}
	if !v.HasTTL || v.TTL != time.Millisecond {
		t.Fatalf("remaining TTL = %v (has=%v), want 1ms", v.TTL, v.HasTTL)
	}
	c.advance(time.Millisecond) // deadline is inclusive: now == deadline expires
	if _, err := s.Get("lease"); code(err) != CodeNotFound {
		t.Fatalf("err = %v, want not_found at deadline", err)
	}
}

func TestExpiredKeyIsAbsentForIfAbsent(t *testing.T) {
	s, c := newTestStore()
	mustPut(t, s, "lock", "agent-a", time.Second)
	c.advance(2 * time.Second)
	// The whole point of TTL locks: a crashed holder's lock frees itself.
	if _, err := s.Put("lock", "agent-b", time.Second, Cond{IfAbsent: true}); err != nil {
		t.Fatalf("take-over of expired lock failed: %v", err)
	}
}

func TestPutReplacesTTLEntirely(t *testing.T) {
	s, c := newTestStore()
	// A plain put clears a previous TTL: the entry becomes persistent.
	mustPut(t, s, "k", "v", time.Second)
	mustPut(t, s, "k", "v2", 0)
	c.advance(time.Hour)
	v, err := s.Get("k")
	if err != nil {
		t.Fatalf("entry expired despite TTL being cleared: %v", err)
	}
	if v.HasTTL {
		t.Fatal("HasTTL = true, want false")
	}
	// A put with a TTL restarts the clock from now (heartbeat refresh).
	mustPut(t, s, "hb", "v", time.Second)
	c.advance(900 * time.Millisecond)
	mustPut(t, s, "hb", "v", time.Second)
	c.advance(900 * time.Millisecond)
	if _, err := s.Get("hb"); err != nil {
		t.Fatalf("refreshed entry expired: %v", err)
	}
}

func TestIncrArithmetic(t *testing.T) {
	s, _ := newTestStore()
	// Missing key counts from zero.
	n, v, err := s.Incr("counter", 5, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 || v.Value != "5" {
		t.Fatalf("n = %d, value = %q, want 5", n, v.Value)
	}
	if n, _, _ = s.Incr("counter", 37, 0, false); n != 42 {
		t.Fatalf("n = %d, want 42", n)
	}
	// Negative deltas decrement (semaphore release).
	if n, _, _ = s.Incr("counter", -2, 0, false); n != 40 {
		t.Fatalf("n = %d, want 40", n)
	}
}

func TestIncrOnNonNumericValueFails(t *testing.T) {
	s, _ := newTestStore()
	mustPut(t, s, "k", "not a number", 0)
	_, _, err := s.Incr("k", 1, 0, false)
	if code(err) != CodeNotNumber {
		t.Fatalf("err = %v, want not_number", err)
	}
	if v, _ := s.Get("k"); v.Value != "not a number" {
		t.Fatalf("failed incr mutated the value: %q", v.Value)
	}
}

func TestIncrTTLHandling(t *testing.T) {
	s, c := newTestStore()
	// By default incr preserves the existing TTL...
	mustPut(t, s, "budget", "10", 10*time.Second)
	if _, _, err := s.Incr("budget", -1, 0, false); err != nil {
		t.Fatal(err)
	}
	c.advance(11 * time.Second)
	if _, err := s.Get("budget"); code(err) != CodeNotFound {
		t.Fatalf("TTL should survive incr: %v", err)
	}
	// ...but can replace it when asked.
	mustPut(t, s, "hb", "0", time.Second)
	if _, _, err := s.Incr("hb", 1, time.Minute, true); err != nil {
		t.Fatal(err)
	}
	c.advance(30 * time.Second)
	if _, err := s.Get("hb"); err != nil {
		t.Fatalf("replaced TTL not honored: %v", err)
	}
}

func TestListReturnsSortedPrefixMatches(t *testing.T) {
	s, _ := newTestStore()
	for _, k := range []string{"job/2", "lock/x", "job/1"} {
		mustPut(t, s, k, "v", 0)
	}
	views := s.List("job/")
	if len(views) != 2 || views[0].Key != "job/1" || views[1].Key != "job/2" {
		t.Fatalf("views = %+v", views)
	}
	if all := s.List(""); len(all) != 3 {
		t.Fatalf("empty prefix should match everything, got %d", len(all))
	}
}

func TestListSkipsExpiredEntries(t *testing.T) {
	s, c := newTestStore()
	mustPut(t, s, "stays", "v", 0)
	mustPut(t, s, "goes", "v", time.Second)
	c.advance(2 * time.Second)
	views := s.List("")
	if len(views) != 1 || views[0].Key != "stays" {
		t.Fatalf("views = %+v", views)
	}
}

func TestSweepRemovesOnlyExpiredEntries(t *testing.T) {
	s, c := newTestStore()
	mustPut(t, s, "a", "v", time.Second)
	mustPut(t, s, "b", "v", time.Minute)
	mustPut(t, s, "c", "v", 0)
	c.advance(5 * time.Second)
	if n := s.Sweep(); n != 1 {
		t.Fatalf("Sweep removed %d, want 1", n)
	}
	if _, err := s.Get("b"); err != nil {
		t.Fatalf("unexpired entry swept: %v", err)
	}
	if _, err := s.Get("c"); err != nil {
		t.Fatalf("persistent entry swept: %v", err)
	}
}

func TestValuesMayBeEmptyAndUnicode(t *testing.T) {
	s, _ := newTestStore()
	mustPut(t, s, "empty", "", 0)
	mustPut(t, s, "jp", "承認済み ✅", 0)
	if v, _ := s.Get("empty"); v.Value != "" {
		t.Fatalf("empty value round-trip: %q", v.Value)
	}
	if v, _ := s.Get("jp"); v.Value != "承認済み ✅" {
		t.Fatalf("unicode value round-trip: %q", v.Value)
	}
}

func TestCreatedAndUpdatedComeFromInjectedClock(t *testing.T) {
	s, c := newTestStore()
	t0 := c.t
	mustPut(t, s, "k", "v1", 0)
	c.advance(time.Minute)
	mustPut(t, s, "k", "v2", 0)
	v, _ := s.Get("k")
	if !v.Created.Equal(t0) {
		t.Fatalf("Created = %v, want %v", v.Created, t0)
	}
	if !v.Updated.Equal(t0.Add(time.Minute)) {
		t.Fatalf("Updated = %v, want %v", v.Updated, t0.Add(time.Minute))
	}
}

func TestStatsCountersTrackMutations(t *testing.T) {
	s, c := newTestStore()
	mustPut(t, s, "a", "1", 0)
	mustPut(t, s, "b", "1", time.Second)
	s.Incr("a", 1, 0, false)
	s.Del("a", Cond{})
	c.advance(2 * time.Second)
	st := s.Stats() // stats sweeps expired "b"
	if st.Keys != 0 {
		t.Fatalf("Keys = %d, want 0", st.Keys)
	}
	if st.Puts != 3 || st.Dels != 1 || st.Expires != 1 {
		t.Fatalf("counters = %+v", st)
	}
	if st.Seq != 5 { // 3 puts + 1 del + 1 expire
		t.Fatalf("Seq = %d, want 5", st.Seq)
	}
}

func TestReCreatedKeyNeverReusesAVersion(t *testing.T) {
	s, _ := newTestStore()
	v1 := mustPut(t, s, "k", "first life", 0)
	s.Del("k", Cond{})
	v2 := mustPut(t, s, "k", "second life", 0)
	if v2.Version <= v1.Version {
		t.Fatalf("re-created version %d not above %d — ABA hazard", v2.Version, v1.Version)
	}
	// A CAS against the first life must fail even though the key exists.
	if _, err := s.Put("k", "stale", 0, Cond{IfVersion: uptr(v1.Version)}); code(err) != CodeVersionConflict {
		t.Fatalf("stale CAS after re-create: %v", err)
	}
}
