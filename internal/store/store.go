// Package store implements the blackboard itself: an in-memory key-value
// map with per-write versions, compare-and-swap conditions, TTL expiry,
// and prefix-scoped watchers.
//
// Versioning model: the store keeps one global, monotonically increasing
// sequence number. Every successful mutation (put, del, incr, expiry)
// consumes one sequence value; a live entry's Version is the sequence
// value of the write that produced it. Versions are therefore unique
// across the whole board and never repeat, which makes CAS immune to
// ABA problems (a key deleted and re-created never reuses a version).
//
// Time is injected via a now() func so every TTL behavior is testable
// without sleeping; callers that want wall-clock behavior pass nil.
package store

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Error codes surfaced to the wire protocol. Kept as plain strings so the
// server can map them 1:1 into responses.
const (
	CodeNotFound        = "not_found"
	CodeVersionConflict = "version_conflict"
	CodeExists          = "exists"
	CodeNotNumber       = "not_number"
)

// Error is a store-level failure with a stable machine-readable code.
// Current carries the key's current version for CAS conflicts (0 when the
// key does not exist), so clients can retry without a second round trip.
type Error struct {
	Code    string
	Message string
	Current uint64
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Cond expresses the optional preconditions of a write.
type Cond struct {
	// IfVersion, when non-nil, requires the key's current version to equal
	// this value. The special value 0 means "the key must not exist",
	// which makes create-only writes a plain CAS.
	IfVersion *uint64
	// IfAbsent requires the key to not exist (sugar for IfVersion=0 that
	// reports a distinct "exists" error code).
	IfAbsent bool
}

// EntryView is an immutable snapshot of one entry, safe to hold after the
// store lock is released.
type EntryView struct {
	Key     string
	Value   string
	Version uint64
	// TTL is the remaining time to live at snapshot time; HasTTL reports
	// whether the entry expires at all.
	TTL     time.Duration
	HasTTL  bool
	Created time.Time
	Updated time.Time
}

// Stats is a point-in-time summary of the board.
type Stats struct {
	Keys     int
	Seq      uint64
	Watchers int
	Puts     uint64
	Dels     uint64
	Expires  uint64
}

type entry struct {
	value    string
	version  uint64
	deadline time.Time // zero = never expires
	created  time.Time
	updated  time.Time
}

// Store is the blackboard. All methods are safe for concurrent use.
type Store struct {
	mu       sync.Mutex
	now      func() time.Time
	entries  map[string]*entry
	seq      uint64
	watchers map[int]*Watcher
	nextID   int

	puts    uint64
	dels    uint64
	expires uint64
}

// New returns an empty board. A nil now defaults to time.Now.
func New(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{
		now:      now,
		entries:  map[string]*entry{},
		watchers: map[int]*Watcher{},
	}
}

// liveLocked returns the entry for key if it exists and has not expired.
// An expired entry is removed on the spot and an expire event is emitted,
// so lazy expiry is observable by watchers even without a sweeper.
func (s *Store) liveLocked(key string, now time.Time) *entry {
	e, ok := s.entries[key]
	if !ok {
		return nil
	}
	if !e.deadline.IsZero() && !now.Before(e.deadline) {
		s.expireLocked(key, e)
		return nil
	}
	return e
}

func (s *Store) expireLocked(key string, e *entry) {
	delete(s.entries, key)
	s.seq++
	s.expires++
	s.emitLocked(Event{Seq: s.seq, Kind: EventExpire, Key: key, Version: e.version})
}

func (s *Store) viewLocked(key string, e *entry, now time.Time) EntryView {
	v := EntryView{
		Key:     key,
		Value:   e.value,
		Version: e.version,
		Created: e.created,
		Updated: e.updated,
	}
	if !e.deadline.IsZero() {
		v.HasTTL = true
		v.TTL = e.deadline.Sub(now)
	}
	return v
}

func checkCond(cond Cond, e *entry) *Error {
	if cond.IfAbsent && e != nil {
		return &Error{
			Code:    CodeExists,
			Message: "key already exists",
			Current: e.version,
		}
	}
	if cond.IfVersion != nil {
		var cur uint64
		if e != nil {
			cur = e.version
		}
		if *cond.IfVersion != cur {
			return &Error{
				Code:    CodeVersionConflict,
				Message: fmt.Sprintf("expected version %d, current is %d", *cond.IfVersion, cur),
				Current: cur,
			}
		}
	}
	return nil
}

// Put writes value under key. A zero ttl means the entry never expires;
// every put replaces the previous TTL entirely. On success the returned
// view carries the new version.
func (s *Store) Put(key, value string, ttl time.Duration, cond Cond) (EntryView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	e := s.liveLocked(key, now)
	if err := checkCond(cond, e); err != nil {
		return EntryView{}, err
	}
	s.seq++
	s.puts++
	if e == nil {
		e = &entry{created: now}
		s.entries[key] = e
	}
	e.value = value
	e.version = s.seq
	e.updated = now
	e.deadline = time.Time{}
	if ttl > 0 {
		e.deadline = now.Add(ttl)
	}
	s.emitLocked(Event{Seq: s.seq, Kind: EventPut, Key: key, Value: value, Version: e.version})
	return s.viewLocked(key, e, now), nil
}

// Get returns a snapshot of key, or a not_found error.
func (s *Store) Get(key string) (EntryView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	e := s.liveLocked(key, now)
	if e == nil {
		return EntryView{}, &Error{Code: CodeNotFound, Message: "no such key: " + key}
	}
	return s.viewLocked(key, e, now), nil
}

// Del removes key, honoring the same CAS conditions as Put. Deleting a
// missing (or expired) key reports not_found.
func (s *Store) Del(key string, cond Cond) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	e := s.liveLocked(key, now)
	if e == nil {
		return &Error{Code: CodeNotFound, Message: "no such key: " + key}
	}
	if err := checkCond(cond, e); err != nil {
		return err
	}
	delete(s.entries, key)
	s.seq++
	s.dels++
	s.emitLocked(Event{Seq: s.seq, Kind: EventDel, Key: key, Version: e.version})
	return nil
}

// Incr atomically adds by to the integer value stored at key. A missing
// key counts from zero. When setTTL is true the entry's TTL is replaced
// with ttl; otherwise an existing TTL is preserved. Values that do not
// parse as a base-10 int64 yield a not_number error.
func (s *Store) Incr(key string, by int64, ttl time.Duration, setTTL bool) (int64, EntryView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	e := s.liveLocked(key, now)
	var cur int64
	if e != nil {
		n, err := strconv.ParseInt(strings.TrimSpace(e.value), 10, 64)
		if err != nil {
			return 0, EntryView{}, &Error{
				Code:    CodeNotNumber,
				Message: fmt.Sprintf("value of %q is not an integer", key),
				Current: e.version,
			}
		}
		cur = n
	}
	next := cur + by
	s.seq++
	s.puts++
	if e == nil {
		e = &entry{created: now}
		s.entries[key] = e
	}
	e.value = strconv.FormatInt(next, 10)
	e.version = s.seq
	e.updated = now
	if setTTL {
		e.deadline = time.Time{}
		if ttl > 0 {
			e.deadline = now.Add(ttl)
		}
	}
	s.emitLocked(Event{Seq: s.seq, Kind: EventPut, Key: key, Value: e.value, Version: e.version})
	return next, s.viewLocked(key, e, now), nil
}

// List returns snapshots of every live entry whose key starts with prefix,
// sorted by key. Expired entries encountered along the way are removed
// (with expire events), so a listing never shows stale data.
func (s *Store) List(prefix string) []EntryView {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepLocked(now)
	views := make([]EntryView, 0, len(s.entries))
	for k, e := range s.entries {
		if strings.HasPrefix(k, prefix) {
			views = append(views, s.viewLocked(k, e, now))
		}
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Key < views[j].Key })
	return views
}

// Sweep removes every expired entry, emitting expire events in key order,
// and returns how many entries were removed. The server calls this on a
// timer so TTL expiry is observed promptly even for idle keys.
func (s *Store) Sweep() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sweepLocked(s.now())
}

func (s *Store) sweepLocked(now time.Time) int {
	var dead []string
	for k, e := range s.entries {
		if !e.deadline.IsZero() && !now.Before(e.deadline) {
			dead = append(dead, k)
		}
	}
	sort.Strings(dead) // deterministic event order
	for _, k := range dead {
		s.expireLocked(k, s.entries[k])
	}
	return len(dead)
}

// Stats reports live counters. The key count reflects only unexpired
// entries (expired ones are swept first).
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(s.now())
	return Stats{
		Keys:     len(s.entries),
		Seq:      s.seq,
		Watchers: len(s.watchers),
		Puts:     s.puts,
		Dels:     s.dels,
		Expires:  s.expires,
	}
}
