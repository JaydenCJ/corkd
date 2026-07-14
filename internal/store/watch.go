// Watcher registry: prefix-scoped event subscriptions with bounded
// buffers. Delivery happens synchronously under the store lock, so a
// watcher created before a mutation is guaranteed to observe it — there
// is no window in which an event can be missed. Slow consumers are
// disconnected (marked lagged) instead of blocking the board.
package store

import (
	"sort"
	"strings"
)

// EventKind classifies a board mutation.
type EventKind string

const (
	// EventPut is a create or update (including incr).
	EventPut EventKind = "put"
	// EventDel is an explicit deletion.
	EventDel EventKind = "del"
	// EventExpire is a TTL-driven removal (lazy or swept).
	EventExpire EventKind = "expire"
	// EventSync marks the end of a state replay: everything before it is
	// a snapshot, everything after it is live.
	EventSync EventKind = "sync"
)

// Event is one observed mutation. Seq is the global sequence value the
// mutation consumed (for EventSync, the board's sequence at snapshot
// time). Value and Version are set for EventPut; Version also carries the
// removed entry's last version for EventDel and EventExpire.
type Event struct {
	Seq     uint64
	Kind    EventKind
	Key     string
	Value   string
	Version uint64
}

// Watcher is a live subscription. Read events from Events(); the channel
// is closed when the watcher is closed or dropped for lagging.
type Watcher struct {
	id     int
	prefix string
	ch     chan Event
	s      *Store
	lagged bool
}

// DefaultWatchBuffer is the per-watcher event buffer used when callers
// pass a non-positive size.
const DefaultWatchBuffer = 256

// Watch subscribes to every mutation of keys starting with prefix (the
// empty prefix matches everything). When withState is true the channel is
// pre-loaded with one put event per matching live entry (sorted by key)
// followed by a sync event, then live events follow — snapshot and
// subscription are atomic, so no mutation can fall in between.
func (s *Store) Watch(prefix string, withState bool, buffer int) *Watcher {
	if buffer <= 0 {
		buffer = DefaultWatchBuffer
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var state []EntryView
	if withState {
		now := s.now()
		s.sweepLocked(now)
		for k, e := range s.entries {
			if strings.HasPrefix(k, prefix) {
				state = append(state, s.viewLocked(k, e, now))
			}
		}
		sort.Slice(state, func(i, j int) bool { return state[i].Key < state[j].Key })
	}
	// Reserve room for the snapshot (plus its sync marker) on top of the
	// live buffer so the replay itself can never overflow the watcher.
	reserve := 0
	if withState {
		reserve = len(state) + 1
	}
	s.nextID++
	w := &Watcher{
		id:     s.nextID,
		prefix: prefix,
		ch:     make(chan Event, buffer+reserve),
		s:      s,
	}
	for _, v := range state {
		w.ch <- Event{Seq: v.Version, Kind: EventPut, Key: v.Key, Value: v.Value, Version: v.Version}
	}
	if withState {
		w.ch <- Event{Seq: s.seq, Kind: EventSync}
	}
	s.watchers[w.id] = w
	return w
}

// GetAndWatch atomically snapshots key and subscribes to its mutations,
// which is the primitive blocking waits are built on: the caller checks
// the snapshot first and, if unsatisfied, consumes events knowing none
// were missed in between. The returned watcher uses key as its prefix, so
// callers must still filter events for the exact key.
func (s *Store) GetAndWatch(key string, buffer int) (EntryView, bool, *Watcher) {
	if buffer <= 0 {
		buffer = DefaultWatchBuffer
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var view EntryView
	var ok bool
	if e := s.liveLocked(key, now); e != nil {
		view = s.viewLocked(key, e, now)
		ok = true
	}
	s.nextID++
	w := &Watcher{id: s.nextID, prefix: key, ch: make(chan Event, buffer), s: s}
	s.watchers[w.id] = w
	return view, ok, w
}

// Events returns the subscription channel.
func (w *Watcher) Events() <-chan Event { return w.ch }

// Lagged reports whether the watcher was dropped because its consumer
// fell more than the buffer size behind.
func (w *Watcher) Lagged() bool {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	return w.lagged
}

// Close unsubscribes and closes the event channel. Safe to call more than
// once and after the store dropped the watcher for lagging.
func (w *Watcher) Close() {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	w.s.dropLocked(w)
}

// WatcherCount reports the number of live subscriptions; tests use it to
// synchronize with concurrently registered waiters.
func (s *Store) WatcherCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.watchers)
}

func (s *Store) dropLocked(w *Watcher) {
	if _, ok := s.watchers[w.id]; !ok {
		return // already dropped or closed
	}
	delete(s.watchers, w.id)
	close(w.ch)
}

// emitLocked fans an event out to every watcher whose prefix matches.
// A watcher with a full buffer is dropped and marked lagged: the board
// must never block on a slow reader.
func (s *Store) emitLocked(ev Event) {
	for _, w := range s.watchers {
		if !strings.HasPrefix(ev.Key, w.prefix) {
			continue
		}
		select {
		case w.ch <- ev:
		default:
			w.lagged = true
			s.dropLocked(w)
		}
	}
}
