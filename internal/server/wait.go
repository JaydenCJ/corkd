// Blocking primitives: wait (one-shot condition on a single key) and
// watch (long-lived event stream). Both are built on the store's atomic
// snapshot+subscribe operations, so no mutation can slip between "check
// current state" and "start listening".
package server

import (
	"net"
	"time"

	"github.com/JaydenCJ/corkd/internal/proto"
	"github.com/JaydenCJ/corkd/internal/store"
)

// doWait blocks until the key satisfies the requested condition or the
// timeout elapses. Conditions:
//
//   - default:      the key exists (any value)
//   - equals=V:     the key exists with exactly value V
//   - gone=true:    the key does not exist (deleted or expired)
//
// The success response snapshots the satisfying state (value/version for
// existence waits). Timeout yields ok=false with error "timeout".
func (s *Server) doWait(req *proto.Request) proto.Response {
	view, exists, w := s.st.GetAndWatch(req.Key, s.cfg.WatchBuffer)
	defer w.Close()

	satisfiedNow := func() (proto.Response, bool) {
		switch {
		case req.Gone:
			if !exists {
				return proto.Response{OK: true, Key: req.Key}, true
			}
		case req.Equals != nil:
			if exists && view.Value == *req.Equals {
				return viewResp(view), true
			}
		default:
			if exists {
				return viewResp(view), true
			}
		}
		return proto.Response{}, false
	}
	if resp, ok := satisfiedNow(); ok {
		return resp
	}

	timer := time.NewTimer(proto.WaitTimeout(req))
	defer timer.Stop()
	for {
		select {
		case ev, open := <-w.Events():
			if !open {
				// Dropped for lagging — with a fresh dedicated watcher this
				// means the board outpaced us by a full buffer.
				return errResp(proto.CodeLagged, "wait fell behind the event stream", 0)
			}
			// The watcher is prefix-scoped to the key, so siblings like
			// "job/1x" also arrive; only exact matches count.
			if ev.Key != req.Key {
				continue
			}
			switch ev.Kind {
			case store.EventPut:
				if req.Gone {
					continue
				}
				if req.Equals != nil && ev.Value != *req.Equals {
					continue
				}
				r := proto.Response{OK: true, Key: req.Key, Value: proto.StringPtr(ev.Value), Version: ev.Version}
				return r
			case store.EventDel, store.EventExpire:
				if req.Gone {
					return proto.Response{OK: true, Key: req.Key}
				}
			}
		case <-timer.C:
			return errResp(proto.CodeTimeout, "wait timed out", 0)
		case <-s.done:
			return errResp(proto.CodeInternal, "server shutting down", 0)
		}
	}
}

// serveWatch turns the connection into an event stream. Events are
// written as they happen; the stream ends when the client hangs up, the
// watcher lags out (final "lagged" event), or the server shuts down.
func (s *Server) serveWatch(conn net.Conn, req *proto.Request) {
	w := s.st.Watch(req.Prefix, req.State, s.cfg.WatchBuffer)
	defer w.Close()

	// Detect client hangup: the client never sends again after watch, so
	// any read completion (EOF or error) means the stream is dead.
	gone := make(chan struct{})
	go func() {
		buf := make([]byte, 1)
		for {
			if _, err := conn.Read(buf); err != nil {
				close(gone)
				return
			}
		}
	}()

	for {
		select {
		case ev, open := <-w.Events():
			if !open {
				writeJSON(conn, proto.Event{Event: proto.CodeLagged, Seq: 0})
				return
			}
			if writeJSON(conn, wireEvent(ev)) != nil {
				return
			}
		case <-gone:
			return
		case <-s.done:
			return
		}
	}
}

func wireEvent(ev store.Event) proto.Event {
	we := proto.Event{Event: string(ev.Kind), Key: ev.Key, Seq: ev.Seq}
	switch ev.Kind {
	case store.EventPut:
		we.Value = proto.StringPtr(ev.Value)
		we.Version = ev.Version
	case store.EventDel, store.EventExpire:
		we.Version = ev.Version
	}
	return we
}
