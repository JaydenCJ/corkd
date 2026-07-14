// Package server serves the blackboard over a unix stream socket, one
// goroutine per connection, speaking the newline-delimited JSON protocol
// from internal/proto. It owns socket lifecycle (stale-socket takeover,
// 0600 permissions, cleanup on close), the TTL sweeper, and the blocking
// semantics of wait and watch.
package server

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/JaydenCJ/corkd/internal/proto"
	"github.com/JaydenCJ/corkd/internal/store"
	"github.com/JaydenCJ/corkd/internal/version"
)

// Config tunes a Server. The zero value plus a Socket path is usable.
type Config struct {
	// Socket is the unix socket path to listen on. Required.
	Socket string
	// Limits bound key/value sizes; zero fields fall back to defaults.
	Limits proto.Limits
	// SweepInterval is how often expired keys are actively removed so
	// watchers see expire events promptly. 0 disables the sweeper (expiry
	// then happens lazily on access). Tests drive Store().Sweep() directly.
	SweepInterval time.Duration
	// WatchBuffer is the per-watcher event buffer (0 = store default).
	WatchBuffer int
	// Now injects a clock for tests; nil means time.Now.
	Now func() time.Time
	// Logf receives one line per lifecycle event; nil silences logging.
	Logf func(format string, args ...any)
}

// Server is one listening blackboard instance.
type Server struct {
	cfg  Config
	st   *store.Store
	ln   net.Listener
	done chan struct{}
	wg   sync.WaitGroup

	mu     sync.Mutex
	closed bool
}

// New builds a Server (and its empty board) from cfg.
func New(cfg Config) *Server {
	if cfg.Limits.MaxKeyBytes <= 0 {
		cfg.Limits.MaxKeyBytes = proto.DefaultLimits().MaxKeyBytes
	}
	if cfg.Limits.MaxValueBytes <= 0 {
		cfg.Limits.MaxValueBytes = proto.DefaultLimits().MaxValueBytes
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	return &Server{
		cfg:  cfg,
		st:   store.New(cfg.Now),
		done: make(chan struct{}),
	}
}

// Store exposes the underlying board (tests and embedders use it).
func (s *Server) Store() *store.Store { return s.st }

// Addr returns the socket path being served.
func (s *Server) Addr() string { return s.cfg.Socket }

// Listen claims the socket. A leftover socket file from a crashed server
// is removed and taken over; a socket with a live server behind it is an
// error — two boards on one path would split the agents' shared memory.
func (s *Server) Listen() error {
	path := s.cfg.Socket
	if path == "" {
		return errors.New("no socket path configured")
	}
	if _, err := os.Stat(path); err == nil {
		conn, err := net.DialTimeout("unix", path, 250*time.Millisecond)
		if err == nil {
			conn.Close()
			return fmt.Errorf("socket %s already has a live server", path)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("removing stale socket: %w", err)
		}
		s.cfg.Logf("removed stale socket %s", path)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		os.Remove(path)
		return fmt.Errorf("restricting socket permissions: %w", err)
	}
	s.ln = ln
	s.cfg.Logf("corkd %s listening on %s", version.Version, path)
	return nil
}

// Serve accepts connections until Close. Listen must have succeeded.
func (s *Server) Serve() error {
	if s.cfg.SweepInterval > 0 {
		s.wg.Add(1)
		go s.sweeper()
	}
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return nil // orderly shutdown
			default:
				return err
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// Close stops accepting, unblocks in-flight waits, waits for connection
// goroutines, and removes the socket file.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	close(s.done)
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	s.wg.Wait()
	os.Remove(s.cfg.Socket)
	return err
}

func (s *Server) sweeper() {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.st.Sweep()
		case <-s.done:
			return
		}
	}
}

func writeJSON(conn net.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = conn.Write(append(b, '\n'))
	return err
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 64*1024), proto.MaxLineBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var req proto.Request
		if err := json.Unmarshal(line, &req); err != nil {
			if writeJSON(conn, errResp(proto.CodeBadRequest, "request is not valid JSON: "+err.Error(), 0)) != nil {
				return
			}
			continue
		}
		if verr := proto.ValidateRequest(&req, s.cfg.Limits); verr != nil {
			if writeJSON(conn, errResp(proto.CodeBadRequest, verr.Message, 0)) != nil {
				return
			}
			continue
		}
		if req.Op == proto.OpWatch {
			s.serveWatch(conn, &req)
			return // the stream consumes the rest of the connection
		}
		if writeJSON(conn, s.dispatch(&req)) != nil {
			return
		}
	}
	if errors.Is(sc.Err(), bufio.ErrTooLong) {
		// Tell the client why it is being hung up on instead of leaving it
		// with a bare EOF.
		writeJSON(conn, errResp(proto.CodeBadRequest,
			fmt.Sprintf("request line exceeds %d bytes", proto.MaxLineBytes), 0))
	}
}

func errResp(code, msg string, current uint64) proto.Response {
	return proto.Response{OK: false, Error: code, Message: msg, Version: current}
}

func storeErr(err error) proto.Response {
	var se *store.Error
	if errors.As(err, &se) {
		return errResp(se.Code, se.Message, se.Current)
	}
	return errResp(proto.CodeInternal, err.Error(), 0)
}

func viewResp(v store.EntryView) proto.Response {
	r := proto.Response{OK: true, Key: v.Key, Value: proto.StringPtr(v.Value), Version: v.Version}
	if v.HasTTL {
		r.TTLMS = proto.Int64Ptr(v.TTL.Milliseconds())
	}
	return r
}

func (s *Server) dispatch(req *proto.Request) proto.Response {
	switch req.Op {
	case proto.OpPing:
		return proto.Response{OK: true, Server: version.Version}
	case proto.OpSet:
		v, err := s.st.Put(req.Key, *req.Value, proto.TTL(req), cond(req))
		if err != nil {
			return storeErr(err)
		}
		return viewResp(v)
	case proto.OpGet:
		v, err := s.st.Get(req.Key)
		if err != nil {
			return storeErr(err)
		}
		return viewResp(v)
	case proto.OpDel:
		if err := s.st.Del(req.Key, cond(req)); err != nil {
			return storeErr(err)
		}
		return proto.Response{OK: true, Key: req.Key}
	case proto.OpIncr:
		by := int64(1)
		if req.By != nil {
			by = *req.By
		}
		n, v, err := s.st.Incr(req.Key, by, proto.TTL(req), req.TTLMS != nil)
		if err != nil {
			return storeErr(err)
		}
		r := viewResp(v)
		r.Num = proto.Int64Ptr(n)
		return r
	case proto.OpKeys:
		return listResp(s.st.List(req.Prefix), false)
	case proto.OpDump:
		return listResp(s.st.List(req.Prefix), true)
	case proto.OpStats:
		st := s.st.Stats()
		return proto.Response{OK: true, Stats: &proto.Stats{
			Keys:     st.Keys,
			Seq:      st.Seq,
			Watchers: st.Watchers,
			Puts:     st.Puts,
			Dels:     st.Dels,
			Expires:  st.Expires,
		}}
	case proto.OpWait:
		return s.doWait(req)
	default:
		// Unreachable after validation; kept as a guard.
		return errResp(proto.CodeBadRequest, "unknown op", 0)
	}
}

func cond(req *proto.Request) store.Cond {
	return store.Cond{IfVersion: req.IfVersion, IfAbsent: req.IfAbsent}
}

func listResp(views []store.EntryView, withValues bool) proto.Response {
	keys := make([]proto.KeyInfo, 0, len(views))
	for _, v := range views {
		ki := proto.KeyInfo{Key: v.Key, Version: v.Version}
		if withValues {
			ki.Value = proto.StringPtr(v.Value)
		}
		if v.HasTTL {
			ki.TTLMS = proto.Int64Ptr(v.TTL.Milliseconds())
		}
		keys = append(keys, ki)
	}
	return proto.Response{OK: true, Keys: keys}
}
