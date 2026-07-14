// Package proto defines corkd's wire protocol: newline-delimited JSON
// over a unix stream socket. Each request is one JSON object on one line;
// each response is one JSON object on one line. The watch op switches the
// connection into a one-way event stream (one JSON event per line) until
// either side hangs up.
//
// The protocol is deliberately trivial to speak without a client library:
// any language's JSON encoder plus a socket is a full client, and so is
// `nc -U` for a human. docs/protocol.md is the normative description.
package proto

import (
	"fmt"
	"time"
	"unicode/utf8"
)

// MaxLineBytes bounds a single request/response line; it comfortably
// fits the largest legal value plus JSON escaping overhead.
const MaxLineBytes = 1 << 20

// Known operations.
const (
	OpPing  = "ping"
	OpSet   = "set"
	OpGet   = "get"
	OpDel   = "del"
	OpKeys  = "keys"
	OpDump  = "dump"
	OpIncr  = "incr"
	OpWait  = "wait"
	OpWatch = "watch"
	OpStats = "stats"
)

// Error codes carried in Response.Error (beyond the store's own codes).
const (
	CodeBadRequest = "bad_request"
	CodeTimeout    = "timeout"
	CodeLagged     = "lagged"
	CodeInternal   = "internal"
)

// Wait timeout bounds, in milliseconds.
const (
	DefaultWaitTimeoutMS = 10_000
	MaxWaitTimeoutMS     = 600_000
)

// Request is a single client command. Optional fields are pointers so
// "absent" and "zero" stay distinguishable (if_version 0 is a meaningful
// CAS condition: the key must not exist).
type Request struct {
	Op        string  `json:"op"`
	Key       string  `json:"key,omitempty"`
	Value     *string `json:"value,omitempty"`
	TTLMS     *int64  `json:"ttl_ms,omitempty"`
	IfVersion *uint64 `json:"if_version,omitempty"`
	IfAbsent  bool    `json:"if_absent,omitempty"`
	By        *int64  `json:"by,omitempty"`     // incr; default 1
	Prefix    string  `json:"prefix,omitempty"` // keys, dump, watch
	Equals    *string `json:"equals,omitempty"` // wait: value must equal this
	Gone      bool    `json:"gone,omitempty"`   // wait: key must be absent
	TimeoutMS *int64  `json:"timeout_ms,omitempty"`
	State     bool    `json:"state,omitempty"` // watch: replay current state first
}

// KeyInfo is one entry in a keys/dump listing.
type KeyInfo struct {
	Key     string  `json:"key"`
	Value   *string `json:"value,omitempty"` // set by dump, omitted by keys
	Version uint64  `json:"version"`
	TTLMS   *int64  `json:"ttl_ms,omitempty"`
}

// Stats mirrors store.Stats on the wire.
type Stats struct {
	Keys     int    `json:"keys"`
	Seq      uint64 `json:"seq"`
	Watchers int    `json:"watchers"`
	Puts     uint64 `json:"puts"`
	Dels     uint64 `json:"dels"`
	Expires  uint64 `json:"expires"`
}

// Response answers exactly one request. OK=false responses carry a stable
// machine-readable Error code and a human-readable Message; CAS conflicts
// also carry the key's current version so clients can retry immediately.
type Response struct {
	OK      bool      `json:"ok"`
	Error   string    `json:"error,omitempty"`
	Message string    `json:"message,omitempty"`
	Key     string    `json:"key,omitempty"`
	Value   *string   `json:"value,omitempty"`
	Version uint64    `json:"version,omitempty"`
	TTLMS   *int64    `json:"ttl_ms,omitempty"`
	Num     *int64    `json:"num,omitempty"` // incr result
	Keys    []KeyInfo `json:"keys,omitempty"`
	Stats   *Stats    `json:"stats,omitempty"`
	Server  string    `json:"server,omitempty"` // corkd version, on ping
}

// Event is one line of a watch stream. Kinds: put, del, expire, sync,
// lagged. Value is present only for put; Version carries the removed
// entry's last version for del/expire.
type Event struct {
	Event   string  `json:"event"`
	Key     string  `json:"key,omitempty"`
	Value   *string `json:"value,omitempty"`
	Version uint64  `json:"version,omitempty"`
	Seq     uint64  `json:"seq"`
}

// Limits bound key and value sizes; the server validates every request
// against them before touching the store.
type Limits struct {
	MaxKeyBytes   int
	MaxValueBytes int
}

// DefaultLimits are generous for coordination data while keeping any
// single line far below MaxLineBytes.
func DefaultLimits() Limits {
	return Limits{MaxKeyBytes: 256, MaxValueBytes: 64 * 1024}
}

// ReqError is a validation failure; the server maps it to a bad_request
// response without touching the store.
type ReqError struct{ Message string }

func (e *ReqError) Error() string { return e.Message }

func badf(format string, args ...any) *ReqError {
	return &ReqError{Message: fmt.Sprintf(format, args...)}
}

// ValidKey reports why key is unusable, or nil. Keys are non-empty UTF-8
// without control characters, bounded by lim — printable, greppable, and
// safe to embed in any log line or shell pipeline.
func ValidKey(key string, lim Limits) *ReqError {
	if key == "" {
		return badf("key must not be empty")
	}
	if len(key) > lim.MaxKeyBytes {
		return badf("key exceeds %d bytes", lim.MaxKeyBytes)
	}
	if !utf8.ValidString(key) {
		return badf("key must be valid UTF-8")
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f {
			return badf("key must not contain control characters")
		}
	}
	return nil
}

// ValidateRequest checks structural validity of req against lim. It does
// not consult the store; CAS conditions are evaluated there.
func ValidateRequest(req *Request, lim Limits) *ReqError {
	switch req.Op {
	case OpPing, OpStats:
		return nil
	case OpKeys, OpDump:
		return nil // prefix may be anything, including empty
	case OpWatch:
		return nil
	case OpSet:
		if err := ValidKey(req.Key, lim); err != nil {
			return err
		}
		if req.Value == nil {
			return badf("set requires a value")
		}
		if len(*req.Value) > lim.MaxValueBytes {
			return badf("value exceeds %d bytes", lim.MaxValueBytes)
		}
		return validTTL(req.TTLMS)
	case OpGet:
		return ValidKey(req.Key, lim)
	case OpDel:
		if req.IfAbsent {
			return badf("del does not accept if_absent")
		}
		return ValidKey(req.Key, lim)
	case OpIncr:
		if err := ValidKey(req.Key, lim); err != nil {
			return err
		}
		return validTTL(req.TTLMS)
	case OpWait:
		if err := ValidKey(req.Key, lim); err != nil {
			return err
		}
		if req.Equals != nil && req.Gone {
			return badf("wait accepts equals or gone, not both")
		}
		if req.TimeoutMS != nil && (*req.TimeoutMS < 1 || *req.TimeoutMS > MaxWaitTimeoutMS) {
			return badf("timeout_ms must be within 1..%d", MaxWaitTimeoutMS)
		}
		return nil
	case "":
		return badf("missing op")
	default:
		return badf("unknown op %q", req.Op)
	}
}

func validTTL(ms *int64) *ReqError {
	if ms != nil && *ms < 0 {
		return badf("ttl_ms must not be negative")
	}
	return nil
}

// WaitTimeout resolves a request's effective wait timeout.
func WaitTimeout(req *Request) time.Duration {
	ms := int64(DefaultWaitTimeoutMS)
	if req.TimeoutMS != nil {
		ms = *req.TimeoutMS
	}
	return time.Duration(ms) * time.Millisecond
}

// TTL resolves a request's TTL (0 = no expiry).
func TTL(req *Request) time.Duration {
	if req.TTLMS == nil {
		return 0
	}
	return time.Duration(*req.TTLMS) * time.Millisecond
}

// StringPtr is a small helper for building requests and responses.
func StringPtr(s string) *string { return &s }

// Int64Ptr is a small helper for building requests and responses.
func Int64Ptr(n int64) *int64 { return &n }
