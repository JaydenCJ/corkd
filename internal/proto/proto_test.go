// Tests for wire types and request validation: what is accepted, what is
// rejected, and that JSON shapes stay stable (they are the protocol).
package proto

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func lim() Limits { return DefaultLimits() }

func TestValidateRejectsUnknownOrMissingOp(t *testing.T) {
	err := ValidateRequest(&Request{Op: "explode"}, lim())
	if err == nil || !strings.Contains(err.Message, "unknown op") {
		t.Fatalf("unknown op: err = %v", err)
	}
	if ValidateRequest(&Request{}, lim()) == nil {
		t.Fatal("empty op accepted")
	}
}

func TestValidateRequiresKeyForKeyOps(t *testing.T) {
	for _, op := range []string{OpGet, OpDel, OpIncr, OpWait} {
		if ValidateRequest(&Request{Op: op}, lim()) == nil {
			t.Fatalf("op %q accepted without a key", op)
		}
	}
}

func TestValidateKeyRules(t *testing.T) {
	// Exactly at the limit is fine; one byte over is not.
	atLimit := strings.Repeat("k", lim().MaxKeyBytes)
	if err := ValidateRequest(&Request{Op: OpGet, Key: atLimit}, lim()); err != nil {
		t.Fatalf("key at limit rejected: %v", err)
	}
	if ValidateRequest(&Request{Op: OpGet, Key: atLimit + "k"}, lim()) == nil {
		t.Fatal("oversized key accepted")
	}
	// Control characters and broken encodings would poison logs and
	// line-oriented pipelines, so they are refused outright.
	for _, key := range []string{"a\nb", "a\tb", "a\x00b", "\x7f", "a\xffb"} {
		if ValidateRequest(&Request{Op: OpGet, Key: key}, lim()) == nil {
			t.Fatalf("malformed key %q accepted", key)
		}
	}
	// Any printable UTF-8 is a legal key.
	if err := ValidateRequest(&Request{Op: OpGet, Key: "レビュー/状態"}, lim()); err != nil {
		t.Fatalf("unicode key rejected: %v", err)
	}
}

func TestValidateSetRules(t *testing.T) {
	if err := ValidateRequest(&Request{Op: OpSet, Key: "k", Value: StringPtr("v")}, lim()); err != nil {
		t.Fatalf("minimal set rejected: %v", err)
	}
	err := ValidateRequest(&Request{Op: OpSet, Key: "k"}, lim())
	if err == nil || !strings.Contains(err.Message, "value") {
		t.Fatalf("set without value: err = %v", err)
	}
	big := strings.Repeat("x", lim().MaxValueBytes+1)
	if ValidateRequest(&Request{Op: OpSet, Key: "k", Value: &big}, lim()) == nil {
		t.Fatal("oversized value accepted")
	}
	neg := &Request{Op: OpSet, Key: "k", Value: StringPtr("v"), TTLMS: Int64Ptr(-1)}
	if ValidateRequest(neg, lim()) == nil {
		t.Fatal("negative ttl accepted")
	}
}

func TestValidateWaitRules(t *testing.T) {
	both := &Request{Op: OpWait, Key: "k", Equals: StringPtr("v"), Gone: true}
	if ValidateRequest(both, lim()) == nil {
		t.Fatal("equals+gone accepted")
	}
	for _, ms := range []int64{0, -5, MaxWaitTimeoutMS + 1} {
		req := &Request{Op: OpWait, Key: "k", TimeoutMS: Int64Ptr(ms)}
		if ValidateRequest(req, lim()) == nil {
			t.Fatalf("timeout_ms=%d accepted", ms)
		}
	}
	req := &Request{Op: OpWait, Key: "k", TimeoutMS: Int64Ptr(MaxWaitTimeoutMS)}
	if err := ValidateRequest(req, lim()); err != nil {
		t.Fatalf("max timeout rejected: %v", err)
	}
}

func TestValidateDelRejectsIfAbsent(t *testing.T) {
	// if_absent on del is always a client bug: deleting a key that must
	// not exist can never succeed, so it is rejected loudly.
	if ValidateRequest(&Request{Op: OpDel, Key: "k", IfAbsent: true}, lim()) == nil {
		t.Fatal("del with if_absent accepted")
	}
}

func TestWaitTimeoutAndTTLResolution(t *testing.T) {
	if d := WaitTimeout(&Request{}); d != DefaultWaitTimeoutMS*time.Millisecond {
		t.Fatalf("default wait timeout = %v", d)
	}
	if d := WaitTimeout(&Request{TimeoutMS: Int64Ptr(1500)}); d != 1500*time.Millisecond {
		t.Fatalf("resolved wait timeout = %v", d)
	}
	if d := TTL(&Request{}); d != 0 {
		t.Fatalf("unset ttl = %v, want 0", d)
	}
	if d := TTL(&Request{TTLMS: Int64Ptr(2500)}); d != 2500*time.Millisecond {
		t.Fatalf("ttl = %v", d)
	}
}

func TestRequestJSONRoundTripPreservesIfVersionZero(t *testing.T) {
	// if_version=0 (create-only CAS) must survive encoding — a naive
	// omitempty on a plain uint64 would silently drop it.
	req := Request{Op: OpSet, Key: "k", Value: StringPtr("v"), IfVersion: new(uint64)}
	b, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"if_version":0`) {
		t.Fatalf("if_version=0 lost: %s", b)
	}
	var back Request
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.IfVersion == nil || *back.IfVersion != 0 {
		t.Fatalf("round-trip = %+v", back.IfVersion)
	}
}

func TestResponseJSONShape(t *testing.T) {
	b, err := json.Marshal(Response{OK: true})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"ok":true}` {
		t.Fatalf("minimal response = %s", b)
	}
	// An empty value is a legal, meaningful payload and must be visible
	// on the wire (pointer, not omitempty-string).
	b, err = json.Marshal(Response{OK: true, Key: "k", Value: StringPtr("")})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"value":""`) {
		t.Fatalf("empty value dropped: %s", b)
	}
}

func TestEventJSONShape(t *testing.T) {
	b, err := json.Marshal(Event{Event: "put", Key: "k", Value: StringPtr("v"), Version: 7, Seq: 7})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"event":"put","key":"k","value":"v","version":7,"seq":7}`
	if string(b) != want {
		t.Fatalf("event json = %s, want %s", b, want)
	}
}

func TestDefaultLimitsAreSane(t *testing.T) {
	l := DefaultLimits()
	if l.MaxKeyBytes < 64 || l.MaxValueBytes < 4096 {
		t.Fatalf("limits too small to be useful: %+v", l)
	}
	// A max-size value must fit a protocol line even when every byte
	// needs JSON escaping (\uXXXX is 6 bytes per input byte).
	if l.MaxValueBytes*6 > MaxLineBytes {
		t.Fatal("worst-case escaped value exceeds MaxLineBytes")
	}
}
