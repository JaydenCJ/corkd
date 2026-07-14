// Data subcommands: set, get, del, incr, keys/dump, wait, watch, stats,
// ping. Each parses flags, performs exactly one protocol exchange (watch:
// a stream), renders text or JSON, and returns an exit code.
package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/JaydenCJ/corkd/internal/proto"
)

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// parseIfVersion turns the --if-version flag (string so that "unset" and
// "0" stay distinguishable — 0 means "must not exist") into a pointer.
func parseIfVersion(s string) (*uint64, error) {
	if s == "" {
		return nil, nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("--if-version must be a non-negative integer, got %q", s)
	}
	return &n, nil
}

// ttlMS converts a --ttl duration flag to wire milliseconds (nil = unset).
func ttlMS(d time.Duration) (*int64, error) {
	if d == 0 {
		return nil, nil
	}
	if d < time.Millisecond {
		return nil, fmt.Errorf("--ttl must be at least 1ms, got %s", d)
	}
	return proto.Int64Ptr(d.Milliseconds()), nil
}

func usageErr(stderr io.Writer, format string, args ...any) int {
	fmt.Fprintf(stderr, "corkd: "+format+"\n", args...)
	return ExitUsage
}

func cmdSet(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("set", stderr)
	ttl := fs.Duration("ttl", 0, "expire the key after this duration (e.g. 30s)")
	ifVersion := fs.String("if-version", "", "only set if current version equals N (0 = must not exist)")
	ifAbsent := fs.Bool("if-absent", false, "only set if the key does not exist")
	asJSON := fs.Bool("json", false, "print the full response as JSON")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	if fs.NArg() != 2 {
		return usageErr(stderr, "set needs KEY and VALUE (got %d args)", fs.NArg())
	}
	key, value := fs.Arg(0), fs.Arg(1)
	if value == "-" {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(stderr, "corkd: reading value from stdin: %v\n", err)
			return ExitRuntime
		}
		value = string(b)
	}
	iv, err := parseIfVersion(*ifVersion)
	if err != nil {
		return usageErr(stderr, "%v", err)
	}
	ms, err := ttlMS(*ttl)
	if err != nil {
		return usageErr(stderr, "%v", err)
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	resp, derr := c.Do(proto.Request{
		Op: proto.OpSet, Key: key, Value: &value,
		TTLMS: ms, IfVersion: iv, IfAbsent: *ifAbsent,
	})
	return finish(resp, derr, stdout, stderr, *asJSON, func() {
		line := fmt.Sprintf("ok key=%s version=%d", resp.Key, resp.Version)
		if resp.TTLMS != nil {
			line += fmt.Sprintf(" ttl_ms=%d", *resp.TTLMS)
		}
		fmt.Fprintln(stdout, line)
	})
}

func cmdGet(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("get", stderr)
	asJSON := fs.Bool("json", false, "print key, value, version and TTL as JSON")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	if fs.NArg() != 1 {
		return usageErr(stderr, "get needs exactly KEY")
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	resp, derr := c.Do(proto.Request{Op: proto.OpGet, Key: fs.Arg(0)})
	return finish(resp, derr, stdout, stderr, *asJSON, func() {
		fmt.Fprintln(stdout, *resp.Value)
	})
}

func cmdDel(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("del", stderr)
	ifVersion := fs.String("if-version", "", "only delete if current version equals N")
	asJSON := fs.Bool("json", false, "print the full response as JSON")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	if fs.NArg() != 1 {
		return usageErr(stderr, "del needs exactly KEY")
	}
	iv, err := parseIfVersion(*ifVersion)
	if err != nil {
		return usageErr(stderr, "%v", err)
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	resp, derr := c.Do(proto.Request{Op: proto.OpDel, Key: fs.Arg(0), IfVersion: iv})
	return finish(resp, derr, stdout, stderr, *asJSON, func() {
		fmt.Fprintf(stdout, "deleted key=%s\n", resp.Key)
	})
}

func cmdIncr(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("incr", stderr)
	by := fs.Int64("by", 1, "amount to add (may be negative)")
	ttl := fs.Duration("ttl", 0, "replace the key's TTL with this duration")
	asJSON := fs.Bool("json", false, "print the full response as JSON")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	if fs.NArg() != 1 {
		return usageErr(stderr, "incr needs exactly KEY")
	}
	ms, err := ttlMS(*ttl)
	if err != nil {
		return usageErr(stderr, "%v", err)
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	resp, derr := c.Do(proto.Request{Op: proto.OpIncr, Key: fs.Arg(0), By: by, TTLMS: ms})
	return finish(resp, derr, stdout, stderr, *asJSON, func() {
		fmt.Fprintln(stdout, *resp.Num)
	})
}

func cmdKeys(args []string, stdout, stderr io.Writer, withValues bool) int {
	name, op := "keys", proto.OpKeys
	if withValues {
		name, op = "dump", proto.OpDump
	}
	fs, socket := newFlags(name, stderr)
	asJSON := fs.Bool("json", false, "print entries as a JSON array")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	if fs.NArg() > 1 {
		return usageErr(stderr, "%s takes at most one PREFIX", name)
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	resp, derr := c.Do(proto.Request{Op: op, Prefix: fs.Arg(0)})
	if derr == nil && resp.OK && *asJSON {
		// A bare array is friendlier to jq than the response envelope.
		if resp.Keys == nil {
			resp.Keys = []proto.KeyInfo{}
		}
		printJSON(stdout, resp.Keys)
		return ExitOK
	}
	return finish(resp, derr, stdout, stderr, false, func() {
		for _, k := range resp.Keys {
			if withValues {
				ttl := "-"
				if k.TTLMS != nil {
					ttl = (time.Duration(*k.TTLMS) * time.Millisecond).String()
				}
				fmt.Fprintf(stdout, "%s\tv%d\tttl=%s\t%s\n", k.Key, k.Version, ttl, *k.Value)
			} else {
				fmt.Fprintln(stdout, k.Key)
			}
		}
	})
}

func cmdWait(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("wait", stderr)
	equals := fs.String("equals", "", "wait until the value equals this string")
	equalsSet := false
	gone := fs.Bool("gone", false, "wait until the key is deleted or expired")
	timeout := fs.Duration("timeout", 10*time.Second, "give up after this duration")
	asJSON := fs.Bool("json", false, "print the full response as JSON")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "equals" {
			equalsSet = true
		}
	})
	if fs.NArg() != 1 {
		return usageErr(stderr, "wait needs exactly KEY")
	}
	if equalsSet && *gone {
		return usageErr(stderr, "--equals and --gone are mutually exclusive")
	}
	if *timeout < time.Millisecond {
		return usageErr(stderr, "--timeout must be at least 1ms")
	}
	req := proto.Request{
		Op: proto.OpWait, Key: fs.Arg(0), Gone: *gone,
		TimeoutMS: proto.Int64Ptr(timeout.Milliseconds()),
	}
	if equalsSet {
		req.Equals = equals
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	resp, derr := c.Do(req)
	return finish(resp, derr, stdout, stderr, *asJSON, func() {
		if *gone {
			fmt.Fprintf(stdout, "gone key=%s\n", resp.Key)
		} else {
			fmt.Fprintln(stdout, *resp.Value)
		}
	})
}

func cmdWatch(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("watch", stderr)
	state := fs.Bool("state", false, "replay current entries (then a sync event) before live events")
	count := fs.Int("count", 0, "exit after N events (0 = stream forever)")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	if fs.NArg() > 1 {
		return usageErr(stderr, "watch takes at most one PREFIX")
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	if err := c.StartWatch(fs.Arg(0), *state); err != nil {
		fmt.Fprintf(stderr, "corkd: %v\n", err)
		return ExitRuntime
	}
	seen := 0
	for {
		ev, err := c.NextEvent()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return ExitOK // server closed the stream
			}
			fmt.Fprintf(stderr, "corkd: watch stream: %v\n", err)
			return ExitRuntime
		}
		printJSON(stdout, ev)
		if ev.Event == proto.CodeLagged {
			fmt.Fprintln(stderr, "corkd: watch lagged behind and was dropped; re-watch with --state")
			return ExitRuntime
		}
		seen++
		if *count > 0 && seen >= *count {
			return ExitOK
		}
	}
}

func cmdStats(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("stats", stderr)
	asJSON := fs.Bool("json", false, "print stats as JSON")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	resp, derr := c.Do(proto.Request{Op: proto.OpStats})
	if derr == nil && resp.OK && *asJSON {
		printJSON(stdout, resp.Stats)
		return ExitOK
	}
	return finish(resp, derr, stdout, stderr, false, func() {
		s := resp.Stats
		fmt.Fprintf(stdout, "keys=%d seq=%d watchers=%d puts=%d dels=%d expires=%d\n",
			s.Keys, s.Seq, s.Watchers, s.Puts, s.Dels, s.Expires)
	})
}

func cmdPing(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("ping", stderr)
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	c, code := dial(*socket, stderr)
	if c == nil {
		return code
	}
	defer c.Close()
	resp, derr := c.Do(proto.Request{Op: proto.OpPing})
	return finish(resp, derr, stdout, stderr, false, func() {
		fmt.Fprintf(stdout, "pong corkd %s\n", resp.Server)
	})
}
