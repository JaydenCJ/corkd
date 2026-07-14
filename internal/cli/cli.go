// Package cli implements the corkd command line: argument parsing,
// subcommand dispatch, exit codes, and human/JSON rendering. All command
// logic goes through internal/client over a real socket, so the CLI tests
// double as end-to-end protocol tests.
package cli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/JaydenCJ/corkd/internal/client"
	"github.com/JaydenCJ/corkd/internal/proto"
	"github.com/JaydenCJ/corkd/internal/version"
)

// Exit codes. Conditions that a caller is expected to branch on (missing
// key, CAS conflict, wait timeout) are 1 so shell scripts can use plain
// `if corkd …`; genuine breakage is kept distinct.
const (
	ExitOK      = 0
	ExitCond    = 1 // condition not met: not_found, conflict, exists, timeout
	ExitUsage   = 2 // bad flags or arguments
	ExitRuntime = 3 // connection or server failure
)

const usageText = `corkd — shared blackboard for co-located agents

Usage:
  corkd serve  [--socket PATH] [--sweep-interval DUR] [--quiet]
               [--max-key-bytes N] [--max-value-bytes N]
  corkd set    [--ttl DUR] [--if-version N] [--if-absent] [--json] KEY VALUE
  corkd get    [--json] KEY
  corkd del    [--if-version N] [--json] KEY
  corkd incr   [--by N] [--ttl DUR] [--json] KEY
  corkd keys   [--json] [PREFIX]
  corkd dump   [--json] [PREFIX]
  corkd wait   [--equals V | --gone] [--timeout DUR] [--json] KEY
  corkd watch  [--state] [--count N] [PREFIX]
  corkd stats  [--json]
  corkd ping
  corkd version

Flags come before positional arguments. VALUE of "-" reads the value from
stdin. All commands accept --socket PATH (default: $CORKD_SOCKET, then
$XDG_RUNTIME_DIR/corkd.sock, then $TMPDIR/corkd-<uid>.sock).

Exit codes: 0 ok · 1 condition not met (missing key, CAS conflict,
wait timeout) · 2 usage error · 3 connection/server error.
`

// Run executes the CLI and returns its process exit code.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageText)
		return ExitUsage
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "corkd %s\n", version.Version)
		return ExitOK
	case "help", "--help", "-h":
		fmt.Fprint(stdout, usageText)
		return ExitOK
	case "serve":
		return cmdServe(rest, stdout, stderr)
	case "set":
		return cmdSet(rest, stdout, stderr)
	case "get":
		return cmdGet(rest, stdout, stderr)
	case "del":
		return cmdDel(rest, stdout, stderr)
	case "incr":
		return cmdIncr(rest, stdout, stderr)
	case "keys":
		return cmdKeys(rest, stdout, stderr, false)
	case "dump":
		return cmdKeys(rest, stdout, stderr, true)
	case "wait":
		return cmdWait(rest, stdout, stderr)
	case "watch":
		return cmdWatch(rest, stdout, stderr)
	case "stats":
		return cmdStats(rest, stdout, stderr)
	case "ping":
		return cmdPing(rest, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "corkd: unknown command %q (see `corkd help`)\n", cmd)
		return ExitUsage
	}
}

// DefaultSocket resolves the socket path shared by server and clients:
// explicit env override first, then the user's runtime dir, then a
// uid-scoped path under the system temp dir.
func DefaultSocket() string {
	if p := os.Getenv("CORKD_SOCKET"); p != "" {
		return p
	}
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		return filepath.Join(d, "corkd.sock")
	}
	return filepath.Join(os.TempDir(), fmt.Sprintf("corkd-%d.sock", os.Getuid()))
}

// newFlags builds a FlagSet with the flags every subcommand shares.
func newFlags(name string, stderr io.Writer) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	socket := fs.String("socket", DefaultSocket(), "unix socket path of the board")
	return fs, socket
}

// dial opens a client, translating failure into an exit code.
func dial(socket string, stderr io.Writer) (*client.Client, int) {
	c, err := client.Dial(socket)
	if err != nil {
		fmt.Fprintf(stderr, "corkd: %v\n", err)
		return nil, ExitRuntime
	}
	return c, ExitOK
}

// finish maps a protocol response onto output and an exit code. onOK
// renders the success case to stdout.
func finish(resp proto.Response, err error, stdout, stderr io.Writer, asJSON bool, onOK func()) int {
	if err != nil {
		fmt.Fprintf(stderr, "corkd: %v\n", err)
		return ExitRuntime
	}
	if !resp.OK {
		if asJSON {
			printJSON(stdout, resp)
		} else {
			fmt.Fprintf(stderr, "corkd: %s: %s\n", resp.Error, resp.Message)
		}
		switch resp.Error {
		case proto.CodeBadRequest, proto.CodeInternal, proto.CodeLagged:
			return ExitRuntime
		default:
			return ExitCond // not_found, version_conflict, exists, timeout, not_number
		}
	}
	if asJSON {
		printJSON(stdout, resp)
	} else if onOK != nil {
		onOK()
	}
	return ExitOK
}

func printJSON(w io.Writer, v any) {
	b, err := jsonMarshal(v)
	if err != nil {
		fmt.Fprintf(w, `{"ok":false,"error":"internal"}`+"\n")
		return
	}
	fmt.Fprintf(w, "%s\n", b)
}
