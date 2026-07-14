// The serve subcommand: run the board in the foreground until SIGINT or
// SIGTERM, then clean up the socket. Everything else is a client.
package cli

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JaydenCJ/corkd/internal/proto"
	"github.com/JaydenCJ/corkd/internal/server"
)

func cmdServe(args []string, stdout, stderr io.Writer) int {
	fs, socket := newFlags("serve", stderr)
	sweep := fs.Duration("sweep-interval", 250*time.Millisecond,
		"how often expired keys are actively removed (0 = lazy expiry only)")
	maxKey := fs.Int("max-key-bytes", proto.DefaultLimits().MaxKeyBytes, "maximum key size")
	maxValue := fs.Int("max-value-bytes", proto.DefaultLimits().MaxValueBytes, "maximum value size")
	quiet := fs.Bool("quiet", false, "suppress lifecycle log lines")
	if fs.Parse(args) != nil {
		return ExitUsage
	}
	if fs.NArg() != 0 {
		return usageErr(stderr, "serve takes no positional arguments")
	}
	if *sweep < 0 {
		return usageErr(stderr, "--sweep-interval must not be negative")
	}
	if *maxKey < 1 || *maxValue < 1 {
		return usageErr(stderr, "size limits must be positive")
	}

	logf := func(format string, a ...any) { fmt.Fprintf(stderr, format+"\n", a...) }
	if *quiet {
		logf = nil
	}
	srv := server.New(server.Config{
		Socket:        *socket,
		Limits:        proto.Limits{MaxKeyBytes: *maxKey, MaxValueBytes: *maxValue},
		SweepInterval: *sweep,
		Logf:          logf,
	})
	if err := srv.Listen(); err != nil {
		fmt.Fprintf(stderr, "corkd: %v\n", err)
		return ExitRuntime
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	done := make(chan error, 1)
	go func() { done <- srv.Serve() }()

	select {
	case s := <-sig:
		if !*quiet {
			fmt.Fprintf(stderr, "corkd: received %s, shutting down\n", s)
		}
		srv.Close()
		<-done
		return ExitOK
	case err := <-done:
		srv.Close()
		if err != nil {
			fmt.Fprintf(stderr, "corkd: %v\n", err)
			return ExitRuntime
		}
		return ExitOK
	}
}
