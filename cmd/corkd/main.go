// Command corkd is a shared blackboard for co-located agents: a watchable
// key-value store with compare-and-swap and TTL, served over a unix socket.
package main

import (
	"os"

	"github.com/JaydenCJ/corkd/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdout, os.Stderr))
}
