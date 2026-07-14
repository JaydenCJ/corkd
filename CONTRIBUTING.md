# Contributing to corkd

Issues, discussions and pull requests are all welcome.

## Getting started

You need Go ≥1.22; nothing else.

```bash
git clone https://github.com/JaydenCJ/corkd && cd corkd
go build ./...
go test ./...
bash scripts/smoke.sh
```

`scripts/smoke.sh` builds the binary, starts a real board on a temp
socket, and drives the whole CLI surface — CAS races, TTL expiry, a
blocking wait, a watch replay, and a SIGTERM shutdown; it must finish by
printing `SMOKE OK`.

## Before you open a pull request

1. `gofmt -l .` reports nothing (formatting is enforced).
2. `go vet ./...` passes with no findings.
3. `go test ./...` passes (91 deterministic tests, no network, no sleeps
   in the store/proto layers — TTL logic runs on an injected clock).
4. `bash scripts/smoke.sh` prints `SMOKE OK`.
5. Add tests for behavior changes; keep logic in pure, unit-testable
   modules (the store never touches sockets, the server never touches
   argv).

## Ground rules

- Keep dependencies at zero — corkd is standard library only, and PRs
  that add a module need extraordinary justification.
- No network, ever: the only I/O surface is a mode-0600 unix socket. No
  telemetry.
- Protocol changes are semver-relevant: anything that alters the JSON
  shapes in `internal/proto` needs a matching update to
  `docs/protocol.md` and a shape-pinning test.
- Determinism first: identical operations must produce identical event
  streams, including all orderings (sweeps emit in key order for exactly
  this reason).
- Code comments and doc comments are written in English.

## Reporting bugs

Include the output of `corkd version`, the exact commands (or protocol
lines) you sent, what the board contained (`corkd dump`), and what you
expected. For wait/watch issues, the event stream printed by
`corkd watch --state` is exactly what the server saw and is the most
useful thing you can attach.

## Security

Please do not open public issues for security problems; use GitHub's
private vulnerability reporting on this repository instead.
