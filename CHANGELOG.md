# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-13

### Added

- In-memory blackboard with globally unique, monotonically increasing
  write versions (one sequence across all keys), making CAS immune to
  ABA even across delete/re-create.
- Compare-and-swap writes and deletes: `if_version` (0 = create-only)
  and `if_absent`, with conflict responses that carry the current
  version so losers can retry without a second round trip.
- TTL expiry with exact-boundary semantics: lazy on access, actively
  swept on a timer, and always observable as `expire` events; plain
  writes replace the TTL, `incr` preserves it unless told otherwise.
- Atomic `incr` counters (negative deltas allowed) for semaphores and
  progress tallies.
- Prefix-scoped watches streamed as NDJSON, with optional atomic
  state-replay (`put`… then `sync`) so late joiners never miss or
  double-see a change; slow consumers are dropped with a `lagged`
  marker instead of stalling the board.
- Blocking `wait` on a single key — existence, `--equals VALUE`, or
  `--gone` — built on an atomic snapshot+subscribe, with timeouts.
- Newline-delimited JSON protocol over a mode-0600 unix socket
  (`docs/protocol.md`), speakable with `nc -U`; stale socket takeover
  and live-socket refusal on startup.
- CLI: `serve`, `set`, `get`, `del`, `incr`, `keys`, `dump`, `wait`,
  `watch`, `stats`, `ping`, `version`, with script-friendly exit codes
  (0 ok / 1 condition not met / 2 usage / 3 runtime) and `--json`
  output everywhere.
- Runnable examples (`examples/mutex.sh`, `examples/pipeline.sh`) and a
  protocol reference (`docs/protocol.md`).
- 91 deterministic offline tests (fake-clock TTL, real-socket protocol
  and CLI integration, race-detector clean) and `scripts/smoke.sh`.

[0.1.0]: https://github.com/JaydenCJ/corkd/releases/tag/v0.1.0
