# corkd examples

Two runnable scripts, both offline and self-contained: each builds
corkd, starts a private board on a temp socket, runs the scenario, and
cleans up after itself.

## mutex.sh

The classic multi-agent failure mode, fixed: three workers race for the
same lock with `set --if-absent --ttl`, exactly one wins, the losers
block on `wait --gone` until it is released. The TTL means a crashed
holder can never deadlock the others.

```bash
bash examples/mutex.sh
```

## pipeline.sh

A producer/consumer pipeline with no polling: the consumer arms
`corkd wait` on the keys it needs, the producer publishes results as it
finishes them, and a `watch` stream logs every mutation on the board as
NDJSON — the audit trail comes for free.

```bash
bash examples/pipeline.sh
```

Both scripts assert on their own output and exit non-zero on any
mismatch, so they double as living documentation that stays honest.
