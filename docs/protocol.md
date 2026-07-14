# The corkd wire protocol

corkd speaks newline-delimited JSON over a unix stream socket: one JSON
object per line in, one JSON object per line out. Any language's JSON
encoder plus a socket is a complete client — and so is a human:

```bash
printf '%s\n' '{"op":"set","key":"k","value":"v"}' '{"op":"get","key":"k"}' \
  | nc -U -q1 "$XDG_RUNTIME_DIR/corkd.sock"
```

A request line is at most 1 MiB. Requests on one connection are answered
in order. The `watch` op is special: it converts the rest of the
connection into a one-way event stream.

## Requests

Every request has an `op`; the other fields depend on it. Optional
numeric fields distinguish "absent" from "zero" — `if_version: 0` is a
meaningful condition (the key must not exist).

| Field | Type | Used by | Meaning |
|---|---|---|---|
| `op` | string | all | one of `ping set get del keys dump incr wait watch stats` |
| `key` | string | set get del incr wait | non-empty UTF-8, no control chars, ≤256 B |
| `value` | string | set | payload, ≤64 KiB (limits configurable at `serve`) |
| `ttl_ms` | int | set incr | expiry in ms; omitted = no expiry (incr: keep current) |
| `if_version` | int | set del | CAS: current version must equal this; 0 = must not exist |
| `if_absent` | bool | set | create-only write, fails with `exists` |
| `by` | int | incr | delta, default 1, may be negative |
| `prefix` | string | keys dump watch | key prefix filter, empty = everything |
| `equals` | string | wait | succeed only when the value equals this |
| `gone` | bool | wait | succeed when the key is deleted or expired |
| `timeout_ms` | int | wait | 1..600000, default 10000 |
| `state` | bool | watch | replay current entries before live events |

## Responses

```json
{"ok":true,"key":"lock/deploy","value":"agent-a","version":2,"ttl_ms":29863}
{"ok":false,"error":"version_conflict","message":"expected version 1, current is 2","version":2}
```

`ok:false` responses carry a stable `error` code, a human `message`, and
— for CAS failures — the key's current `version` so a retry needs no
extra round trip. Codes: `not_found`, `version_conflict`, `exists`,
`not_number`, `timeout`, `bad_request`, `lagged`, `internal`.

`keys`/`dump` return a `keys` array of `{key, version, ttl_ms?}` objects
(`dump` adds `value`); `stats` returns a `stats` object; `ping` returns
the server version in `server`.

## Versions

The board keeps one global sequence. Every successful mutation (put,
del, incr, expiry) consumes one value, and a live entry's `version` is
the sequence value of the write that produced it. Versions are therefore
unique across the whole board and never reused, so a CAS can never be
fooled by a key that was deleted and re-created (no ABA).

## Watch streams

After a `watch` request the server sends one event per line and reads
nothing further; closing your end unsubscribes.

```json
{"event":"put","key":"job/1","value":"queued","version":5,"seq":5}
{"event":"sync","seq":5}
{"event":"del","key":"job/1","version":5,"seq":6}
{"event":"expire","key":"lock/deploy","version":2,"seq":7}
```

- `put` carries the new value and version (also emitted by `incr`).
- `del` / `expire` carry the removed entry's last version.
- `sync` is sent only with `state:true`, after the snapshot replay:
  everything before it is current state, everything after it is live.
  Snapshot and subscription are taken atomically under the store lock,
  so no mutation can be missed or seen twice around the marker.
- `lagged` is the final event when your consumer fell a full buffer
  (default 256 events) behind and was dropped; re-watch with `state`.

## wait

`wait` is a one-shot server-side block: the server snapshots the key and
subscribes in one atomic step, answers immediately if the condition
already holds, otherwise responds with the first satisfying event — or
`{"ok":false,"error":"timeout"}` when `timeout_ms` elapses. Conditions:
default = key exists, `equals` = key has exactly this value, `gone` =
key absent.

## Transport guarantees

- The socket is created mode 0600 — the board is private to your user.
- A stale socket file (crashed server) is silently taken over; a socket
  with a live server behind it is refused, so two boards can never split
  one path.
- Writes are applied under a single lock: clients always observe a total
  order, and `seq` gaps never appear in a healthy stream.
