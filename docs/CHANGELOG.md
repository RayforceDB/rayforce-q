# Changelog

All notable changes to `rayforce-q` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/), and the project adheres to [Semantic Versioning](https://semver.org/). Bindings pin a tag, so each release is a stable point they can build against.

## [2.1.1]

### Fixed

- **`make rayforce` failed under gcc's `-Werror`.** The sed patch that injects
  the Q serve hook into the core's `main.c` appended a compound statement after
  `if (poll) ray_runtime_set_poll(poll);` on the same line, which gcc rejects
  with `-Werror=misleading-indentation` (clang accepts it, so the break only
  surfaced on Linux builds). The injection now emits a single braced
  `if (poll) { ... }` block.

## [2.1.0]

### Added

- **Connections on the event loop** — `q_conn_attach(poll, fd)` puts an already-connected, already-handshaken `q_connect` fd under the poll's rx machine, with `q_conn_send` / `q_conn_close` for round-trips and teardown. `q_conn_send` writes its SYNC frame and then pumps the connection until the matching RESPONSE arrives, dispatching — not swallowing — whatever else lands in between. One rx state machine now serves both directions: inbound connections from the listener and outbound ones attached here. Mirrors `conn_pump` / `sync_send` in the rayforce core's own IPC.
- **Rayforce can be a q subscriber.** A pushed frame is dispatched instead of ignored, so `(.q.send h ".net.sub[0]")` once is a complete subscription: packets then arrive on the event loop, with no polling and no timer. A non-string payload goes through `ray_eval`, which makes a publisher's `` (`upd;packet) `` call `upd` — exactly what q's `.z.ps` does with `value x`, so a subscriber reads the same in Rayfall as it does in a q RDB. List arguments are marked literal-fallback first, as `core/ipc.c` does, so symbols inside a payload stay data instead of resolving against the environment.
- **`--poll` mode in the test driver**, plus `test/rfl/push/`: pushed lists, tables and dicts-of-tables, ordering across several frames in one read, async string payloads, a missing handler, a nested send on a busy handle, and use-after-close. The client suite now runs twice — with and without an event loop — because the poll path has to be a drop-in for every request/response case, not a feature bolted on next to it.

### Fixed

- **A pushed frame was read as the next response.** The client is a blocking socket nobody watches, so an async frame from a publisher sat in the receive buffer until the following `q_send` consumed it as its own reply — and from then on every response on that connection was one frame out of step. Frames are now routed by message type: RESPONSE to the sender parked on the connection, everything else to evaluation. The bug was timing-dependent, and therefore invisible whenever no push happened to land between two requests.
- **Connection fds leaked.** `q_on_close` freed the per-connection state but never closed the socket, so every connection the server dropped — protocol error, oversized handshake, peer close — leaked its descriptor. It now closes the fd, like `ipc_on_close` in the core, and releases a RESPONSE that was deposited for a sync wait which died mid-round-trip.

### Changed

- **`.q.connect` attaches to the event loop** when the host runtime has one, and the handle is then a poll selector id rather than a raw socket fd. Handles stay opaque — `.q.send` and `.q.close` take exactly what `.q.connect` returned — and a host without a poll (a binding embedding only the client, the `.rfl` test driver) keeps the blocking path and the fd handle unchanged. `q.c` itself is untouched: its contract of a bare, thread-safe, poll-free fd is what bindings without an event loop depend on.
- **A non-string request is no longer rejected.** `eval_request` used to answer `only string (char-vector) queries are supported`; it now falls through to `ray_eval`, the same contract the native IPC server has.

## [2.0.2]

### Fixed

- **Null gate on decode**: rayforce aggregation kernels consult null sentinels only when a vector carries the `RAY_ATTR_HAS_NULLS` gate, which a raw payload copy never sets. Decoded vectors with q nulls (`0N`, `0n`, null temporals, `0Ng`) produced wrong aggregates — `avg` folded `INT64_MIN` in as data, `min` returned the sentinel itself — while the payload bytes were correct. The decoder now scans each sentinel-supporting vector (fixed-width, datetime, and real paths) and flips the gate via one idempotent `ray_vec_set_null` on the first hit.
- **SYM vector encode ignored the resolution domain**: splayed/mmap SYM columns store ids as positions in the table's symfile domain, but the encoder resolved them against the runtime intern table — a served splayed table returned empty or wrong symbols. Ids now resolve through `ray_sym_vec_domain` / `ray_sym_domain_str`; the runtime-domain case is unchanged.
- **SYM vector encode assumed 8-byte ids**: narrow storage widths (`RAY_SYM_W8/W16/W32`, the attrs low bits) were read as `int64_t`, walking off the payload — encoding a narrow SYM vector (any splayed column with a small dictionary) crashed the server. Element reads are now width-aware.

## [2.0.1]

### Fixed

- **Datetime (`KZ`) decode**: a q datetime — an IEEE-754 double of fractional days since 2000-01-01 — was copied raw into a `RAY_TIMESTAMP` (i64 nanoseconds), reinterpreting the double's bit pattern instead of converting the value. Timestamps came back silently wrong by ~121 years (e.g. `2026.07.10T09:30:00.000` decoded to `2147.11.17D00:15:16.509272747`), and because the result carried the correct type code, consumers could not detect the corruption. Both the vector and the atom (previously unhandled — it errored with `unsupported wire type`) now convert days → nanoseconds, with the q null (`0n`) mapping to the null timestamp.


## [2.0.0]

### Added

- **Q server core** — a second language-neutral pair, `q_server.c` / `q_server.h`, mirroring the client: `q_serve(poll, port)` registers a non-blocking Q listener on a rayforce poll, evaluating each request as Rayfall and replying Q-encoded.
- **Embedded server**: `rayforce -q PORT` serves Rayfall over the Q wire on the REPL's own event loop (REPL stays interactive).
- **String-column wire support**: a column of strings (`RAY_STR`) serializes as a q general list of char-vectors and round-trips back.

### Fixed

- **GUID atom serialization**: a GUID *atom* keeps its 16 bytes in a child block, so `q_encode` was emitting pointer bytes for a single GUID


## [1.0.0]

First stable release of the Q IPC wire-format core: a language-neutral
`q.c` / `q.h` pair that bindings compile into their native extension alongside
the rayforce core.

### Added

- **Public API** over rayforce `ray_t`:
  - `q_connect(host, port, user, password, timeout_ms)` — open a connection,
    with optional username/password authentication and a connect + per-operation
    send/recv timeout (`<= 0` blocks).
  - `q_send(fd, msg, err, n)` — synchronous request/response.
  - `q_close(fd)`.
  - Split form `q_encode` / `q_exchange` / `q_decode`, so a binding can release a
    runtime lock (e.g. the CPython GIL) around just the blocking network wait:
    encode/decode touch the rayforce symbol table (hold the lock), `q_exchange`
    is pure socket I/O (release it).
- **Connection handle is the raw socket fd** — thread-safe and unbounded, with
  no shared connection table.
- **Wire-format coverage**
  - Atoms and vectors: bool, byte, short, int, long, real, float, char, symbol,
    guid.
  - Temporal: date, time, timestamp.
  - Nested: general lists, tables, and dicts (decoded to native rayforce dicts).
  - Typed nulls (`0N`, `0Nh`, `0n`, …) round-trip via matching sentinels.
  - Decompression of compressed server responses.
- **Error handling**: a q server-side error surfaces as a `RAY_ERROR` whose code
  and message both carry the q error text; transport/serialization failures
  return a short reason in the caller's `err` buffer.
- **Safety guards**: rejects messages larger than 4 GiB and big-endian peers.
- **Embedded binary** (`make rayforce`): a `rayforce` binary with the Q client
  compiled in and exposed as the `.q.connect` / `.q.send` / `.q.close` rayfall
  env functions, so any script or REPL session can query a Q server
  (`embed/q_env.c` is the registration shim).
- **Tests**: rayfall integration suite (`test/`) — connection lifecycle, every
  atom/vector type, temporal, collections, server errors, authentication, and
  nulls — run against a live `q` server, with a GitHub Actions workflow.
- **Docs**: [`README.md`](../README.md) overview and
  [`INTEGRATING.md`](./INTEGRATING.md) pin → compile → glue guide with Python,
  rayfall, and Rust examples.

### Notes

- Not a standalone library: it requires the rayforce core's `<rayforce.h>` and
  `table/sym.h` on the include path and links nothing else (defines its own
  `RAY_ATTR_DICT` / `ray_scalar_elem_size` fallbacks).
- A Q keyed table decodes to a 2-element `(keys, values)` list (rayforce has
  no keyed-table type); vector attributes (`s#`/`u#`/`p#`) are not preserved.

[1.0.0]: https://github.com/RayforceDB/rayforce-q/releases/tag/1.0.0
