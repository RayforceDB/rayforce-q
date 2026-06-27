# Changelog

All notable changes to `rayforce-q` are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/), and the project adheres to [Semantic Versioning](https://semver.org/). Bindings pin a tag, so each release is a stable point they can build against.

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
