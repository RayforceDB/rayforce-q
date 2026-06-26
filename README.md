# rayforce-q

Q IPC wire-format client for [RayforceDB](https://github.com/RayforceDB),
factored out as a small **language-neutral C core** so every RayforceDB
language binding (Python, Rust, …) can link the *same* version of the plugin.

## What's here

| File    | Purpose                                                            |
| ------- | ----------------------------------------------------------------- |
| `q.h`  | Public API: `q_connect`, `q_close`, `q_send`.                  |
| `q.c`  | Q wire-format serialize/deserialize + blocking-socket client.  |

The core operates purely on rayforce core `ray_t` objects and the rayforce
public API (`<rayforce.h>`). It contains **no** binding-specific code (no
CPython, no Rust FFI). A binding provides a thin glue layer that:

1. converts its native values to/from `ray_t`, and
2. maps `q_*` return codes / error strings onto its own exception model.

## API

```c
#include "q.h"

/* host/port + optional auth + connect/op timeout (ms; <=0 blocks). */
int    q_connect(const char *host, int port, const char *user,
                  const char *password, int timeout_ms);   /* -> fd >= 0, or Q_ERR_* */
int    q_close(int fd);                                    /* -> 0, or -1 (bad fd)     */
ray_t *q_send(int fd, ray_t *msg, char *err, size_t n);    /* -> ray_t*, or NULL + err */

/* Split form, so a binding can release a runtime lock (e.g. the CPython GIL)
 * around just the blocking network wait: */
int    q_encode(ray_t *msg, uint8_t **req, int64_t *req_len, char *err, size_t n);
int    q_exchange(int fd, const uint8_t *req, int64_t req_len, uint8_t **resp,
                   int64_t *resp_len, int *compressed, char *err, size_t n);
ray_t *q_decode(uint8_t *resp, int64_t resp_len, int compressed, char *err, size_t n);
```

The connection handle is the raw socket fd, so the client is thread-safe and
unbounded (no shared connection table). `q_send` returns a freshly-owned
`ray_t`, which may itself be a `RAY_ERROR` carrying a Q server-side error; a
`NULL` return instead signals a transport/serialization failure, with a short
reason written to `err`. `q_encode`/`q_decode` touch the rayforce symbol
table (hold the lock); `q_exchange` is pure socket I/O (release the lock).

## Building into a binding

`rayforce-q` is not a standalone library — it is compiled **into** a binding's
build alongside the rayforce core, which supplies `<rayforce.h>` and the
`table/sym.h` internal header. Drop `q.c`/`q.h` next to the binding's sources
and compile with the core's include paths.

See **[INTEGRATING.md](./INTEGRATING.md)** for the full pin → compile → glue
guide, with Python (reference), rayfall, and Rust examples. The RayforceDB
Python binding (`Makefile` / `scripts/prepare_build.sh` + its C glue) is the
canonical integration: it pulls this repo at a pinned ref and copies `q.*` into
its `pyext/` build dir.

## Tests

Integration tests (rayfall, run against a live `q` server) live in
[`test/`](./test) and run in CI. Bindings only ever compile `q.c` / `q.h`, so
the test scaffolding is inert for them. See [`test/README.md`](./test/README.md).

## Versioning

Bindings pin a specific tag/commit of this repo so they all ship the identical
wire implementation. The current version is in [`VERSION`](./VERSION).
