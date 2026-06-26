# rayforce-kx

KDB+ IPC wire-format client for [RayforceDB](https://github.com/RayforceDB),
factored out as a small **language-neutral C core** so every RayforceDB
language binding (Python, Rust, …) can link the *same* version of the plugin.

## What's here

| File    | Purpose                                                            |
| ------- | ----------------------------------------------------------------- |
| `kx.h`  | Public API: `kx_connect`, `kx_close`, `kx_send`.                  |
| `kx.c`  | KDB+ wire-format serialize/deserialize + blocking-socket client.  |

The core operates purely on rayforce core `ray_t` objects and the rayforce
public API (`<rayforce.h>`). It contains **no** binding-specific code (no
CPython, no Rust FFI). A binding provides a thin glue layer that:

1. converts its native values to/from `ray_t`, and
2. maps `kx_*` return codes / error strings onto its own exception model.

## API

```c
#include "kx.h"

int    kx_connect(const char *host, int port);              /* -> slot >= 0, or KX_ERR_* */
int    kx_close(int slot);                                  /* -> 0, or -1 (bad handle)  */
ray_t *kx_send(int slot, ray_t *msg, char *err, size_t n);  /* -> ray_t*, or NULL + err  */
```

`kx_send` returns a freshly-owned `ray_t`. That object may itself be a
`RAY_ERROR` carrying a KDB+ server-side error string; a `NULL` return instead
signals a transport/serialization failure, with a short reason written to
`err`.

## Building into a binding

`rayforce-kx` is not a standalone library — it is compiled **into** a binding's
build alongside the rayforce core, which supplies `<rayforce.h>` and the
`table/sym.h` internal header. Drop `kx.c`/`kx.h` next to the binding's sources
and compile with the core's include paths.

See **[INTEGRATING.md](./INTEGRATING.md)** for the full pin → compile → glue
guide, with Python (reference), rayfall, and Rust examples. The RayforceDB
Python binding (`Makefile` / `scripts/prepare_build.sh` + `raypy_kdb.c`) is the
canonical integration: it pulls this repo at a pinned ref and copies `kx.*` into
its `pyext/` build dir.

## Tests

Integration tests (rayfall, run against a live `q` server) live on the
[`tests`](https://github.com/RayforceDB/rayforce-kx/tree/tests) branch, kept off
`main` so bindings pull only `kx.c` / `kx.h`.

## Versioning

Bindings pin a specific tag/commit of this repo so they all ship the identical
wire implementation. The current version is in [`VERSION`](./VERSION).
