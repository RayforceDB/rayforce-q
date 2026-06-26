# Baking rayforce-kx into a binding

`rayforce-kx` is the shared KDB+ wire-format core for RayforceDB. It is **not a
standalone library** — it is C source you compile *into* a binding's native
extension, next to the rayforce core. This guide shows how a binding
(Python, Rust, …) consumes it so every binding ships the identical wire
implementation.

The whole integration is three steps: **pin → compile → glue.**

---

## 1. Pin a version

All bindings must build the *same* `kx.c`, so pin a tag or commit — never track
a moving branch. The current version is in [`VERSION`](./VERSION); releases are
git tags (e.g. `v0.1.0`).

Pick whichever fetch mechanism fits your build:

| Mechanism            | When to use                                              |
| -------------------- | ------------------------------------------------------- |
| Clone at build time  | Default. Clone `--branch <tag> --depth 1` into a temp dir. |
| Git submodule        | If you want the pinned commit tracked in your repo tree. |
| Vendoring            | If your build must be fully offline; copy `kx.c`/`kx.h` and record the version. |

Expose an override so developers can build against a local checkout (see the
Python reference below).

---

## 2. Compile `kx.c` into your native build

`kx.c` depends only on the **rayforce core**, which your binding already builds
against. Its requirements:

- Include paths: the core's public `include/` (for `<rayforce.h>`) **and** its
  `src/` (for the internal `table/sym.h`, used by `RAY_SYM_W64`).
- No other dependencies. `kx.c` defines its own `RAY_ATTR_DICT` and
  `ray_scalar_elem_size` fallbacks, so it needs nothing from your binding.
- Standard POSIX sockets (`<sys/socket.h>`, `<netdb.h>`).

Drop `kx.c`/`kx.h` next to your extension sources and compile with the same
flags as the rest of your native code, e.g.:

```sh
cc -c kx.c -I<core>/include -I<core>/src -o kx.o
```

Then link `kx.o` into your extension/binary. Your glue includes `"kx.h"` to
call into it.

---

## 3. Write a thin glue layer

`kx.h` exposes exactly three entry points over rayforce core `ray_t`:

```c
int    kx_connect(const char *host, int port);              /* -> slot >= 0, or KX_ERR_* */
int    kx_close(int slot);                                  /* -> 0, or -1 (bad handle)  */
ray_t *kx_send(int slot, ray_t *msg, char *err, size_t n);  /* -> ray_t*, or NULL + err  */
```

Your glue does two jobs:

1. **Convert** your binding's native values to/from `ray_t` (host string + port
   in; decoded `ray_t` out).
2. **Map results onto your error model:**
   - `kx_connect` < 0 → branch on `KX_ERR_SOCKET` / `KX_ERR_HANDSHAKE` /
     `KX_ERR_FULL`.
   - `kx_send` returns `NULL` → transport/serialization failure; the reason is
     in your `err` buffer.
   - `kx_send` returns a `ray_t` that is itself a `RAY_ERROR` → a KDB+
     server-side error (carries the q error text). Surface it as a normal
     error, not a value.

### Reference: CPython binding

The Python binding (`rayforce-py`) is the canonical example. ~80 lines of glue
wrap the three calls as the `kdb_connect` / `kdb_close` / `kdb_send` extension
methods, and its build pulls a pinned `rayforce-kx` and copies `kx.*` into the
extension's source dir before compiling:

- glue: `rayforce/capi/raypy_kdb.c`
- build wiring: `Makefile` (`RAYFORCE_KX_GITHUB` / `RAYFORCE_KX_REF` /
  `RAYFORCE_KX_LOCAL_PATH`, copy into `pyext/`) and
  `scripts/prepare_build.sh`.

The build pattern, distilled:

```make
RAYFORCE_KX_GITHUB     ?= https://github.com/RayforceDB/rayforce-kx.git
RAYFORCE_KX_REF        ?= v0.1.0          # pin
RAYFORCE_KX_LOCAL_PATH ?=                 # local-checkout override

pull_kx:
	@if [ -n "$(RAYFORCE_KX_LOCAL_PATH)" ]; then \
		rsync -a --exclude='.git' "$(RAYFORCE_KX_LOCAL_PATH)/" tmp/rayforce-kx/; \
	else \
		git clone --depth 1 --branch $(RAYFORCE_KX_REF) $(RAYFORCE_KX_GITHUB) tmp/rayforce-kx; \
	fi
	cp tmp/rayforce-kx/kx.* <your-native-src-dir>/   # picked up by your build
```

### Reference: rayfall builtins

The [`test/`](./test) directory shows a second, non-Python glue:
`test/kx_builtins.c` binds the three calls as rayfall language builtins. It is a
compact template for a glue layer that isn't CPython.

### Sketch: Rust binding

A Rust binding follows the same shape — compile `kx.c` in `build.rs` (via `cc`),
declare the three functions, and convert at the `ray_t` boundary:

```rust
// build.rs
cc::Build::new()
    .file("vendor/rayforce-kx/kx.c")
    .include(core_include_dir)   // <rayforce.h>
    .include(core_src_dir)       // table/sym.h
    .compile("kx");

// FFI
extern "C" {
    fn kx_connect(host: *const c_char, port: c_int) -> c_int;
    fn kx_close(slot: c_int) -> c_int;
    fn kx_send(slot: c_int, msg: *mut RayT, err: *mut c_char, n: usize) -> *mut RayT;
}
```

Map `kx_send`'s `NULL`/`RAY_ERROR`/value triple onto a `Result<RayValue, KxError>`
exactly as the C and Python glue do.

---

## Checklist

- [ ] Pinned to a tag/commit (not a branch).
- [ ] `kx.c` compiled with the core's `include/` **and** `src/` on the include path.
- [ ] `kx.o` linked into the extension.
- [ ] Glue maps `KX_ERR_*`, `NULL`, and a returned `RAY_ERROR` to your error model.
- [ ] A local-checkout override for development.
