# Baking rayforce-q into a binding

`rayforce-q` is the shared Q wire-format core for RayforceDB. It is **not a
standalone library** — it is C source you compile *into* a binding's native
extension, next to the rayforce core. This guide shows how a binding 
consumes it so every binding ships the identical wire implementation.

---

## 1. Pin a version

All bindings must build the *same* `q.c`, so pin a tag or commit — never track
a moving branch. The current version is in [`VERSION`](./VERSION); releases are
git tags (e.g. `1.0.0`).

---

## 2. Compile `q.c` into your native build

`q.c` depends only on the **rayforce core**, which your binding already builds
against. Its requirements:

- Include paths: the core's public `include/` (for `<rayforce.h>`) **and** its
  `src/` (for the internal `table/sym.h`, used by `RAY_SYM_W64`).
- No other dependencies. `q.c` defines its own `RAY_ATTR_DICT` and
  `ray_scalar_elem_size` fallbacks, so it needs nothing from your binding.
- Standard POSIX sockets (`<sys/socket.h>`, `<netdb.h>`).

Drop `q.c`/`q.h` next to your extension sources and compile with the same
flags as the rest of your native code, e.g.:

```sh
cc -c q.c -I<core>/include -I<core>/src -o q.o
```

Then link `q.o` into your extension/binary. Your glue includes `"q.h"` to call into it.

---

## 3. Write a thin glue layer

`q.h` exposes these entry points over rayforce core `ray_t`. The connection
handle is the raw socket fd (>= 0), so there is no shared connection table —
the client is thread-safe and unbounded.

```c
int    q_connect(const char *host, int port, const char *user,
                  const char *password, int timeout_ms);   /* -> fd >= 0, or Q_ERR_* */
int    q_close(int fd);
ray_t *q_send(int fd, ray_t *msg, char *err, size_t n);   /* convenience: encode+exchange+decode */
```

Your glue does two jobs:

1. **Convert** your binding's native values to/from `ray_t` (host/port/creds in;
   decoded `ray_t` out). `user`/`password` may be `NULL`/`""`; `timeout_ms <= 0`
   blocks.
2. **Map results onto your error model:**
   - `q_connect` < 0: branch on `Q_ERR_SOCKET` / `Q_ERR_HANDSHAKE` / `Q_ERR_TIMEOUT`.
   - `q_send` returns `NULL` → transport/serialization failure; the reason is in your `err` buffer.
   - `q_send` returns a `ray_t` that is itself a `RAY_ERROR`: a Q server-side error (the q error text is in both the error code and message). Surface it as a normal error, not a value.

### Releasing a runtime lock around the network wait

`q_send` runs everything under the caller's current lock. If your runtime has a
global lock that should be released during the blocking server round-trip (the
CPython GIL is the canonical case), call the three-step form instead — encode
and decode touch the rayforce symbol table (hold the lock); `q_exchange` is
pure socket I/O (release the lock):

Python example:
```c
uint8_t *req; int64_t req_len;
if (q_encode(msg, &req, &req_len, err, sizeof err) < 0) { /* error */ }

uint8_t *resp; int64_t resp_len; int compressed; int rc;
Py_BEGIN_ALLOW_THREADS;                       /* release the GIL */
rc = q_exchange(fd, req, req_len, &resp, &resp_len, &compressed, err, sizeof err);
Py_END_ALLOW_THREADS;
free(req);
if (rc < 0) { /* error */ }

ray_t *result = q_decode(resp, resp_len, compressed, err, sizeof err);
free(resp);
```

### Reference: CPython binding

The Python binding (`rayforce-py`) is the canonical example. A thin glue layer
wraps the three calls as the binding's connect / close / send extension methods,
and its build pulls a pinned `rayforce-q` and copies `q.*` into the
extension's source dir before compiling:

- glue: the binding's C glue file under `rayforce/capi/`
- build wiring: `Makefile` (`RAYFORCE_Q_GITHUB` / `RAYFORCE_Q_REF` /
  `RAYFORCE_Q_LOCAL_PATH`, copy into `pyext/`) and
  `scripts/prepare_build.sh`.

The build pattern, distilled:

```make
RAYFORCE_Q_GITHUB     ?= https://github.com/RayforceDB/rayforce-q.git
RAYFORCE_Q_REF        ?= 0.2.0           # pin
RAYFORCE_Q_LOCAL_PATH ?=                 # local-checkout override

pull_q:
	@if [ -n "$(RAYFORCE_Q_LOCAL_PATH)" ]; then \
		rsync -a --exclude='.git' "$(RAYFORCE_Q_LOCAL_PATH)/" tmp/rayforce-q/; \
	else \
		git clone --depth 1 --branch $(RAYFORCE_Q_REF) $(RAYFORCE_Q_GITHUB) tmp/rayforce-q; \
	fi
	cp tmp/rayforce-q/q.* <your-native-src-dir>/   # picked up by your build
```

### Reference: rayfall builtins

The [`test/`](./test) directory shows a second, non-Python glue:
`test/q_builtins.c` binds the three calls as rayfall language builtins. It is a
compact template for a glue layer that isn't CPython.
