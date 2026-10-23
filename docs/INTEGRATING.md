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
  `src/` (for the internal `table/sym.h`, used by `RAY_SYM_W64`, and
  `core/ipc.h` and `core/sock.h`, for keepalive).
- A core with `ray_sock_set_keepalive`: v2.11.0 or later (core #589).
  `q.c` uses it and its constants, so an older core fails to compile.
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
int    q_connect_opts(const char *host, int port, const char *user,
                      const char *password, int timeout_ms,
                      int keepalive_ms);                   /* q_connect, keepalive chosen */
int    q_close(int fd);
ray_t *q_send(int fd, ray_t *msg, char *err, size_t n);   /* convenience: encode+exchange+decode */
```

If you also compiled `q_server.c`, `q_server.h` adds the one server entry point.
Call it with the runtime's poll to start a listener on that event loop.

```c
int64_t q_serve(ray_poll_t *poll, int port);              /* be a Q server */
```

Your glue does two jobs:

1. **Convert** your binding's native values to/from `ray_t` (host/port/creds in;
   decoded `ray_t` out). `user`/`password` may be `NULL`/`""`; `timeout_ms <= 0`
   blocks.
2. **Map results onto your error model:**
   - `q_connect` < 0: branch on `Q_ERR_SOCKET` / `Q_ERR_HANDSHAKE` / `Q_ERR_TIMEOUT`.
   - `q_send` returns `NULL` → transport/serialization failure; the reason is in your `err` buffer.
   - `q_send` returns a `ray_t` that is itself a `RAY_ERROR`: a Q server-side error (the q error text is in both the error code and message). Surface it as a normal error, not a value.

### Keepalive

Every `q_connect` connection has kernel TCP keepalive, so a peer that vanishes
without closing (a suspended host, a NAT or VPN that drops the flow) is
noticed instead of a read waiting forever. `q_connect_opts` takes the same
`keepalive_ms` as the core's `ray_ipc_connect_opts`:

| `keepalive_ms`         | Armed                                                                  |
|------------------------|------------------------------------------------------------------------|
| `Q_KEEPALIVE_AUTO` (0) | keepalive at the core's 60 s default; no bound on unacknowledged data |
| `Q_KEEPALIVE_OFF` (-1) | nothing                                                                |
| `n > 0`                | keepalive at budget `n` ms, plus `TCP_USER_TIMEOUT` of `n` ms          |

A budget maps onto the socket as the core's `ray_sock_set_keepalive` does: the
first probe after `n/2` of idle, then a probe every `n/6`, each in whole
seconds of at least 1. On Linux, with a budget, `TCP_USER_TIMEOUT` decides:
the peer is dead at the first probe tick (`idle + k·interval`) at or after
`n`, up to one interval late (25 s → 28 s). Otherwise (`Q_KEEPALIVE_AUTO`, or
a budget on another platform) it is dead after three unanswered probes, at
`idle + 3·interval` in whole seconds, which can come up to a third early
(10 s → 8 s); `Q_KEEPALIVE_AUTO` is 60 s. Probes only go out while nothing is
in flight, so a live peer that is slow to answer is never affected.
`TCP_USER_TIMEOUT` also fails a send that stays unacknowledged for `n` ms
(`TCP_RXT_CONNDROPTIME` on macOS), which includes a live peer that stops
reading, so it comes only with an explicit budget.

Keepalive is armed after the handshake, on the blocking socket, and survives
`q_conn_attach`. Arming is best-effort, as in the core: if `SO_KEEPALIVE`
itself fails, the connection is returned without keepalive; a tuning option
the platform lacks or rejects keeps its system default.

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

If the reply is a wide table and you keep only some of its columns, decode
with `q_decode_cols` instead. It takes the same arguments plus the column
names, and returns a table of only those columns, in the order given. The
others are skipped without being built, so you don't pay for one string per
cell or one symbol intern per cell in columns you would drop anyway:

```c
const char *keep[] = {"date", "sym", "px"};
ray_t *result = q_decode_cols(resp, resp_len, compressed, keep, 3, err, sizeof err);
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
