# rayforce-q

Q IPC wire-format client for [RayforceDB](https://github.com/RayforceDB) — a small, **language-neutral C core and compiled binary**

The core operates **purely** on rayforce `ray_t` objects and the public `<rayforce.h>` API

# Embedded binary

`make rayforce` builds a `rayforce` binary with **both** the Q client and the Q server compiled in.

### Client

The client is exposed as `.q.*` rayfall env functions, so any script or REPL session can query a Q server:

```clojure
(set h (.q.connect "localhost" 5000 "" "" 0))    ;; host port [user password [timeout_ms]]
(.q.send h "([] a:1 2 3; b:`x`y`z)")             ;; -> a rayforce table
(.q.close h)
```

Connections live on the runtime's event loop, so the peer can **push** to them — which is all a q subscription is. Frames are routed by message type: a response wakes the `.q.send` waiting for it, anything else is evaluated. A publisher's `` (`upd;packet) `` therefore calls `upd`, the same dispatch q performs with `value x`:

```clojure
(set upd (fn [packet] (insert 'mtr (get packet 'metrics))))
(set h (.q.connect "localhost" 5000 "" "" 5000))
(.q.send h ".net.sub[0]")                        ;; subscribe once — packets then arrive on their own
```

Inside a handler, `(.q.handle)` is the connection the frame came in on (the value `.q.connect` returned), `-1` anywhere else — so one process can subscribe to several publishers and still tell whose packet it holds:

```clojure
(set upd (fn [packet] (record (cluster-of (.q.handle)) packet)))
```

A host without a poll (a binding that embeds only the client) keeps the plain blocking `q.c` path and its raw-fd handle.

Every `.q.connect` connection has TCP keepalive at the core's 60 s default, so a publisher that vanishes without closing (a dropped VPN or NAT flow) fails the connection instead of leaving a subscription waiting forever.

### Server

The server is exposed via the `-q PORT` flag — it shares the REPL's event loop, so the REPL stays interactive while it serves Q clients:

```sh
./rayforce -q 25565        # serve Rayfall over the Q wire on 127.0.0.1:25565 (loopback only)
./rayforce -q 0.0.0.0:25565   # every interface, explicitly
RAYFORCE_Q_BIND_HOST=10.0.0.5 ./rayforce -q 25565   # env fallback when -q has no host
```

An empty (or whitespace-only) request answers `(::)`, like kdb's `h ""`. An error's text travels with its category, so q sees `'domain: custom failure`. With a journal open (`.log.open`, `-l`/`-L`) every inbound **sync** q frame is journaled before it is evaluated, exactly as the core does for native IPC, and a failed journal write refuses the request with an `io` error; async frames are not journaled.

**Inbound connections** get the same handle, push and hook verbs as outbound ones, enough for a kdb-compatible pub/sub server:

```clojure
;; (.q.handle)         the connection whose frame (or close) is being handled; -1 elsewhere
;; (.q.post h msg)     send msg as a q ASYNC frame on any open q connection, inbound or outbound;
;;                     queued, never waits; null on success, `handle`/`io` errors otherwise
;; (.q.on 'sync  fn)   hook: gets the decoded request, REPLACES the default eval, its value is the reply
;; (.q.on 'async fn)   hook: gets the request, no reply
;; (.q.on 'close fn)   hook: gets the handle, once per inbound connection, just before teardown
;; (.q.on 'sync 0)     any non-lambda clears the hook
(.q.on 'sync (fn [r]
  (if (== (first r) '.net.sub)               ;; q: h(`.net.sub;5) -- a name the default eval could not call
      (do (set subs (concat subs (list (.q.handle)))) 5)
      (eval r))))
(.q.post (first subs) (list 'upd (dict ['a 'b] [1 2])))   ;; q client receives (`upd;`a`b!1 2) async
```

Rayforce stores q's legacy temporal kinds in the nearest engine type (`z` as
TIMESTAMP, `m` as DATE, and `u`/`v` as TIME), so replies normally use q
`p`/`d`/`t`. Reply-local wrappers select the narrower q wire kind explicitly
without changing the stored value or any unrelated send:

```clojure
(.q.as_kind 'datetime 2026.07.10D09:30:00.123000000) ; q -15h (`z`)
(.q.with_kinds
  (dict ['time 'expiry] ['datetime 'month])
  trades)                                               ; per-column q kinds
```

Supported kinds are `'datetime`, `'month`, `'minute`, and `'second`. Values
must be exactly representable: datetime rejects sub-millisecond timestamps,
month requires the first day, and minute/second require an in-range whole
unit. Typed nulls keep the requested q kind. The wrapper may appear inside a
larger reply such as ``(list 'upd 'trade wrapped-table)`` and is recognized
only for that encoding; without one the existing `p`/`d`/`t` mapping is
unchanged. Internally it remains a normal owning two-item LIST, so Rayfall may
inspect or store it; private adapter metadata, not its LIST shape, makes it an
encoding directive. Encoding borrows the wrapper and does not consume it.

For a collection of heterogeneous tables, each table applies the intersection
of its columns and the supplied schema. Every schema name must nevertheless
match at least one table in that collection, so a misspelled column fails the
encode instead of silently using the default wire kind everywhere.

`.q.on.*` cannot be `set` from Rayfall (dotted names are reserved), hence the verb; semantics mirror the core's `.ipc.on.*`. A char-vector request reaches the sync/async hook as an unparsed string (`(eval (parse r))` reproduces the default), a hook error is encoded to the client (sync) or logged (async, close). `(.q.handle)` is valid inside all three hooks. Hooks are process-global and live as long as the runtime: `ray_poll_destroy` releases them after the open connections' close hooks have run, a runtime destroyed without a poll needs `q_hooks_release()` first, and `q_env_register()` (once per runtime) drops what an earlier runtime left.

**Hooks and the WAL.** With a sync hook installed, a sync request that is journaled is recorded as `(.q.dispatch_sync (quote <request>))`; `.q.dispatch_sync` applies the sync hook installed at that moment, or evaluates by default when there is none. A replay therefore reproduces the live run if the application installs its hooks (`.q.on`) before `.log.replay`, which is the natural order, since it loads its code first; a replay without the hook evaluates the requests by default.

A Q listener evaluates what it is sent, so it binds **loopback by default**. The address comes from `-q HOST:PORT` (mirroring the core's `--port HOST:PORT`), else `RAYFORCE_Q_BIND_HOST`, else `127.0.0.1`; use `0.0.0.0` for the old all-interfaces behaviour. `HOST` must be an IPv4 address.

Both `-q` and `--q-serve` require a decimal TCP port in `1..65535` (optionally `HOST:PORT`). A
missing or invalid port prints a diagnostic and exits with status `2`, so a
service supervisor does not mistake a failed Q listener for a healthy
process. Arguments after `--` are passed to the Rayfall application and are
not interpreted as Q-server options.

# API

See **[INTEGRATING.md](./docs/INTEGRATING.md)** for more details.


### Client

```c
#include "q.h"

/* host/port + optional auth + connect/op timeout (ms; <= 0 blocks). */
int    q_connect(const char *host, int port, const char *user,
                 const char *password, int timeout_ms);   /* -> fd >= 0, or Q_ERR_* */
/* q_connect with the keepalive chosen: Q_KEEPALIVE_AUTO, Q_KEEPALIVE_OFF, or a budget in ms. */
int    q_connect_opts(const char *host, int port, const char *user,
                      const char *password, int timeout_ms, int keepalive_ms);
int    q_close(int fd);
ray_t *q_send(int fd, ray_t *msg, char *err, size_t n);    /* -> ray_t*, or NULL + err */

/* Split form, to release a runtime lock (e.g. the CPython GIL) around just the
 * blocking network wait — q_encode/q_decode touch the rayforce symbol table
 * (hold the lock); q_exchange is pure socket I/O (release the lock). */
int    q_encode(ray_t *msg, uint8_t **req, int64_t *req_len, char *err, size_t n);
int    q_exchange(int fd, const uint8_t *req, int64_t req_len, uint8_t **resp,
                  int64_t *resp_len, int *compressed, char *err, size_t n);
ray_t *q_decode(uint8_t *resp, int64_t resp_len, int compressed, char *err, size_t n);

/* q_decode, but a table keeps only the named columns, in the order named. */
ray_t *q_decode_cols(uint8_t *resp, int64_t resp_len, int compressed,
                     const char *const *cols, int ncols, char *err, size_t n);
```

The connection handle is the raw socket fd, so the client is thread-safe and unbounded (no shared connection table). `q_send` returns a freshly-owned
`ray_t`, which may itself be a `RAY_ERROR` carrying a Q server-side error. A `NULL` return signals a transport/serialization failure with a short reason in `err`.

Keepalive needs rayforce core v2.11.0 or later. See [INTEGRATING.md](./docs/INTEGRATING.md#keepalive) for how a budget maps onto the socket.

`q_decode_cols` is for a caller that keeps a few columns of a wide table. The other columns are skipped by their serialized length: no allocation per string cell and no symbol interning. A name the table lacks comes back as a `RAY_ERROR`. See `q.h` for keyed tables and edge cases.


### Server

To *be* a Q server (the mirror of the client), bindings additionally compile [`q_server.c`](./q_server.c) / [`q_server.h`](./q_server.h) and call:

```c
#include "q_server.h"

/* Register a non-blocking Q listener on an existing rayforce poll; the server
 * runs on the same event loop as the REPL/IPC (no thread). Returns a poll
 * selector id (>= 0), or -1. */
int64_t q_serve(ray_poll_t *poll, int port);                       /* all interfaces */
int64_t q_serve_at(ray_poll_t *poll, const char *host, int port);  /* one IPv4 address; NULL/"" = q_serve */

/* Push, hooks (see the Rayfall verbs above). */
ray_t  *q_conn_post(ray_poll_t *poll, int64_t id, ray_t *msg);  /* async frame; NULL or error */
int     q_hook_set(int which, ray_t *fn);   /* Q_HOOK_SYNC / _ASYNC / _CLOSE; non-lambda clears */
void    q_hooks_release(void);              /* release every hook; heap still alive */
```

A connected Q client (real Q `hopen`, or `q_connect`) sends a char-vector. The server evaluates it as **Rayfall** against the embedded runtime and returns the result Q-encoded.
