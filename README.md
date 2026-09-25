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

A host without a poll (a binding that embeds only the client) keeps the plain blocking `q.c` path and its raw-fd handle.

### Server

The server is exposed via the `-q PORT` flag — it shares the REPL's event loop, so the REPL stays interactive while it serves Q clients:

```sh
./rayforce -q 25565        # serve Rayfall over the Q wire on port 25565
```

Both `-q` and `--q-serve` require a decimal TCP port in `1..65535`. A
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
int    q_close(int fd);
ray_t *q_send(int fd, ray_t *msg, char *err, size_t n);    /* -> ray_t*, or NULL + err */

/* Split form, to release a runtime lock (e.g. the CPython GIL) around just the
 * blocking network wait — q_encode/q_decode touch the rayforce symbol table
 * (hold the lock); q_exchange is pure socket I/O (release the lock). */
int    q_encode(ray_t *msg, uint8_t **req, int64_t *req_len, char *err, size_t n);
int    q_exchange(int fd, const uint8_t *req, int64_t req_len, uint8_t **resp,
                  int64_t *resp_len, int *compressed, char *err, size_t n);
ray_t *q_decode(uint8_t *resp, int64_t resp_len, int compressed, char *err, size_t n);
```

The connection handle is the raw socket fd, so the client is thread-safe and unbounded (no shared connection table). `q_send` returns a freshly-owned
`ray_t`, which may itself be a `RAY_ERROR` carrying a Q server-side error. A `NULL` return signals a transport/serialization failure with a short reason in `err`.


### Server

To *be* a Q server (the mirror of the client), bindings additionally compile [`q_server.c`](./q_server.c) / [`q_server.h`](./q_server.h) and call:

```c
#include "q_server.h"

/* Register a non-blocking Q listener on an existing rayforce poll; the server
 * runs on the same event loop as the REPL/IPC (no thread). Returns a poll
 * selector id (>= 0), or -1. */
int64_t q_serve(ray_poll_t *poll, int port);
```

A connected Q client (real Q `hopen`, or `q_connect`) sends a char-vector. The server evaluates it as **Rayfall** against the embedded runtime and returns the result Q-encoded.
