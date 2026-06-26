# rayforce-q

Q IPC wire-format client for [RayforceDB](https://github.com/RayforceDB) —
a small, **language-neutral C core**

The core operates **purely** on rayforce `ray_t` objects and the public
`<rayforce.h>` API.

## API

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

The connection handle is the raw socket fd, so the client is thread-safe and
unbounded (no shared connection table). `q_send` returns a freshly-owned
`ray_t`, which may itself be a `RAY_ERROR` carrying a Q server-side error; a
`NULL` return signals a transport/serialization failure with a short reason in
`err`.

## Usage

`rayforce-q` is not a standalone library — bindings compile `q.c`/`q.h` **into**
their native extension, alongside the rayforce core (which supplies
`<rayforce.h>` and `table/sym.h`). See **[INTEGRATING.md](./docs/INTEGRATING.md)** for 
more details
