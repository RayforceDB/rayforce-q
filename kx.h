#ifndef RAYFORCE_KX_H
#define RAYFORCE_KX_H
/*
 * rayforce-kx — KDB+ IPC wire-format client for RayforceDB.
 *
 * Language-neutral C core shared across RayforceDB language bindings
 * (Python, Rust, ...). It speaks the documented KDB+ wire format over
 * blocking sockets and converts between the wire and rayforce core
 * `ray_t` objects. There is no binding-specific (e.g. CPython) code here;
 * each binding wraps these entry points in its own glue.
 *
 * A connection handle is the raw socket file descriptor (>= 0). There is no
 * global connection table, so the client is thread-safe and unbounded: each
 * fd is independent and owned by the caller.
 *
 * Supported KDB+ wire types
 *   atoms:   -KB -KG -KH -KI -KJ -KE -KF -KC -KS -KP -KM -KD -KN -KU -KV
 *            -KT -UU
 *   vectors: KB UU KG KH KI KJ KE KF KC KS KP KM KD KZ KN KU KV KT
 *   nested:  0 (general list), XT (table), XD (dict)
 *   error:   -128
 */

#include <rayforce.h>
#include <stddef.h>
#include <stdint.h>

/* kx_connect failure codes, returned in place of a connection fd. */
#define KX_ERR_SOCKET (-1)    /* could not open/connect the TCP socket    */
#define KX_ERR_HANDSHAKE (-2) /* socket connected but handshake/auth failed */
#define KX_ERR_TIMEOUT (-3)   /* connect timed out                        */

/* Open a TCP connection to a KDB+ server and perform the login handshake.
 *
 *   user / password  may be NULL or "" for servers without authentication.
 *   timeout_ms       connect timeout and per-operation send/recv timeout;
 *                    <= 0 means block indefinitely (no timeout).
 *
 * Returns a connection fd (>= 0), or one of the KX_ERR_* codes. */
int kx_connect(const char *host, int port, const char *user,
               const char *password, int timeout_ms);

/* Close a connection fd. Returns 0 on success, -1 on an invalid fd. */
int kx_close(int fd);

/* Serialize `msg` into a complete KDB+ wire request (header + body). Touches
 * the rayforce symbol table, so call it with any runtime lock held (e.g. the
 * CPython GIL). On success returns 0 and sets *req (malloc'd; caller frees)
 * and *req_len; on failure returns -1 and writes err. */
int kx_encode(ray_t *msg, uint8_t **req, int64_t *req_len, char *err,
              size_t errlen);

/* Send a pre-encoded request on `fd` and read the raw response body. This is
 * pure blocking socket I/O plus malloc — no rayforce object access — so it is
 * safe to run with a runtime lock released (release the GIL around this to
 * keep other threads live during the server round-trip). On success returns 0
 * and sets *resp (malloc'd; caller frees), *resp_len and *compressed; on
 * failure returns -1 and writes err. */
int kx_exchange(int fd, const uint8_t *req, int64_t req_len, uint8_t **resp,
                int64_t *resp_len, int *compressed, char *err, size_t errlen);

/* Decompress (if needed) and deserialize a response body obtained from
 * kx_exchange into a freshly-owned rayforce object. The result may itself be a
 * RAY_ERROR carrying a KDB+ server-side error. Touches the symbol table, so
 * call it with the runtime lock held. Returns NULL on a decode failure. */
ray_t *kx_decode(uint8_t *resp, int64_t resp_len, int compressed, char *err,
                 size_t errlen);

/* Convenience: kx_encode + kx_exchange + kx_decode, all under the caller's
 * current lock. Bindings that need to release a lock around the network wait
 * should call the three steps directly instead. Returns the decoded object
 * (possibly a RAY_ERROR), or NULL on a transport/serialization failure with a
 * short reason written to err. */
ray_t *kx_send(int fd, ray_t *msg, char *err, size_t errlen);

#endif /* RAYFORCE_KX_H */
