/*
 * Copyright (c) 2026 RayforceDB Team
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#ifndef RAYFORCE_Q_H
#define RAYFORCE_Q_H
/*
 * rayforce-q — Q IPC wire-format client for RayforceDB.
 *
 * Language-neutral C core shared across RayforceDB language bindings.
 *
 * It speaks the documented Q wire format over blocking sockets and
 * converts between the wire and rayforce core `ray_t` objects.
 *
 * A connection handle is the raw socket file descriptor (>= 0). There is no
 * global connection table. Independent fds may be used concurrently; calls
 * sharing one fd must serialize complete request/response exchanges. Each fd
 * remains owned by the caller.
 */

#include <rayforce.h>
#include <stddef.h>
#include <stdint.h>

/* q_connect failure codes, returned in place of a connection fd. */
#define Q_ERR_SOCKET (-1)    /* could not open/connect the TCP socket    */
#define Q_ERR_HANDSHAKE (-2) /* socket connected but handshake/auth failed */
#define Q_ERR_TIMEOUT (-3)   /* connect timed out                        */

/* Dead-peer detection, as the core's RAY_IPC_KEEPALIVE_* (core/ipc.h). A
 * peer that vanishes without closing (a suspended host, a NAT or VPN that
 * drops the flow) is noticed by kernel keepalive instead of the connection
 * waiting forever. See the core's ray_sock_set_keepalive for how a budget
 * maps onto the socket options.
 *
 *   Q_KEEPALIVE_AUTO  keepalive at the core's 60 s default, no
 *                     unacknowledged-data bound
 *   Q_KEEPALIVE_OFF   no keepalive
 *   n > 0             keepalive at budget n ms, plus the unacknowledged-data
 *                     bound (TCP_USER_TIMEOUT), so a peer that stops reading
 *                     for n ms counts as dead too */
#define Q_KEEPALIVE_AUTO 0
#define Q_KEEPALIVE_OFF (-1)

/* Open a TCP connection to a Q server and perform the login handshake.
 * Keepalive is Q_KEEPALIVE_AUTO. */
int q_connect(const char *host, int port, const char *user,
              const char *password, int timeout_ms);

/* As q_connect, with the keepalive chosen: Q_KEEPALIVE_AUTO,
 * Q_KEEPALIVE_OFF, or a budget in ms. It is armed once the handshake has
 * succeeded, best-effort as in the core: if SO_KEEPALIVE fails, the
 * connection is returned without keepalive; a tuning option the platform
 * lacks or rejects keeps its system default. */
int q_connect_opts(const char *host, int port, const char *user,
                   const char *password, int timeout_ms, int keepalive_ms);

/* Close a connection fd. Returns 0 on success, -1 on an invalid fd. */
int q_close(int fd);

/* Serialize `msg` into a complete Q wire request (header + body). Touches
 * the rayforce symbol table, so call it with any runtime lock held (e.g. the
 * CPython GIL). */
int q_encode(ray_t *msg, uint8_t **req, int64_t *req_len, char *err,
             size_t errlen);

/* Send a pre-encoded request on `fd` and read the raw response body. A
 * transport or framing failure shuts down the socket because the request or
 * response boundary may be out of sync; the caller still owns and must close
 * the fd. */
int q_exchange(int fd, const uint8_t *req, int64_t req_len, uint8_t **resp,
               int64_t *resp_len, int *compressed, char *err, size_t errlen);

/* Decompress (if needed) and deserialize a response body obtained from
 * q_exchange into a freshly-owned rayforce object. The result may be a
 * RAY_ERROR carrying a Q server-side error. Touches the symbol table, so
 * call it with the runtime lock held. Returns NULL on a decode failure. */
ray_t *q_decode(uint8_t *resp, int64_t resp_len, int compressed, char *err,
                size_t errlen);

/* As q_decode, but a table comes back with only the named columns, in the
 * order named (as q's cols#t). The other columns are skipped by their
 * serialized length: not allocated, not interned, and not checked beyond
 * their length, so a column q_decode rejects (a timespan) can be skipped. For
 * a keyed table the key columns always come back and the names pick among
 * the value columns; naming a key column is allowed and changes nothing. A
 * name the table lacks is a RAY_ERROR naming it. A NULL or repeated name
 * returns NULL with err set. With ncols == 0, or a message that is not a
 * table, this is q_decode. A compressed message still decompresses whole. */
ray_t *q_decode_cols(uint8_t *resp, int64_t resp_len, int compressed,
                     const char *const *cols, int ncols, char *err,
                     size_t errlen);

/* Convenience: q_encode + q_exchange + q_decode, all under the caller's
 * current lock. Bindings that need to release a lock around the network wait
 * should call the three steps directly instead. Returns the decoded object
 * (possibly a RAY_ERROR), or NULL on a transport/serialization failure with a
 * short reason written to err. */
ray_t *q_send(int fd, ray_t *msg, char *err, size_t errlen);

#endif /* RAYFORCE_Q_H */
