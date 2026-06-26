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
 * global connection table, so the client is thread-safe and unbounded: each
 * fd is independent and owned by the caller.
 */

#include <rayforce.h>
#include <stddef.h>
#include <stdint.h>

/* q_connect failure codes, returned in place of a connection fd. */
#define Q_ERR_SOCKET (-1)    /* could not open/connect the TCP socket    */
#define Q_ERR_HANDSHAKE (-2) /* socket connected but handshake/auth failed */
#define Q_ERR_TIMEOUT (-3)   /* connect timed out                        */

/* Open a TCP connection to a Q server and perform the login handshake. */
int q_connect(const char *host, int port, const char *user,
              const char *password, int timeout_ms);

/* Close a connection fd. Returns 0 on success, -1 on an invalid fd. */
int q_close(int fd);

/* Serialize `msg` into a complete Q wire request (header + body). Touches
 * the rayforce symbol table, so call it with any runtime lock held (e.g. the
 * CPython GIL). */
int q_encode(ray_t *msg, uint8_t **req, int64_t *req_len, char *err,
             size_t errlen);

/* Send a pre-encoded request on `fd` and read the raw response body. */
int q_exchange(int fd, const uint8_t *req, int64_t req_len, uint8_t **resp,
               int64_t *resp_len, int *compressed, char *err, size_t errlen);

/* Decompress (if needed) and deserialize a response body obtained from
 * q_exchange into a freshly-owned rayforce object. The result may be a
 * RAY_ERROR carrying a Q server-side error. Touches the symbol table, so
 * call it with the runtime lock held. Returns NULL on a decode failure. */
ray_t *q_decode(uint8_t *resp, int64_t resp_len, int compressed, char *err,
                size_t errlen);

/* Convenience: q_encode + q_exchange + q_decode, all under the caller's
 * current lock. Bindings that need to release a lock around the network wait
 * should call the three steps directly instead. Returns the decoded object
 * (possibly a RAY_ERROR), or NULL on a transport/serialization failure with a
 * short reason written to err. */
ray_t *q_send(int fd, ray_t *msg, char *err, size_t errlen);

#endif /* RAYFORCE_Q_H */
