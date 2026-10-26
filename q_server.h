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

#ifndef RAYFORCE_Q_SERVER_H
#define RAYFORCE_Q_SERVER_H
/*
 * rayforce-q — Q IPC wire-format server for RayforceDB. The mirror of q.h.
 *
 * Where q.h lets you call a Q server, this lets you be one: a Q client or
 * rayforce-q's own q_connect sends a char-vector, the server evaluates it as a
 * *Rayfall* statement against the embedded runtime, and returns the result
 * Q-encoded. The Q runtime is only the transport.
 *
 * Like q.c, this is language-neutral C shared across bindings: a binding that
 * wants to host a Q server compiles q_server.c (alongside q.c, whose codec it
 * reuses) and calls q_serve() with the runtime's poll. The shipped rayforce
 * binary does exactly that behind `rayforce -q PORT`
 *
 * The listener is non-blocking and runs entirely on the rayforce poll you
 * hand it — the same event loop the REPL and native IPC use — so it never
 * spawns a thread. Every request is evaluated on the poll thread, serialized
 * with the rest of the runtime.
 */

#include <rayforce.h>

#include "core/poll.h" /* ray_poll_t */

#include <stdint.h>

/* Register a Q-protocol listener on `poll`, bound to TCP `port`. Returns the
 * poll selector id (>= 0) on success, or -1 on failure */
int64_t q_serve(ray_poll_t *poll, int port);

/* Whether a serialized request of `size` bytes may be journaled: the core's
 * replay rejects a frame payload above 256 MiB, so the q listener refuses one
 * (a `limit` error) before it writes or evaluates it. Exposed for tests. */
int q_journal_size_ok(int64_t size);

/* ===== Outbound connections =====
 *
 * q.c's client is blocking and poll-free by design (bindings without an event
 * loop need it that way), which makes it request/response only: a frame the
 * peer pushes unsolicited would be read as the answer to the next q_send.
 *
 * Attaching a connected fd to the poll removes that restriction. The event
 * loop owns the reads, so a q publisher can push into this process — the
 * shape every q subscription has: send `.net.sub` once, then receive.
 */

/* Put an already-connected, already-handshaken q_connect fd under `poll`'s
 * rx machine. Returns a poll selector id (>= 0) — the handle for the calls
 * below — or -1. The fd is owned by the poll from here on; do not q_close it.
 * The socket's recv timeout (what q_connect's timeout_ms set) is read here
 * and becomes q_conn_send's round-trip budget; none = wait indefinitely.
 */
int64_t q_conn_attach(ray_poll_t *poll, int fd);

/* Sync round-trip on an attached connection: write a SYNC frame, then pump
 * this connection until its RESPONSE arrives — dispatching, not swallowing,
 * any frame that arrives in between (a pushed async runs its handler, an
 * inbound sync request is answered). Returns a freshly-owned object, which
 * may be a RAY_ERROR from the peer or a local io/handle error. When the
 * budget inherited at attach time runs out first, the connection is closed
 * (a late RESPONSE must not be taken for the next request's reply) and a
 * `timeout` error is returned; the handle is then no longer open. */
ray_t *q_conn_send(ray_poll_t *poll, int64_t id, ray_t *msg);

/* Send `msg` as a q ASYNC frame on an open connection, inbound or outbound,
 * and return at once: the frame is queued on the poll and flushed when the
 * socket is writable. Returns NULL on success or an owned error: `handle` if
 * `id` is not an open, established q connection, `io` if the message cannot be
 * encoded or the connection's queue is full. A later q_conn_send on the same
 * handle writes any still-queued frames first, so order is kept. */
ray_t *q_conn_post(ray_poll_t *poll, int64_t id, ray_t *msg);

/* Check whether `id` identifies an open Q connection owned by this module. */
int q_conn_is_handle(ray_poll_t *poll, int64_t id);

/* The connection (inbound or attached) whose frame, or close, is being
 * handled right now: valid inside a handler such a frame called and inside
 * the hooks below, -1 everywhere else. */
int64_t q_conn_current(void);

/* ===== Hooks =====
 *
 * What the core's `.ipc.on.*` are for native IPC, for q frames; `.q.on.*` is a
 * reserved name Rayfall cannot `set`, hence a setter. Each hook is a lambda
 * (anything else clears it):
 *   SYNC   called with the decoded request INSTEAD of the default eval; its
 *          value (or error) is the reply. A char vector arrives unparsed.
 *   ASYNC  called with the decoded request; there is no reply, an error is
 *          logged.
 *   CLOSE  called with the handle once per established inbound connection,
 *          just before teardown; errors are logged and swallowed.
 * q_conn_current() is the connection inside all three. Returns 0, or -1 for
 * an unknown hook. The lambda is retained; a hook outlives neither its
 * runtime nor a q_hook_set that replaces it. */
enum { Q_HOOK_SYNC = 0, Q_HOOK_ASYNC = 1, Q_HOOK_CLOSE = 2, Q_HOOK_COUNT = 3 };
int q_hook_set(int which, ray_t *fn);

/* `.q.dispatch_sync`: the sync request `req` (borrowed) answered by the
 * installed sync hook, or by the default evaluation when there is none. A sync
 * request received while a WAL is open and a sync hook is installed is
 * journaled as (.q.dispatch_sync (quote <request>)), so `.log.replay` runs the
 * request through whichever hook is installed then: install the hooks before
 * replaying (the application loads its code, `.q.on` included, first), or the
 * replay evaluates the requests by default. */
ray_t *q_dispatch_sync(ray_t *req);

/* Close an attached connection and release its state. */
void q_conn_close(ray_poll_t *poll, int64_t id);

/* q_decode with the decompressed body bounded by `max_body`. q_decode bounds
 * it at the client's 2 GiB; the rx machine passes each connection's own
 * limit, so a connection the listener accepted stays at 256 MiB. Internal to
 * rayforce-q: defined in q.c, called from q_server.c. */
ray_t *q_decode_bounded(uint8_t *body, int64_t body_len, int compressed,
                        int64_t max_body, char *err, size_t errlen);

#endif /* RAYFORCE_Q_SERVER_H */
