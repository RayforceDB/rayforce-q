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

#define _GNU_SOURCE
/*
 * q_server.c — Q wire-protocol connections on a rayforce poll (see q_server.h
 * for the overview and the public entry points).
 *
 * Two kinds of connection share one non-blocking rx state machine, mirroring
 * Rayforce core/ipc.c:
 *
 *   - inbound  (q_serve): accepted by our listener, handshake then frames.
 *   - outbound (q_conn_attach): a client fd q.c already connected and
 *     handshook, handed to the poll so pushed frames are read by the event
 *     loop instead of being mistaken for the next response.
 *
 * They differ only in which message types they expect: an inbound connection
 * never sees a RESPONSE, an outbound one gets them for its own sync sends.
 */

#include "q_server.h" /* q_serve, q_conn_*, ray_poll_t                          */
#include "q.h" /* pulls in <rayforce.h>: ray_eval_str, q_encode/q_decode */

#include "core/sock.h" /* ray_sock_listen/accept/recv/send/close               */
#include "lang/eval.h" /* ray_eval, RAY_EVAL_LITERAL_FALLBACK                  */
#include "ops/ops.h" /* ray_is_lazy, ray_lazy_materialize                    */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Wire header — must match q.c byte-for-byte */
typedef struct {
  uint8_t endianness; /* 1 = little-endian                              */
  uint8_t msgtype;    /* 0 = async, 1 = sync, 2 = response              */
  uint8_t compressed; /* 0/1                                            */
  uint8_t reserved;
  uint32_t size; /* total message length incl. this 8-byte header       */
} q_header_t;

#define Q_LITTLE_ENDIAN 1
#define Q_MSG_ASYNC 0
#define Q_MSG_RESPONSE 2
#define Q_CAP_MAX 3                     /* matches q.c client capability  */
#define Q_MAX_BODY ((int64_t)256 << 20) /* reject absurd frames (256 MiB) */
#define Q_MAX_HANDSHAKE 512             /* credential blob upper bound    */

/* Per-connection rx state. `hdr` carries the current frame's header between
 * the header and body reads; `cap` accumulates the last byte seen during the
 * NUL-terminated handshake (the capability byte sits just before the NUL).
 *
 * The sync_* triple is the outbound side's round-trip slot: q_conn_send parks
 * on the connection and the rx machine deposits the RESPONSE frame here. */
typedef struct {
  q_header_t hdr;
  uint8_t cap;
  int hs_len;
  uint8_t sync_waiting; /* a q_conn_send is parked on this connection */
  uint8_t sync_ready;   /* its RESPONSE has been deposited below      */
  ray_t *sync_resp;
} q_conn_t;

static void q_release_any(ray_t *obj) {
  if (obj == NULL)
    return;
  if (RAY_IS_ERR(obj))
    ray_error_free(obj);
  else
    ray_release(obj);
}

/* A list argument carries data, not variable references — mark it so a symbol
 * inside it stays a literal instead of resolving against the environment.
 * Mirrors mark_ipc_literal_fallbacks in core/ipc.c. */
static void q_mark_literals(ray_t *obj) {
  if (obj == NULL || RAY_IS_ERR(obj) || obj->type != RAY_LIST)
    return;
  obj->attrs |= RAY_EVAL_LITERAL_FALLBACK;
  ray_t **elems = (ray_t **)ray_data(obj);
  for (int64_t i = 0; i < ray_len(obj); i++)
    q_mark_literals(elems[i]);
}

/* Turn a decoded request into a result. Borrows `req`.
 *
 * A char-vector (RAY_STR) is evaluated as Rayfall — that is what a q client
 * sending `h "expr"` means. Anything else goes through ray_eval, which is the
 * identity for plain data and a call for an expression list: a q peer's
 * `(`fn;arg;…)` message therefore invokes `fn`, exactly as it would in q.
 * Same contract as the native IPC server (core/ipc.c). */
static ray_t *eval_request(ray_t *req) {
  if (req == NULL)
    return NULL; /* empty request -> identity reply */
  if (RAY_IS_ERR(req))
    return ray_error("q server: malformed request", NULL);

  if (req->type == -RAY_STR) {
    size_t len = ray_str_len(req);
    char *src = (char *)malloc(len + 1);
    if (src == NULL)
      return ray_error("q server: out of memory", NULL);
    memcpy(src, ray_str_ptr(req), len);
    src[len] = '\0';
    ray_t *res = ray_eval_str(src);
    free(src);
    return res;
  }

  if (req->type == RAY_LIST) {
    ray_t **elems = (ray_t **)ray_data(req);
    for (int64_t i = 1; i < ray_len(req); i++)
      q_mark_literals(elems[i]);
  }
  ray_t *res = ray_eval(req);
  /* A lazy result is an internal deferred-DAG value that cannot be
   * serialized; force it before it can reach the wire. */
  if (res != NULL && ray_is_lazy(res))
    res = ray_lazy_materialize(res);
  return res;
}

/* Encode `result` as a Q response and write it. A NULL result encodes as the
 * identity (::). q_encode stamps the header as SYNC, so flip the message-type
 * byte to RESPONSE before sending. */
static void q_send_result(ray_sock_t fd, ray_t *result) {
  uint8_t *buf = NULL;
  int64_t len = 0;
  char err[128] = {0};
  if (q_encode(result, &buf, &len, err, sizeof err) < 0) {
    ray_t *e = ray_error(err[0] ? err : "q server: encode failed", NULL);
    int rc = q_encode(e, &buf, &len, err, sizeof err);
    q_release_any(e);
    if (rc < 0)
      return;
  }
  buf[1] = Q_MSG_RESPONSE;
  ray_sock_send(fd, buf, (size_t)len);
  free(buf);
}

static int64_t q_recv_fn(int64_t fd, uint8_t *buf, int64_t len) {
  return ray_sock_recv((ray_sock_t)fd, buf, (size_t)len);
}

static ray_t *q_read_handshake(ray_poll_t *poll, ray_selector_t *sel);
static ray_t *q_read_header(ray_poll_t *poll, ray_selector_t *sel);
static ray_t *q_read_body(ray_poll_t *poll, ray_selector_t *sel);
static void q_on_close(ray_poll_t *poll, ray_selector_t *sel);

/* Accept callback on the listener selector. */
static ray_t *q_accept(ray_poll_t *poll, ray_selector_t *sel) {
  ray_sock_t nfd = ray_sock_accept((ray_sock_t)sel->fd);
  if (nfd == RAY_INVALID_SOCK)
    return NULL;
  ray_sock_set_nonblocking(nfd);

  q_conn_t *cd = (q_conn_t *)calloc(1, sizeof *cd);
  if (cd == NULL) {
    ray_sock_close(nfd);
    return NULL;
  }

  ray_poll_reg_t reg = {0};
  reg.fd = (int64_t)nfd;
  reg.type = RAY_SEL_SOCKET;
  reg.recv_fn = q_recv_fn;
  reg.read_fn = q_read_handshake;
  reg.close_fn = q_on_close;
  reg.data = cd;

  int64_t id = ray_poll_register(poll, &reg);
  if (id < 0) {
    ray_sock_close(nfd);
    free(cd);
    return NULL;
  }
  /* Read the NUL-terminated login one byte at a time. */
  ray_selector_t *ns = ray_poll_get(poll, id);
  if (ns)
    ray_poll_rx_request(poll, ns, 1);
  return NULL;
}

/* Q login: "<user>[:<pass>]" + capability byte + NUL */
static ray_t *q_read_handshake(ray_poll_t *poll, ray_selector_t *sel) {
  if (!sel->rx.buf || sel->rx.buf->offset < 1)
    return NULL;
  q_conn_t *cd = (q_conn_t *)sel->data;
  uint8_t b = sel->rx.buf->data[0];

  if (b == 0x00) {
    uint8_t reply = cd->cap < Q_CAP_MAX ? cd->cap : Q_CAP_MAX;
    ray_sock_send((ray_sock_t)sel->fd, &reply, 1);
    sel->rx.read_fn = q_read_header;
    ray_poll_rx_request(poll, sel, (int64_t)sizeof(q_header_t));
    return NULL;
  }
  cd->cap = b; /* last non-NUL byte wins (the capability byte) */
  if (++cd->hs_len > Q_MAX_HANDSHAKE) {
    ray_poll_deregister(poll, sel->id);
    return NULL;
  }
  ray_poll_rx_request(poll, sel, 1);
  return NULL;
}

static ray_t *q_read_header(ray_poll_t *poll, ray_selector_t *sel) {
  q_conn_t *cd = (q_conn_t *)sel->data;
  if (!sel->rx.buf || sel->rx.buf->offset < (int64_t)sizeof(q_header_t))
    return NULL;
  memcpy(&cd->hdr, sel->rx.buf->data, sizeof(q_header_t));

  if (cd->hdr.endianness != Q_LITTLE_ENDIAN) {
    ray_poll_deregister(poll, sel->id);
    return NULL;
  }
  int64_t body = (int64_t)cd->hdr.size - (int64_t)sizeof(q_header_t);
  if (body < 0 || body > Q_MAX_BODY) {
    ray_poll_deregister(poll, sel->id);
    return NULL;
  }
  if (body == 0) {
    if (cd->hdr.msgtype != 0) /* sync -> identity reply */
      q_send_result((ray_sock_t)sel->fd, NULL);
    ray_poll_rx_request(poll, sel, (int64_t)sizeof(q_header_t));
    return NULL;
  }
  sel->rx.read_fn = q_read_body;
  ray_poll_rx_request(poll, sel, body);
  return NULL;
}

static ray_t *q_read_body(ray_poll_t *poll, ray_selector_t *sel) {
  q_conn_t *cd = (q_conn_t *)sel->data;
  int64_t body = (int64_t)cd->hdr.size - (int64_t)sizeof(q_header_t);
  if (!sel->rx.buf || sel->rx.buf->offset < body)
    return NULL;

  /* q_decode fully materializes the request into ray_t objects, so the rx
   * buffer is free to reuse the moment it returns. */
  q_header_t hdr = cd->hdr;
  int64_t id = sel->id;
  char err[128] = {0};
  ray_t *req =
      q_decode(sel->rx.buf->data, body, hdr.compressed, err, sizeof err);

  sel->rx.read_fn = q_read_header;
  ray_poll_rx_request(poll, sel, (int64_t)sizeof(q_header_t));

  /* A RESPONSE belongs to the q_conn_send parked on this connection — it is
   * data, not something to evaluate. */
  if (hdr.msgtype == Q_MSG_RESPONSE) {
    if (req == NULL)
      req = ray_error("q client: malformed response", "%s",
                      err[0] ? err : "decode failed");
    if (cd->sync_waiting && !cd->sync_ready) {
      cd->sync_resp = req;
      cd->sync_ready = 1;
    } else {
      fprintf(stderr, "q: unsolicited response frame dropped\n");
      q_release_any(req);
    }
    return NULL;
  }

  ray_t *result = req ? eval_request(req)
                      : ray_error("q server: malformed request", "%s",
                                  err[0] ? err : "q server: decode failed");
  if (req)
    q_release_any(req);

  if (hdr.msgtype !=
      Q_MSG_ASYNC) { /* sync expects a response, async does not */
    ray_selector_t *cur = ray_poll_get(poll, id); /* eval may have closed it */
    if (cur)
      q_send_result((ray_sock_t)cur->fd, result);
  } else if (result != NULL && RAY_IS_ERR(result)) {
    /* Async has no reply channel, so an error here would vanish silently —
     * the one place an operator could learn a push handler is broken. */
    fprintf(stderr, "q: async message raised an error\n");
  }
  if (result)
    q_release_any(result);
  return NULL;
}

static void q_on_close(ray_poll_t *poll, ray_selector_t *sel) {
  (void)poll;
  if (sel->data) {
    q_conn_t *cd = (q_conn_t *)sel->data;
    /* A RESPONSE deposited for a sync wait that never consumed it (peer died
     * mid-round-trip) would otherwise leak. */
    if (cd->sync_resp)
      q_release_any(cd->sync_resp);
    free(cd);
    sel->data = NULL;
  }
  ray_sock_close((ray_sock_t)sel->fd);
}

/* Register a Q-protocol listener on `poll`. */
int64_t q_serve(ray_poll_t *poll, int port) {
  if (poll == NULL)
    return -1;
  if (port < 1 || port > 65535) {
    fprintf(stderr, "q: invalid port %d (expected 1..65535)\n", port);
    return -1;
  }
  ray_sock_t fd = ray_sock_listen((uint16_t)port);
  if (fd == RAY_INVALID_SOCK) {
    fprintf(stderr, "q: cannot listen on port %d (in use?)\n", port);
    return -1;
  }
  ray_sock_set_nonblocking(fd);

  ray_poll_reg_t reg = {0};
  reg.fd = (int64_t)fd;
  reg.type = RAY_SEL_SOCKET;
  reg.read_fn = q_accept;

  int64_t id = ray_poll_register(poll, &reg);
  if (id < 0) {
    ray_sock_close(fd);
    return -1;
  }
  fprintf(stderr, "q: listening on %d (Rayfall over the Q wire)\n", port);
  return id;
}

/* ===================== Outbound connections ===============================
 * A q.c client fd is a blocking socket nobody watches, so a frame the peer
 * pushes (a q publisher's `neg[h](`upd;data)`) sits in the receive buffer
 * until the next q_send reads it AS ITS OWN RESPONSE. Attaching the fd to the
 * poll fixes that: the event loop reads every frame and routes it by type —
 * RESPONSE to the waiting q_conn_send, anything else through the same
 * eval_request as an inbound request. Being a subscriber is then just being
 * a q process that happens to run Rayfall.
 * ======================================================================== */

/* Drain whatever is readable on one connection through its rx state machine
 * WITHOUT blocking. Returns 0 when the socket is dry, -1 if the selector was
 * deregistered (peer closed or protocol error). Mirrors conn_pump in
 * core/ipc.c. */
static int q_conn_pump(ray_poll_t *poll, int64_t id) {
  for (;;) {
    ray_selector_t *sel = ray_poll_get(poll, id);
    if (sel == NULL)
      return -1;
    if (!sel->rx.buf || !sel->rx.recv_fn || !sel->rx.read_fn)
      return 0;
    while (sel->rx.buf->offset < sel->rx.buf->size) {
      int64_t nr =
          sel->rx.recv_fn(sel->fd, sel->rx.buf->data + sel->rx.buf->offset,
                          sel->rx.buf->size - sel->rx.buf->offset);
      if (nr <= 0) {
        if (nr < 0 && errno == EINTR)
          continue;
        if (nr < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
          return 0; /* drained — caller decides whether to wait */
        ray_poll_deregister(poll, id);
        return -1;
      }
      sel->rx.buf->offset += nr;
    }
    /* Phase buffer complete — advance the state machine. read_fn may
     * deregister or re-arm the selector; the loop re-validates either way. */
    sel->rx.read_fn(poll, sel);
  }
}

int64_t q_conn_attach(ray_poll_t *poll, int fd) {
  if (poll == NULL || fd < 0)
    return -1;
  ray_sock_set_nonblocking((ray_sock_t)fd);

  q_conn_t *cd = (q_conn_t *)calloc(1, sizeof *cd);
  if (cd == NULL)
    return -1;

  ray_poll_reg_t reg = {0};
  reg.fd = (int64_t)fd;
  reg.type = RAY_SEL_SOCKET;
  reg.recv_fn = q_recv_fn;
  reg.read_fn = q_read_header; /* q_connect already did the handshake */
  reg.close_fn = q_on_close;
  reg.data = cd;

  int64_t id = ray_poll_register(poll, &reg);
  if (id < 0) {
    free(cd);
    return -1;
  }
  ray_selector_t *sel = ray_poll_get(poll, id);
  if (sel)
    ray_poll_rx_request(poll, sel, (int64_t)sizeof(q_header_t));
  return id;
}

ray_t *q_conn_send(ray_poll_t *poll, int64_t id, ray_t *msg) {
  ray_selector_t *sel = poll ? ray_poll_get(poll, id) : NULL;
  if (sel == NULL || sel->data == NULL)
    return ray_error("handle", "q: not an open connection");
  q_conn_t *cd = (q_conn_t *)sel->data;
  if (cd->sync_waiting)
    /* Two waiters cannot both claim the next RESPONSE — this is a handler
     * that called back into the connection it was dispatched from. */
    return ray_error("io", "q: nested sync send on busy handle");

  uint8_t *req = NULL;
  int64_t req_len = 0;
  char err[128] = {0};
  if (q_encode(msg, &req, &req_len, err, sizeof err) < 0)
    return ray_error("send", "%s", err[0] ? err : "q: encode failed");
  int64_t sent = ray_sock_send((ray_sock_t)sel->fd, req, (size_t)req_len);
  free(req);
  if (sent < 0)
    return ray_error("io", "q: send failed");

  cd->sync_waiting = 1;
  cd->sync_ready = 0;
  cd->sync_resp = NULL;

  for (;;) {
    /* Process what is already readable, then block for more. The pump can
     * deregister the selector (peer died), which frees cd — so re-resolve and
     * compare the data pointer before touching it. The compare also guards
     * against this id being reused by a connection opened inside a dispatched
     * message handler. */
    int rc = q_conn_pump(poll, id);
    sel = ray_poll_get(poll, id);
    if (sel == NULL || sel->data != (void *)cd)
      return ray_error("io", "q: connection closed");
    if (cd->sync_ready) {
      ray_t *result = cd->sync_resp;
      cd->sync_resp = NULL;
      cd->sync_ready = 0;
      cd->sync_waiting = 0;
      return result ? result : ray_error("io", "q: bad response");
    }
    if (rc < 0)
      return ray_error("io", "q: connection closed");
    int w = ray_sock_wait_readable_intr((ray_sock_t)sel->fd, -1);
    if (w == -2)
      continue; /* interrupted by a signal — keep waiting */
    if (w < 0) {
      cd->sync_waiting = 0;
      return ray_error("io", "q: recv failed");
    }
  }
}

void q_conn_close(ray_poll_t *poll, int64_t id) {
  if (poll == NULL)
    return;
  ray_poll_deregister(poll, id); /* q_on_close frees the state and the fd */
}
