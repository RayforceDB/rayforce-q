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
 * q_server.c — Q wire-protocol server for RayforceDB, the mirror of q.c (see
 * q_server.h for the overview and the public q_serve() entry point).
 *
 * The connection is a non-blocking rx state machine driven by the poller,
 * mirroring Rayforce core/ipc.c.
 */

#include "q_server.h" /* q_serve, ray_poll_t                                    */
#include "q.h" /* pulls in <rayforce.h>: ray_eval_str, q_encode/q_decode */

#include "core/sock.h" /* ray_sock_listen/accept/recv/send/close               */

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
#define Q_MSG_RESPONSE 2
#define Q_CAP_MAX 3                     /* matches q.c client capability  */
#define Q_MAX_BODY ((int64_t)256 << 20) /* reject absurd frames (256 MiB) */
#define Q_MAX_HANDSHAKE 512             /* credential blob upper bound    */

/* Per-connection rx state. `hdr` carries the current frame's header between
 * the header and body reads; `cap` accumulates the last byte seen during the
 * NUL-terminated handshake (the capability byte sits just before the NUL). */
typedef struct {
  q_header_t hdr;
  uint8_t cap;
  int hs_len;
} q_conn_t;

/* Turn a decoded request into a result. A char-vector (RAY_STR) is evaluated as
 * Rayfall. */
static ray_t *eval_request(ray_t *req) {
  if (req == NULL)
    return NULL; /* empty request -> identity reply */
  if (RAY_IS_ERR(req))
    return ray_error("q server: malformed request", NULL);
  if (req->type != -RAY_STR)
    return ray_error(
        "q server: only string (char-vector) queries are supported", NULL);

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
    ray_release(e);
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
  ray_t *req = q_decode(sel->rx.buf->data, body, hdr.compressed, NULL, 0);

  sel->rx.read_fn = q_read_header;
  ray_poll_rx_request(poll, sel, (int64_t)sizeof(q_header_t));

  ray_t *result = eval_request(req);
  if (req)
    ray_release(req);

  if (hdr.msgtype != 0) { /* sync expects a response, async does not */
    ray_selector_t *cur = ray_poll_get(poll, id); /* eval may have closed it */
    if (cur)
      q_send_result((ray_sock_t)cur->fd, result);
  }
  if (result)
    ray_release(result);
  return NULL;
}

static void q_on_close(ray_poll_t *poll, ray_selector_t *sel) {
  (void)poll;
  if (sel->data) {
    free(sel->data);
    sel->data = NULL;
  }
}

/* Register a Q-protocol listener on `poll`. */
int64_t q_serve(ray_poll_t *poll, int port) {
  if (poll == NULL)
    return -1;
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
