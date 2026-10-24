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

#include "core/runtime.h" /* ray_error_msg                                     */
#include "core/sock.h" /* ray_sock_listen/accept/recv/send/close               */
#include "core/timer.h" /* ray_time_now_ms                                     */
#include "lang/eval.h" /* ray_eval, RAY_EVAL_LITERAL_FALLBACK                  */
#include "ops/ops.h" /* ray_is_lazy, ray_lazy_materialize                    */
#include "core/ipc.h" /* RAY_IPC_MSG_*, RAY_IPC_FLAG_RESTRICTED             */
#include "store/journal.h" /* ray_journal_is_open, ray_journal_write_bytes  */
#include "store/serde.h" /* ray_ipc_header_t, ray_serde_size, ray_ser_raw   */

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>

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
#define Q_MSG_SYNC 1
#define Q_MSG_RESPONSE 2
#define Q_CAP_MAX 3 /* matches q.c client capability  */
/* A connection the listener accepted may come from anyone, so a frame's
 * body, on the wire and decompressed, is capped at 256 MiB. One q_conn_attach
 * took was opened by this process to a peer it chose, and gets what the
 * classic header can declare, as in q.c. The replies q_send_result queues
 * stay within 256 MiB on either. */
#define Q_MAX_BODY ((int64_t)256 << 20)
#define Q_MAX_CLIENT_BODY ((int64_t)INT32_MAX - (int64_t)sizeof(q_header_t))
#define Q_RX_CHUNK_SIZE ((int64_t)64 << 10)
#define Q_MAX_HANDSHAKE 512             /* credential blob upper bound    */
#define Q_CONN_MAGIC UINT32_C(0x7150434e)

/* Per-connection rx state. `hdr` carries the current frame's header between
 * the header and body reads; `cap` accumulates the last byte seen during the
 * NUL-terminated handshake (the capability byte sits just before the NUL).
 *
 * The sync_* triple is the outbound side's round-trip slot: q_conn_send parks
 * on the connection and the rx machine deposits the RESPONSE frame here. */
typedef struct {
  uint32_t magic;
  q_header_t hdr;
  uint8_t cap;
  int hs_len;
  uint8_t sync_waiting; /* a q_conn_send is parked on this connection */
  uint8_t sync_ready;   /* its RESPONSE has been deposited below      */
  uint8_t outbound;     /* q_conn_attach client side; may receive RESPONSE */
  ray_t *sync_resp;
  int sync_timeout_ms; /* q_conn_send round-trip budget; <= 0 blocks  */
  uint8_t *rx_body;
  int64_t rx_body_len;
  int64_t rx_body_cap;
  int64_t rx_body_total;
} q_conn_t;

/* The outbound connection whose pushed frame is being evaluated, or -1. A
 * handler reads it through q_conn_current (`.q.handle`), which is how one
 * process subscribed to several publishers tells their frames apart. */
static int64_t q_current_handle = -1;

int64_t q_conn_current(void) { return q_current_handle; }

static int64_t q_conn_max_body(const q_conn_t *cd) {
  return cd->outbound ? Q_MAX_CLIENT_BODY : Q_MAX_BODY;
}

static void q_release_any(ray_t *obj) {
  if (obj == NULL)
    return;
  if (RAY_IS_ERR(obj))
    ray_error_free(obj);
  else
    ray_release(obj);
}

static ray_t *q_journal_form(ray_t *req);

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
    /* kdb answers `h ""` with the identity (::); tools and feeds use it as a
     * ping/flush. An empty or whitespace-only request is that, not a parse
     * error. */
    {
      const char *p = ray_str_ptr(req);
      size_t i = 0;
      while (i < len && (p[i] == ' ' || p[i] == '\t' || p[i] == '\n' ||
                         p[i] == '\r'))
        i++;
      if (i == len)
        return NULL;
    }
    char *src = (char *)malloc(len + 1);
    if (src == NULL)
      return ray_error("q server: out of memory", NULL);
    memcpy(src, ray_str_ptr(req), len);
    src[len] = '\0';
    ray_t *res = ray_eval_str(src);
    free(src);
    return res;
  }

  /* Evaluate the request in its data form: every LIST argument wrapped in
   * (quote ...). That is the exact object a sync request journals, so live
   * dispatch and WAL replay run the same expression and cannot disagree
   * (a LIST argument whose head names a callable, such as (`upd;(`in;1 2)),
   * reaches the handler as the list, as it does in kdb+, on both paths). */
  ray_t *form = q_journal_form(req); /* NULL: not a call, evaluate as is */
  if (form != NULL && RAY_IS_ERR(form))
    return form;
  ray_raise_clear(); /* ray_eval_str does this; a raise left over is not ours */
  ray_t *res = ray_eval(form ? form : req);
  if (form != NULL)
    ray_release(form);
  /* A lazy result is an internal deferred-DAG value that cannot be
   * serialized; force it before it can reach the wire. */
  if (res != NULL && ray_is_lazy(res))
    res = ray_lazy_materialize(res);
  return res;
}

/* ---- WAL auto-journal ---------------------------------------------------
 *
 * core/ipc.c eval_payload_core logs every inbound native SYNC frame to the
 * open journal BEFORE evaluating it, so a crash mid-handler still leaves the
 * message on disk and `.log.replay` rebuilds the state. A q frame reaches the
 * runtime through eval_request instead, so it has to do the same itself or a
 * Rayfall service fed over `-q` journals nothing.
 *
 * The frame is written in the NATIVE format (ray_ipc_header_t + ray_ser_raw
 * payload) because that is what the journal replays. Same rules as the core:
 *   - SYNC only. ASYNC frames and RESPONSEs are not logged (the core treats
 *     sync as the state-mutation channel, so background pings and pushes do
 *     not pollute the log). NOTE: q feeds usually push `neg[h](`upd;x)`, which
 *     is async and therefore NOT journaled; a q publisher that needs durable
 *     writes must use a sync send.
 *   - Nothing is written when no journal is open or while one is replaying
 *     (ray_journal_write_bytes is a no-op then).
 *   - The restricted bit of the current eval context is copied into the
 *     logged header so replay re-imposes it.
 *   - If the write fails the request is NOT evaluated and the client gets an
 *     `io` error: an un-logged mutation defeats the point of having a WAL.
 *
 * Replay-fidelity: the native replay evaluates the deserialized object with a
 * plain ray_eval and does not re-mark RAY_EVAL_LITERAL_FALLBACK (the mark is
 * not part of the wire format). A q `(`upd;(`a;1))` journaled verbatim would
 * therefore replay its argument list as a CALL. Rather than persist something
 * different from what ran, the request is turned into ONE data form: every
 * LIST argument wrapped in `quote`, `(upd (quote (a 1)))`. That form is what
 * is journaled AND what eval_request evaluates, so live dispatch and replay
 * run the same expression by construction, including for a LIST argument
 * whose head names a callable (`(`upd;(`in;1 2))` hands `upd` the list on both
 * paths). Char-vector and non-list requests are journaled as they are, like
 * the core.
 *
 * Replay bound: the core's replay stops at a frame whose payload is larger
 * than 256 MiB (`hdr.size`), so a bigger one is refused here, before it is
 * written or evaluated: a handler must not succeed on a request whose log
 * entry would end recovery.
 *
 * Eval scope: the core evaluates an auto-journaled frame with the
 * auto-journal flag set, which makes `.log.write` refuse (the frame is
 * already in the log; a nested synthetic record would make replay differ
 * from the live run). q_read_body sets the same flag around the eval through
 * ray_ipc_set_auto_journal_eval when the core provides it. */
static ray_t *q_journal_form(ray_t *req) {
  if (req->type != RAY_LIST || ray_len(req) < 2)
    return NULL; /* journal the request itself */
  ray_t **elems = (ray_t **)ray_data(req);
  int64_t qid = ray_sym_intern("quote", 5);
  ray_t *out = ray_list_new(ray_len(req));
  if (out == NULL || RAY_IS_ERR(out))
    return out;
  out = ray_list_append(out, elems[0]);
  for (int64_t i = 1; out && !RAY_IS_ERR(out) && i < ray_len(req); i++) {
    /* Only list arguments are marked literal on the live path (see
     * eval_request), so only those are quoted; any other argument is
     * journaled as is and resolves on replay exactly as it did live. */
    if (elems[i] == NULL || elems[i]->type != RAY_LIST) {
      out = ray_list_append(out, elems[i]);
      continue;
    }
    ray_t *q = ray_list_new(2);
    ray_t *head = ray_sym(qid);
    if (q == NULL || RAY_IS_ERR(q) || head == NULL || RAY_IS_ERR(head)) {
      if (q && !RAY_IS_ERR(q))
        ray_release(q);
      if (head && !RAY_IS_ERR(head))
        ray_release(head);
      ray_release(out);
      return ray_error("oom", NULL);
    }
    q = ray_list_append(q, head);
    ray_release(head);
    q = ray_list_append(q, elems[i]);
    out = ray_list_append(out, q);
    ray_release(q);
  }
  return out;
}

/* Journal one inbound SYNC request. Returns NULL when it is safe to evaluate
 * (logged, or nothing to log), or an owned `io` error to reply with instead. */
#define Q_JOURNAL_MAX_PAYLOAD (256LL * 1024 * 1024)

int q_journal_size_ok(int64_t size) {
  return size > 0 && size <= Q_JOURNAL_MAX_PAYLOAD;
}

static ray_t *q_journal_request(ray_t *req) {
  if (req == NULL || RAY_IS_ERR(req) || !ray_journal_is_open())
    return NULL;
#ifndef RAY_IPC_HAS_SET_AUTO_JOURNAL_EVAL
  /* A journaled request must be evaluated in the core's auto-journal scope,
   * or a nested `.log.write` appends a synthetic record the replay cannot
   * reproduce.  This core cannot set that scope, so refuse the request here,
   * before anything is appended or evaluated, rather than journal it unsafely. */
  return ray_error("domain", "journaling an inbound q request needs a "
                             "rayforce core with ray_ipc_set_auto_journal_eval; "
                             "request refused");
#endif
  ray_t *form = q_journal_form(req);
  if (form != NULL && RAY_IS_ERR(form))
    return form;
  ray_t *obj = form ? form : req;

  int64_t size = ray_serde_size(obj);
  if (size > Q_JOURNAL_MAX_PAYLOAD) {
    if (form)
      ray_release(form);
    return ray_error("limit", "request of %lld bytes exceeds the %lld-byte limit "
                              "of a journal entry; refused",
                     (long long)size, (long long)Q_JOURNAL_MAX_PAYLOAD);
  }
  uint8_t *payload = size > 0 ? (uint8_t *)malloc((size_t)size) : NULL;
  ray_err_t je = RAY_ERR_IO;
  if (payload != NULL && ray_ser_raw(payload, obj) == size) {
    ray_ipc_header_t hdr = {
        .prefix = RAY_SERDE_PREFIX,
        .version = RAY_SERDE_WIRE_VERSION,
        .flags = ray_eval_get_restricted() ? RAY_IPC_FLAG_RESTRICTED : 0,
        .endian = RAY_SERDE_ENDIAN,
        .msgtype = RAY_IPC_MSG_SYNC,
        .size = size,
    };
    je = ray_journal_write_bytes(&hdr, payload, size);
  }
  free(payload);
  if (form)
    ray_release(form);
  if (je != RAY_OK) {
    fprintf(stderr, "log: ERROR  journal write failed (rc=%d) — refusing to "
                    "evaluate\n", (int)je);
    return ray_error("io", "journal write failed; mutation refused");
  }
  return NULL;
}

/* Encode `result` as a Q response and write it. A NULL result encodes as the
 * identity (::). q_encode stamps the header as SYNC, so flip the message-type
 * byte to RESPONSE before sending. */
static int64_t q_send_fn(int64_t fd, uint8_t *buf, int64_t len) {
  int flags = 0;
#ifdef MSG_NOSIGNAL
  flags |= MSG_NOSIGNAL;
#endif
  return (int64_t)send((int)fd, buf, (size_t)len, flags);
}

static int q_send_result(ray_poll_t *poll, ray_selector_t *sel, ray_t *result) {
  uint8_t *buf = NULL;
  int64_t len = 0;
  char err[128] = {0};
  /* An error object carries only its category. The explanation is either the
   * runtime's last-error message or, for an uncaught (raise x), the pending
   * raised value (as the REPL prints it). Load it into the last-error slot
   * right before q_encode, which frames it as "category: message" so a q
   * client sees `'domain: custom failure` rather than a bare `'domain`. */
  ray_t *loaded = NULL;
  if (result != NULL && RAY_IS_ERR(result)) {
    char raised[256];
    const char *detail = ray_error_msg();
    if (detail == NULL && ray_raise_render(raised, sizeof raised))
      detail = raised;
    if (detail != NULL) {
      char text[256];
      snprintf(text, sizeof text, "%s", detail);
      const char *code = ray_err_code(result);
      loaded = ray_error(code ? code : "error", "%s", text);
      result = loaded;
    }
  }
  int enc = q_encode(result, &buf, &len, err, sizeof err);
  if (loaded != NULL)
    ray_error_free(loaded);
  if (enc < 0) {
    ray_t *e = ray_error(err[0] ? err : "q server: encode failed", NULL);
    int rc = q_encode(e, &buf, &len, err, sizeof err);
    q_release_any(e);
    if (rc < 0)
      return -1;
  }
  buf[1] = Q_MSG_RESPONSE;
  int64_t queued = 0;
  for (ray_poll_buf_t *p = sel->tx.buf; p != NULL; p = p->next) {
    int64_t pending = p->size - p->offset;
    if (pending < 0 || queued > Q_MAX_BODY - pending) {
      free(buf);
      return -1;
    }
    queued += pending;
  }
  if (len > Q_MAX_BODY - queued) {
    free(buf);
    return -1;
  }
  ray_poll_buf_t *out = ray_poll_buf_new(len);
  if (out == NULL) {
    free(buf);
    return -1;
  }
  memcpy(out->data, buf, (size_t)len);
  free(buf);
  if (sel->tx.buf == NULL) {
    sel->tx.buf = out;
  } else {
    ray_poll_buf_t *tail = sel->tx.buf;
    while (tail->next != NULL)
      tail = tail->next;
    tail->next = out;
  }
  ray_poll_tx_request(poll, sel);
  return 0;
}

static void q_close_listener(ray_poll_t *poll, ray_selector_t *sel) {
  (void)poll;
  ray_sock_close((ray_sock_t)sel->fd);
}

static void q_close_connection_sigpipe(ray_sock_t fd) {
#ifdef SO_NOSIGPIPE
  int yes = 1;
  setsockopt((int)fd, SOL_SOCKET, SO_NOSIGPIPE, &yes, sizeof yes);
#else
  (void)fd;
#endif
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
  q_close_connection_sigpipe(nfd);

  q_conn_t *cd = (q_conn_t *)calloc(1, sizeof *cd);
  if (cd == NULL) {
    ray_sock_close(nfd);
    return NULL;
  }
  cd->magic = Q_CONN_MAGIC;

  ray_poll_reg_t reg = {0};
  reg.fd = (int64_t)nfd;
  reg.type = RAY_SEL_SOCKET;
  reg.recv_fn = q_recv_fn;
  reg.send_fn = q_send_fn;
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
  if (body < 0 || body > q_conn_max_body(cd)) {
    ray_poll_deregister(poll, sel->id);
    return NULL;
  }
  if (cd->hdr.msgtype != Q_MSG_ASYNC && cd->hdr.msgtype != Q_MSG_SYNC &&
      cd->hdr.msgtype != Q_MSG_RESPONSE) {
    ray_poll_deregister(poll, sel->id);
    return NULL;
  }
  if (cd->hdr.compressed > 1) {
    ray_poll_deregister(poll, sel->id);
    return NULL;
  }
  if (cd->hdr.msgtype == Q_MSG_RESPONSE && !cd->outbound) {
    ray_poll_deregister(poll, sel->id);
    return NULL;
  }
  free(cd->rx_body);
  cd->rx_body = NULL;
  cd->rx_body_len = 0;
  cd->rx_body_cap = 0;
  cd->rx_body_total = body;
  if (body == 0) {
    if (cd->hdr.msgtype != Q_MSG_RESPONSE) {
      ray_poll_deregister(poll, sel->id);
      return NULL;
    }
    if (cd->sync_waiting && !cd->sync_ready) {
      cd->sync_resp = ray_error("empty", "q: empty response body");
      cd->sync_ready = 1;
    }
    ray_poll_rx_request(poll, sel, (int64_t)sizeof(q_header_t));
    return NULL;
  }
  sel->rx.read_fn = q_read_body;
  ray_poll_rx_request(poll, sel,
                      body < Q_RX_CHUNK_SIZE ? body : Q_RX_CHUNK_SIZE);
  return NULL;
}

static ray_t *q_read_body(ray_poll_t *poll, ray_selector_t *sel) {
  q_conn_t *cd = (q_conn_t *)sel->data;
  if (!sel->rx.buf || sel->rx.buf->offset <= 0)
    return NULL;

  int64_t received = sel->rx.buf->offset;
  int64_t needed = cd->rx_body_len + received;
  if (needed < cd->rx_body_len || needed > cd->rx_body_total) {
    ray_poll_deregister(poll, sel->id);
    return NULL;
  }
  if (needed > cd->rx_body_cap) {
    int64_t cap = cd->rx_body_cap
                      ? cd->rx_body_cap
                      : (cd->rx_body_total < Q_RX_CHUNK_SIZE
                             ? cd->rx_body_total
                             : Q_RX_CHUNK_SIZE);
    while (cap < needed) {
      if (cap > cd->rx_body_total / 2) {
        cap = cd->rx_body_total;
        break;
      }
      cap *= 2;
    }
    uint8_t *body = (uint8_t *)realloc(cd->rx_body, (size_t)cap);
    if (body == NULL) {
      ray_poll_deregister(poll, sel->id);
      return NULL;
    }
    cd->rx_body = body;
    cd->rx_body_cap = cap;
  }
  memcpy(cd->rx_body + cd->rx_body_len, sel->rx.buf->data,
         (size_t)received);
  cd->rx_body_len = needed;

  if (cd->rx_body_len < cd->rx_body_total) {
    int64_t left = cd->rx_body_total - cd->rx_body_len;
    ray_poll_rx_request(poll, sel,
                        left < Q_RX_CHUNK_SIZE ? left : Q_RX_CHUNK_SIZE);
    return NULL;
  }

  /* q_decode fully materializes the request into ray_t objects, so the rx
   * buffer is free to reuse the moment it returns. */
  q_header_t hdr = cd->hdr;
  int64_t id = sel->id;
  char err[128] = {0};
  ray_t *req = q_decode_bounded(cd->rx_body, cd->rx_body_len, hdr.compressed,
                                q_conn_max_body(cd), err, sizeof err);
  /* Kept for the close below. The error's code holds 7 bytes, and the next
   * error made on this thread replaces its message, so copy the message now.
   * A compressed frame's body opens with the size it declares uncompressed,
   * which says more about a refused frame than its size on the wire. */
  char reason[256] = {0};
  if (req != NULL && RAY_IS_ERR(req)) {
    const char *msg = ray_error_msg();
    snprintf(reason, sizeof reason, "%s", msg ? msg : ray_err_code(req));
  }
  uint32_t size_uncompressed = 0;
  if (hdr.compressed && cd->rx_body_len >= 4)
    memcpy(&size_uncompressed, cd->rx_body, sizeof size_uncompressed);
  free(cd->rx_body);
  cd->rx_body = NULL;
  cd->rx_body_len = 0;
  cd->rx_body_cap = 0;
  cd->rx_body_total = 0;

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

  /* A frame a connection this process opened cannot decode is a hole in what
   * its peer sent. Nothing fills it: the peer will not send it again, and
   * the handler that should have seen it never runs. Closing makes the hole
   * a dead connection, which a subscriber notices and reconnects from. A kdb+
   * peer cannot push an error value, so from one an error here is the
   * decode's own. A rayforce-q peer can, since q_encode writes an error as
   * one, and that is treated the same: the subscriber cannot use it either.
   * An inbound peer gets the error instead, below, as it always has. */
  if (cd->outbound && (req == NULL || RAY_IS_ERR(req))) {
    if (hdr.compressed)
      fprintf(stderr,
              "q: a pushed frame of %u bytes, %u uncompressed, did not "
              "decode, closing the connection: %s\n",
              (unsigned)hdr.size, (unsigned)size_uncompressed,
              req ? reason : (err[0] ? err : "decode failed"));
    else
      fprintf(stderr,
              "q: a pushed frame of %u bytes did not decode, closing the "
              "connection: %s\n",
              (unsigned)hdr.size,
              req ? reason : (err[0] ? err : "decode failed"));
    q_release_any(req);
    ray_poll_deregister(poll, id);
    return NULL;
  }

  /* Published for the handler's duration only, and restored rather than
   * cleared: a handler that pumps another connection (a .q.send on it)
   * dispatches that connection's frames from inside this one. */
  int64_t prev_handle = q_current_handle;
  if (cd->outbound)
    q_current_handle = id;
  ray_t *result = NULL;
  int journaled = req && !RAY_IS_ERR(req) && hdr.msgtype == Q_MSG_SYNC &&
                  ray_journal_is_open();
  if (req && hdr.msgtype == Q_MSG_SYNC)
    result = q_journal_request(req); /* io/limit error => do not evaluate */
  if (result == NULL) {
#ifdef RAY_IPC_HAS_SET_AUTO_JOURNAL_EVAL
    /* The core's scope for an auto-journaled eval: `.log.write` refuses. */
    bool prev_scope = ray_ipc_set_auto_journal_eval(
        journaled ? true : ray_ipc_auto_journal_eval());
#else
    (void)journaled;
#endif
    result = req ? eval_request(req)
                 : ray_error("q server: malformed request", "%s",
                             err[0] ? err : "q server: decode failed");
#ifdef RAY_IPC_HAS_SET_AUTO_JOURNAL_EVAL
    ray_ipc_set_auto_journal_eval(prev_scope);
#endif
  }
  q_current_handle = prev_handle;
  if (req)
    q_release_any(req);

  if (hdr.msgtype !=
      Q_MSG_ASYNC) { /* sync expects a response, async does not */
    ray_selector_t *cur = ray_poll_get(poll, id); /* eval may have closed it */
    if (cur && q_send_result(poll, cur, result) < 0)
      ray_poll_deregister(poll, id);
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
    cd->magic = 0;
    free(cd->rx_body);
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
  reg.close_fn = q_close_listener;

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
    void *data = sel->data;
    sel->rx.read_fn(poll, sel);
    sel = ray_poll_get(poll, id);
    if (sel == NULL || sel->data != data)
      return -1;
    if (((q_conn_t *)data)->sync_ready)
      return 0;
  }
}

/* Send a complete frame without letting a slow peer park the shared event
 * loop forever. The socket is nonblocking; a configured timeout covers both
 * readiness waits and short writes. */
static int q_send_frame_until(int fd, const uint8_t *buf, size_t len,
                              int bounded, int64_t deadline_ms) {
  size_t sent = 0;
  while (sent < len) {
    int flags = 0;
#ifdef MSG_NOSIGNAL
    flags |= MSG_NOSIGNAL;
#endif
    ssize_t n = send(fd, buf + sent, len - sent, flags);
    if (n > 0) {
      sent += (size_t)n;
      continue;
    }
    if (n == 0) {
      errno = EPIPE;
      return -1;
    }
    if (errno == EINTR)
      continue;
    if (errno != EAGAIN && errno != EWOULDBLOCK)
      return -1;

    int wait_ms = -1;
    if (bounded) {
      int64_t left = deadline_ms - ray_time_now_ms();
      if (left <= 0) {
        errno = ETIMEDOUT;
        return -1;
      }
      wait_ms = left > INT_MAX ? INT_MAX : (int)left;
    }
    struct pollfd pfd = {.fd = fd, .events = POLLOUT};
    int ready;
    do {
      ready = poll(&pfd, 1, wait_ms);
    } while (ready < 0 && errno == EINTR);
    if (ready == 0) {
      errno = ETIMEDOUT;
      return -1;
    }
    if (ready < 0)
      return -1;
  }
  return 0;
}

/* The per-operation timeout q_connect applied to the socket (SO_RCVTIMEO).
 * Once the fd is non-blocking under a poll, the kernel no longer enforces
 * it, so q_conn_send has to — read it here before the switch. 0 = none. */
static int q_sock_recv_timeout_ms(int fd) {
  struct timeval tv = {0, 0};
  socklen_t n = sizeof tv;
  if (getsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, &n) < 0)
    return 0;
  int64_t ms = (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
  return ms > INT32_MAX ? INT32_MAX : (int)ms;
}

int64_t q_conn_attach(ray_poll_t *poll, int fd) {
  if (poll == NULL || fd < 0)
    return -1;
  int timeout_ms = q_sock_recv_timeout_ms(fd);
  ray_sock_set_nonblocking((ray_sock_t)fd);
  q_close_connection_sigpipe((ray_sock_t)fd);

  q_conn_t *cd = (q_conn_t *)calloc(1, sizeof *cd);
  if (cd == NULL)
    return -1;
  cd->magic = Q_CONN_MAGIC;
  cd->outbound = 1;
  cd->sync_timeout_ms = timeout_ms;

  ray_poll_reg_t reg = {0};
  reg.fd = (int64_t)fd;
  reg.type = RAY_SEL_SOCKET;
  reg.recv_fn = q_recv_fn;
  reg.send_fn = q_send_fn;
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
  if (sel == NULL || sel->close_fn != q_on_close || sel->data == NULL ||
      ((q_conn_t *)sel->data)->magic != Q_CONN_MAGIC)
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
  int bounded = cd->sync_timeout_ms > 0;
  int64_t deadline_ms = bounded ? ray_time_now_ms() + cd->sync_timeout_ms : 0;
  int send_rc = q_send_frame_until((int)sel->fd, req, (size_t)req_len, bounded,
                                   deadline_ms);
  free(req);
  if (send_rc < 0) {
    const char *code = errno == ETIMEDOUT ? "timeout" : "io";
    ray_poll_deregister(poll, id);
    return ray_error(code, "q: send failed");
  }

  cd->sync_waiting = 1;
  cd->sync_ready = 0;
  cd->sync_resp = NULL;

  /* The socket's recv timeout is the whole round-trip's budget, as it is on
   * the blocking path where the kernel enforces it per recv. */
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
    int wait_ms = -1;
    if (bounded) {
      int64_t left = deadline_ms - ray_time_now_ms();
      if (left <= 0) {
        /* The request is in flight and its RESPONSE may still land; left
         * open, the next sync send on this handle would claim it as its
         * own reply. Tear the connection down and let the caller
         * reconnect. q_on_close frees cd. */
        ray_poll_deregister(poll, id);
        return ray_error("timeout", "q: response timed out");
      }
      wait_ms = left > INT32_MAX ? INT32_MAX : (int)left;
    }
    int w = ray_sock_wait_readable_intr((ray_sock_t)sel->fd, wait_ms);
    if (w == -2 || w == 0)
      continue; /* signal, or the slice elapsed — the deadline check decides */
    if (w < 0) {
      cd->sync_waiting = 0;
      return ray_error("io", "q: recv failed");
    }
  }
}

int q_conn_is_handle(ray_poll_t *poll, int64_t id) {
  if (poll == NULL)
    return 0;
  ray_selector_t *sel = ray_poll_get(poll, id);
  return sel != NULL && sel->close_fn == q_on_close && sel->data != NULL &&
         ((q_conn_t *)sel->data)->magic == Q_CONN_MAGIC;
}

void q_conn_close(ray_poll_t *poll, int64_t id) {
  if (!q_conn_is_handle(poll, id))
    return;
  ray_poll_deregister(poll, id); /* q_on_close frees the state and the fd */
}
