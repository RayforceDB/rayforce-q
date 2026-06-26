/*
 * q_builtins.c — exposes the q Q client as rayfall builtins.
 *
 * The pinned rayforce core has no runtime .so loading, so a binding-independent
 * way to drive q from the rayfall language is to compile it into a small
 * rayforce-linked binary and register the functions in the global environment
 * with the runtime `ray_env_set` API. This file is that registration shim; it
 * is test-only (lives under test/) and is never shipped to bindings.
 *
 * Bound names (plain identifiers — `ray_env_set` rejects the reserved
 * `.`-root): (qconnect host port [user password [timeout_ms]]) -> int handle,
 * or error (qsend    handle msg) -> decoded rayforce object (may be a server
 * error) (qclose   handle)     -> null
 */

#include "q.h"

#include "lang/env.h"  /* ray_fn_unary / ray_fn_binary */
#include "lang/eval.h" /* ray_unary_fn / ray_binary_fn, RAY_FN_NONE */

#include <string.h>

/* Read a signed integer out of any fixed-width rayforce integer/temporal atom.
 * Sets *ok = 0 if the object is not such an atom. */
static int64_t q_atom_i64(ray_t *a, int *ok) {
  *ok = 1;
  if (a == NULL) {
    *ok = 0;
    return 0;
  }
  switch (a->type) {
  case -RAY_I64:
  case -RAY_TIMESTAMP:
    return a->i64;
  case -RAY_I32:
  case -RAY_DATE:
  case -RAY_TIME:
    return a->i32;
  case -RAY_I16:
    return a->i16;
  case -RAY_U8:
  case -RAY_BOOL:
    return a->u8;
  default:
    *ok = 0;
    return 0;
  }
}

/* Copy a RAY_STR atom into a NUL-terminated C buffer; "" if not a string. */
static void q_str_arg(ray_t *a, char *buf, size_t cap) {
  buf[0] = '\0';
  if (a == NULL || a->type != -RAY_STR)
    return;
  size_t n = ray_str_len(a);
  if (n >= cap)
    n = cap - 1;
  memcpy(buf, ray_str_ptr(a), n);
  buf[n] = '\0';
}

/* (qconnect host port [user password [timeout_ms]]) */
static ray_t *qb_connect(ray_t **args, int64_t n) {
  if (n < 2 || n > 5)
    return ray_error("arity",
                     "qconnect: host port [user password [timeout_ms]]");
  if (args[0] == NULL || args[0]->type != -RAY_STR)
    return ray_error("type", "qconnect: host must be a string");
  int ok;
  int64_t p = q_atom_i64(args[1], &ok);
  if (!ok)
    return ray_error("type", "qconnect: port must be an integer");

  char hbuf[256];
  size_t hn = ray_str_len(args[0]);
  if (hn >= sizeof hbuf)
    return ray_error("length", "qconnect: host too long");
  memcpy(hbuf, ray_str_ptr(args[0]), hn);
  hbuf[hn] = '\0';

  char ubuf[128], pbuf[128];
  q_str_arg(n >= 3 ? args[2] : NULL, ubuf, sizeof ubuf);
  q_str_arg(n >= 4 ? args[3] : NULL, pbuf, sizeof pbuf);
  int timeout_ms = 0;
  if (n >= 5) {
    int tok;
    timeout_ms = (int)q_atom_i64(args[4], &tok);
    if (!tok)
      timeout_ms = 0;
  }

  int fd = q_connect(hbuf, (int)p, ubuf, pbuf, timeout_ms);
  if (fd < 0) {
    /* Error code (not message) is what ray_fmt renders, so make it the
     * meaningful, assertable token. */
    switch (fd) {
    case Q_ERR_TIMEOUT:
      return ray_error("timeout", "qconnect: connect timed out");
    case Q_ERR_HANDSHAKE:
      /* short code: ray_fmt caps the displayed error code length */
      return ray_error("auth", "qconnect: handshake/auth failed");
    default:
      return ray_error("connect", "qconnect: connect failed");
    }
  }
  return ray_i64(fd);
}

static ray_t *qb_send(ray_t *handle, ray_t *msg) {
  int ok;
  int64_t fd = q_atom_i64(handle, &ok);
  if (!ok)
    return ray_error("type", "qsend: handle must be an integer");

  char err[128] = {0};
  ray_t *res = q_send((int)fd, msg, err, sizeof err);
  if (res == NULL) {
    /* Surface a meaningful code: a closed/invalid slot vs a send failure. */
    const char *code = strstr(err, "handle") ? "handle" : "send";
    return ray_error(code, err[0] ? err : "qsend: send failed");
  }
  /* res may itself be a RAY_ERROR (a Q server-side error); returning it
   * lets the rayfall `!-` assertion observe the decoded error. */
  return res;
}

static ray_t *qb_close(ray_t *handle) {
  int ok;
  int64_t fd = q_atom_i64(handle, &ok);
  if (!ok)
    return ray_error("type", "qclose: handle must be an integer");
  q_close((int)fd);
  return RAY_NULL_OBJ;
}

/* Bind the builtins into the current runtime's global environment. Call once,
 * after ray_runtime_create(). ray_env_set retains the function object, so the
 * local reference is released afterwards. */
void q_register_builtins(void) {
  ray_t *f;

  f = ray_fn_vary("qconnect", RAY_FN_NONE, qb_connect);
  ray_env_set(ray_sym_intern("qconnect", 8), f);
  ray_release(f);

  f = ray_fn_binary("qsend", RAY_FN_NONE, qb_send);
  ray_env_set(ray_sym_intern("qsend", 5), f);
  ray_release(f);

  f = ray_fn_unary("qclose", RAY_FN_NONE, qb_close);
  ray_env_set(ray_sym_intern("qclose", 6), f);
  ray_release(f);
}
