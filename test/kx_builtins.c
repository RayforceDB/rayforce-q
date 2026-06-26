/*
 * kx_builtins.c — exposes the kx KDB+ client as rayfall builtins.
 *
 * The pinned rayforce core has no runtime .so loading, so a binding-independent
 * way to drive kx from the rayfall language is to compile it into a small
 * rayforce-linked binary and register the functions in the global environment
 * with the runtime `ray_env_set` API. This file is that registration shim; it
 * is test-only and lives on the kx `tests` branch, never shipped to bindings.
 *
 * Bound names (plain identifiers — `ray_env_set` rejects the reserved `.`-root):
 *   (kxconnect host port)  -> int handle, or error
 *   (kxsend    handle msg) -> decoded rayforce object (may be a server error)
 *   (kxclose   handle)     -> null
 */

#include "kx.h"

#include "lang/env.h"  /* ray_fn_unary / ray_fn_binary */
#include "lang/eval.h" /* ray_unary_fn / ray_binary_fn, RAY_FN_NONE */

#include <string.h>

/* Read a signed integer out of any fixed-width rayforce integer/temporal atom.
 * Sets *ok = 0 if the object is not such an atom. */
static int64_t kx_atom_i64(ray_t *a, int *ok) {
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

static ray_t *kxb_connect(ray_t *host, ray_t *port) {
  if (host == NULL || host->type != -RAY_STR)
    return ray_error("type", "kxconnect: host must be a string");
  int ok;
  int64_t p = kx_atom_i64(port, &ok);
  if (!ok)
    return ray_error("type", "kxconnect: port must be an integer");

  char hbuf[256];
  size_t hn = ray_str_len(host);
  if (hn >= sizeof hbuf)
    return ray_error("length", "kxconnect: host too long");
  memcpy(hbuf, ray_str_ptr(host), hn);
  hbuf[hn] = '\0';

  int slot = kx_connect(hbuf, (int)p);
  if (slot < 0) {
    /* Error code (not message) is what ray_fmt renders, so make it the
     * meaningful, assertable token. */
    switch (slot) {
    case KX_ERR_SOCKET:
      return ray_error("connect", "kxconnect: connect failed");
    case KX_ERR_HANDSHAKE:
      return ray_error("handshake", "kxconnect: handshake failed");
    default:
      return ray_error("full", "kxconnect: connection table full");
    }
  }
  return ray_i64(slot);
}

static ray_t *kxb_send(ray_t *handle, ray_t *msg) {
  int ok;
  int64_t slot = kx_atom_i64(handle, &ok);
  if (!ok)
    return ray_error("type", "kxsend: handle must be an integer");

  char err[128] = {0};
  ray_t *res = kx_send((int)slot, msg, err, sizeof err);
  if (res == NULL) {
    /* Surface a meaningful code: a closed/invalid slot vs a send failure. */
    const char *code = strstr(err, "handle") ? "handle" : "send";
    return ray_error(code, err[0] ? err : "kxsend: send failed");
  }
  /* res may itself be a RAY_ERROR (a KDB+ server-side error); returning it
   * lets the rayfall `!-` assertion observe the decoded error. */
  return res;
}

static ray_t *kxb_close(ray_t *handle) {
  int ok;
  int64_t slot = kx_atom_i64(handle, &ok);
  if (!ok)
    return ray_error("type", "kxclose: handle must be an integer");
  kx_close((int)slot);
  return RAY_NULL_OBJ;
}

/* Bind the builtins into the current runtime's global environment. Call once,
 * after ray_runtime_create(). ray_env_set retains the function object, so the
 * local reference is released afterwards. */
void kx_register_builtins(void) {
  ray_t *f;

  f = ray_fn_binary("kxconnect", RAY_FN_NONE, kxb_connect);
  ray_env_set(ray_sym_intern("kxconnect", 9), f);
  ray_release(f);

  f = ray_fn_binary("kxsend", RAY_FN_NONE, kxb_send);
  ray_env_set(ray_sym_intern("kxsend", 6), f);
  ray_release(f);

  f = ray_fn_unary("kxclose", RAY_FN_NONE, kxb_close);
  ray_env_set(ray_sym_intern("kxclose", 7), f);
  ray_release(f);
}
