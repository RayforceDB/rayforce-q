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

/*
 * q_env.c — register the Q IPC client as `.q.*` rayfall builtins.
 *
 * Compiled into a rayforce binary (see `make rayforce`) and called once at
 * startup, so any rayfall script or REPL session can talk to a Q server:
 *
 *   (set h (.q.connect "localhost" 5000 "" "" 0))
 *   (.q.send h "([] a:1 2 3; b:`x`y`z)")
 *   (.q.close h)
 *
 * This is the binary-side counterpart of a binding's glue layer: it converts
 * rayfall arguments to/from `ray_t` and calls into q.c. `.q.*` lives in the
 * reserved system namespace (like `.ipc.*` / `.os.*`), so it is bound with
 * ray_env_bind rather than the user-facing ray_env_set.
 */

#include "q.h"

#include "lang/env.h"  /* ray_fn_*, ray_env_bind, ray_env_bind_flat */
#include "lang/eval.h" /* RAY_FN_NONE */

#include <string.h>

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

/* (.q.connect host port [user password [timeout_ms]]) -> fd, or error. */
static ray_t *qb_connect(ray_t **args, int64_t n) {
  if (n < 2 || n > 5)
    return ray_error(".q.connect: host port [user password [timeout_ms]]",
                     NULL);
  if (args[0] == NULL || args[0]->type != -RAY_STR)
    return ray_error(".q.connect: host must be a string", NULL);
  int ok;
  int64_t port = q_atom_i64(args[1], &ok);
  if (!ok)
    return ray_error(".q.connect: port must be an integer", NULL);

  char host[256];
  size_t hn = ray_str_len(args[0]);
  if (hn >= sizeof host)
    return ray_error(".q.connect: host too long", NULL);
  memcpy(host, ray_str_ptr(args[0]), hn);
  host[hn] = '\0';

  char user[128], password[128];
  q_str_arg(n >= 3 ? args[2] : NULL, user, sizeof user);
  q_str_arg(n >= 4 ? args[3] : NULL, password, sizeof password);
  int timeout_ms = 0;
  if (n >= 5) {
    int tok;
    timeout_ms = (int)q_atom_i64(args[4], &tok);
    if (!tok)
      timeout_ms = 0;
  }

  int fd = q_connect(host, (int)port, user, password, timeout_ms);
  if (fd < 0) {
    switch (fd) {
    case Q_ERR_TIMEOUT:
      return ray_error(".q.connect: connect timed out", NULL);
    case Q_ERR_HANDSHAKE:
      return ray_error(".q.connect: handshake/auth failed", NULL);
    default:
      return ray_error(".q.connect: connect failed", NULL);
    }
  }
  return ray_i64(fd);
}

/* (.q.send handle msg) -> decoded response (may be a server error). */
static ray_t *qb_send(ray_t *handle, ray_t *msg) {
  int ok;
  int64_t fd = q_atom_i64(handle, &ok);
  if (!ok)
    return ray_error(".q.send: handle must be an integer", NULL);

  char err[128] = {0};
  ray_t *res = q_send((int)fd, msg, err, sizeof err);
  if (res == NULL)
    return ray_error(err[0] ? err : ".q.send: send failed", NULL);
  return res;
}

/* (.q.close handle) -> null. */
static ray_t *qb_close(ray_t *handle) {
  int ok;
  int64_t fd = q_atom_i64(handle, &ok);
  if (!ok)
    return ray_error(".q.close: handle must be an integer", NULL);
  q_close((int)fd);
  return RAY_NULL_OBJ;
}

/* Bind a builtin under its reserved-namespace name (`.q.*`): ray_env_bind
 * builds the namespace dict, ray_env_bind_flat registers the flat name for
 * REPL completion. Mirrors how the core registers `.sys.*` / `.os.*`. */
static void q_bind(const char *name, ray_t *fn) {
  int64_t id = ray_sym_intern(name, strlen(name));
  ray_env_bind(id, fn);
  ray_env_bind_flat(id, fn);
}

/* Call once after ray_runtime_create(), before the REPL / script runs. */
void q_env_register(void) {
  ray_t *f;

  f = ray_fn_vary(".q.connect", RAY_FN_NONE, qb_connect);
  q_bind(".q.connect", f);
  ray_release(f);

  f = ray_fn_binary(".q.send", RAY_FN_NONE, qb_send);
  q_bind(".q.send", f);
  ray_release(f);

  f = ray_fn_unary(".q.close", RAY_FN_NONE, qb_close);
  q_bind(".q.close", f);
  ray_release(f);
}
