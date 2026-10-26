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

#include <rayforce.h>

#include "core/poll.h"    /* ray_poll_create / run / destroy        */
#include "core/runtime.h" /* ray_runtime_set_poll                   */
#include "q.h"            /* q_decode / q_connect / q_exchange      */
#include "q_server.h"     /* q_serve                                */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <unistd.h>

/* Registers `.q.connect` / `.q.send` / `.q.close` */
void q_env_register(void);
int64_t q_serve_from_args(ray_poll_t *poll, int argc, char **argv);

typedef struct {
  uint8_t endianness;
  uint8_t msgtype;
  uint8_t compressed;
  uint8_t reserved;
  uint32_t size;
} test_q_header_t;

/* --serve PORT: build a runtime + poll, start the Q server, run the loop. */
static int run_server(int port) {
  ray_runtime_t *rt = ray_runtime_create(0, NULL);
  if (rt == NULL) {
    fprintf(stderr, "driver: failed to create rayforce runtime\n");
    return 1;
  }
  ray_poll_t *poll = ray_poll_create();
  if (poll == NULL) {
    fprintf(stderr, "driver: failed to create poll\n");
    ray_runtime_destroy(rt);
    return 1;
  }
  ray_runtime_set_poll(poll);
  if (q_serve(poll, port) < 0) {
    fprintf(stderr, "driver: cannot listen on port %d\n", port);
    ray_poll_destroy(poll);
    ray_runtime_destroy(rt);
    return 1;
  }
  ray_poll_run(poll);
  ray_poll_destroy(poll);
  ray_runtime_destroy(rt);
  return 0;
}

/* The journal admits a request only up to what the core's replay accepts
 * (hdr.size <= 256 MiB). A LIST [symbol, U8 vector of N bytes] is a q body of
 * N+17 and a native payload of N+26, so the largest admitted q body of exactly
 * 256 MiB (N = 256 MiB - 17) has a payload of 256 MiB + 9: refused. */
static int run_journal_bound_selftest(void) {
  const int64_t max = 256LL * 1024 * 1024;
  int failures = 0;
  failures += !q_journal_size_ok(1);
  failures += !q_journal_size_ok(max);
  failures += q_journal_size_ok(max + 1);
  failures += q_journal_size_ok((max - 17) + 26);
  failures += !q_journal_size_ok((max - 26) + 26);
  failures += q_journal_size_ok(0);
  failures += q_journal_size_ok(-1);
  printf("journal bound selftest: %s\n", failures ? "FAIL" : "ok");
  return failures ? 1 : 0;
}

/* --serve-args ARGS...: as --serve, but the listener comes from the same
 * q_serve_from_args the rayforce binary uses (`-q [HOST:]PORT`), so the CLI
 * and its default bind address are what is under test. */
static int run_server_args(int argc, char **argv) {
  ray_runtime_t *rt = ray_runtime_create(0, NULL);
  if (rt == NULL)
    return 1;
  q_env_register();
  ray_poll_t *poll = ray_poll_create();
  if (poll == NULL) {
    ray_runtime_destroy(rt);
    return 1;
  }
  ray_runtime_set_poll(poll);
  if (q_serve_from_args(poll, argc, argv) < 0) {
    ray_poll_destroy(poll);
    ray_runtime_destroy(rt);
    return 2;
  }
  ray_poll_run(poll);
  ray_poll_destroy(poll);
  ray_runtime_destroy(rt);
  return 0;
}

static int run_q_cli_selftest(void) {
  ray_runtime_t *rt = ray_runtime_create(0, NULL);
  if (rt == NULL) {
    fprintf(stderr, "q CLI selftest: failed to create runtime\n");
    return 1;
  }
  ray_poll_t *poll = ray_poll_create();
  if (poll == NULL) {
    fprintf(stderr, "q CLI selftest: failed to create poll\n");
    ray_runtime_destroy(rt);
    return 1;
  }

  char *no_flag[] = {"rayforce", "--file", "app.rfl"};
  char *bad_port[] = {"rayforce", "-q", "70000"};
  char *junk_port[] = {"rayforce", "--q-serve", "1abc"};
  char *missing_port[] = {"rayforce", "-q"};
  char *app_args[] = {"rayforce", "--", "-q", "70000"};
  char *bad_hostport[] = {"rayforce", "-q", "127.0.0.1:abc"};
  char *empty_port[] = {"rayforce", "-q", "127.0.0.1:"};
  char *bad_host[] = {"rayforce", "-q", "not-an-ip:25999"};
  int failures = 0;
  if (q_serve_from_args(poll, 3, no_flag) != 0)
    failures++;
  if (q_serve_from_args(poll, 3, bad_port) >= 0)
    failures++;
  if (q_serve_from_args(poll, 3, junk_port) >= 0)
    failures++;
  if (q_serve_from_args(poll, 2, missing_port) >= 0)
    failures++;
  if (q_serve_from_args(poll, 4, app_args) != 0)
    failures++;
  if (q_serve_from_args(poll, 3, bad_hostport) >= 0)
    failures++;
  if (q_serve_from_args(poll, 3, empty_port) >= 0)
    failures++;
  if (q_serve_from_args(poll, 3, bad_host) >= 0) /* never widens to wildcard */
    failures++;

  ray_poll_destroy(poll);
  ray_runtime_destroy(rt);
  printf("q CLI selftest: %s\n", failures ? "FAIL" : "ok");
  return failures ? 1 : 0;
}

static int fmt_eq(ray_t *a, ray_t *b) {
  if (a == NULL && b == NULL)
    return 1;
  if (a == NULL || b == NULL)
    return 0;
  ray_t *sa = ray_fmt(a, 0);
  ray_t *sb = ray_fmt(b, 0);
  int eq = sa && sb && ray_str_len(sa) == ray_str_len(sb) &&
           memcmp(ray_str_ptr(sa), ray_str_ptr(sb), ray_str_len(sa)) == 0;
  if (sa)
    ray_release(sa);
  if (sb)
    ray_release(sb);
  return eq;
}

static void fmt_into(ray_t *v, char *out, size_t cap) {
  ray_t *s = v ? ray_fmt(v, 0) : NULL;
  size_t n = s ? ray_str_len(s) : 0;
  if (n >= cap)
    n = cap - 1;
  if (n > 0)
    memcpy(out, ray_str_ptr(s), n);
  out[n] = '\0';
  if (s)
    ray_release(s);
}

static size_t rstrip(char *s, size_t len) {
  while (len > 0 && (s[len - 1] == ' ' || s[len - 1] == '\t' ||
                     s[len - 1] == '\r' || s[len - 1] == '\n'))
    len--;
  s[len] = '\0';
  return len;
}

static char *lstrip(char *p, size_t len) {
  size_t i = 0;
  while (i < len && (p[i] == ' ' || p[i] == '\t'))
    i++;
  return (i < len) ? (p + i) : NULL;
}

/* Find a separator (" -- " / " !- ") outside of a string literal. */
static char *find_top_sep(char *s, const char *marker) {
  size_t mlen = strlen(marker);
  int in_str = 0, esc = 0;
  for (char *p = s; *p; p++) {
    char c = *p;
    if (esc) {
      esc = 0;
      continue;
    }
    if (c == '\\') {
      esc = 1;
      continue;
    }
    if (c == '"') {
      in_str = !in_str;
      continue;
    }
    if (in_str)
      continue;
    if (strncmp(p, marker, mlen) == 0)
      return p;
  }
  return NULL;
}

/* Run one .rfl file. Returns 0 on pass, 1 on failure. */
static int run_rfl_file(const char *path) {
  FILE *f = fopen(path, "rb");
  if (!f) {
    fprintf(stderr, "  cannot open %s\n", path);
    return 1;
  }
  fseek(f, 0, SEEK_END);
  long n = ftell(f);
  rewind(f);
  char *src = (char *)malloc((size_t)n + 1);
  size_t r = fread(src, 1, (size_t)n, f);
  src[r] = '\0';
  fclose(f);

  int line_no = 0, assert_count = 0, failed = 0;
  char *p = src;

  while (*p) {
    char *nl = strchr(p, '\n');
    size_t line_len = nl ? (size_t)(nl - p) : strlen(p);
    line_no++;
    char saved = nl ? *nl : '\0';
    if (nl)
      *nl = '\0';

    line_len = rstrip(p, line_len);
    char *start = lstrip(p, line_len);
    if (!start || (start[0] == ';' && start[1] == ';'))
      goto next;

    char *eq = find_top_sep(start, " -- ");
    char *er = find_top_sep(start, " !- ");

    if (eq) {
      assert_count++;
      *eq = '\0';
      char *lhs = start, *rhs = eq + 4;
      ray_t *le = ray_eval_str(lhs);
      if (RAY_IS_ERR(le)) {
        char b[512];
        fmt_into(le, b, sizeof b);
        fprintf(stderr, "  %s:%d: LHS error: %s  -- src: %s\n", path, line_no,
                b, lhs);
        ray_error_free(le);
        failed = 1;
        goto next;
      }
      ray_t *re = ray_eval_str(rhs);
      if (RAY_IS_ERR(re)) {
        char b[512];
        fmt_into(re, b, sizeof b);
        fprintf(stderr, "  %s:%d: RHS error: %s  -- src: %s\n", path, line_no,
                b, rhs);
        ray_release(le);
        ray_error_free(re);
        failed = 1;
        goto next;
      }
      if (!fmt_eq(le, re)) {
        char lb[512], rb[512];
        fmt_into(le, lb, sizeof lb);
        fmt_into(re, rb, sizeof rb);
        fprintf(stderr, "  %s:%d: expected \"%s\", got \"%s\"  -- src: %s\n",
                path, line_no, rb, lb, lhs);
        failed = 1;
      }
      ray_release(le);
      ray_release(re);
    } else if (er) {
      assert_count++;
      *er = '\0';
      char *expr = start, *substr = er + 4;
      ray_t *ev = ray_eval_str(expr);
      if (!RAY_IS_ERR(ev)) {
        char b[512];
        fmt_into(ev, b, sizeof b);
        fprintf(stderr, "  %s:%d: expected error \"%s\", got: %s  -- src: %s\n",
                path, line_no, substr, b, expr);
        if (ev)
          ray_release(ev);
        failed = 1;
        goto next;
      }
      ray_t *es = ray_fmt(ev, 0);
      const char *ep = es ? ray_str_ptr(es) : "";
      if (!strstr(ep, substr)) {
        fprintf(stderr, "  %s:%d: error \"%s\" missing \"%s\"  -- src: %s\n",
                path, line_no, ep, substr, expr);
        failed = 1;
      }
      if (es)
        ray_release(es);
      ray_error_free(ev);
    } else {
      ray_t *ev = ray_eval_str(start);
      if (ev && RAY_IS_ERR(ev)) {
        char b[512];
        fmt_into(ev, b, sizeof b);
        fprintf(stderr, "  %s:%d: eval error: %s  -- src: %s\n", path, line_no,
                b, start);
        ray_error_free(ev);
        failed = 1;
      } else if (ev) {
        ray_release(ev);
      }
    }

  next:
    if (nl)
      *nl = saved;
    p = nl ? nl + 1 : p + line_len;
  }

  if (!failed && assert_count == 0) {
    fprintf(stderr, "  %s: no assertions found\n", path);
    failed = 1;
  }
  free(src);
  printf("  %-40s %s (%d assertions)\n", path, failed ? "FAIL" : "ok",
         assert_count);
  return failed;
}

/* Eval one setup expression, discarding the result. */
static void eval_setup(const char *fmt, const char *arg) {
  char buf[256];
  snprintf(buf, sizeof buf, fmt, arg);
  ray_t *r = ray_eval_str(buf);
  if (r && !RAY_IS_ERR(r))
    ray_release(r);
}

/* Bind the server coordinates the .rfl files connect to (qhost/qport) plus
 * the auth server (qauthport) and credentials (quser/qpass), so tests stay
 * free of hard-coded values. */
static void inject_server(const char *host, const char *port,
                          const char *authport, const char *user,
                          const char *pass) {
  eval_setup("(set qhost \"%s\")", host);
  eval_setup("(set qport %s)", port);
  eval_setup("(set qauthport %s)", authport);
  eval_setup("(set quser \"%s\")", user);
  eval_setup("(set qpass \"%s\")", pass);
}

static void release_any(ray_t *r) {
  if (r == NULL)
    return;
  if (RAY_IS_ERR(r))
    ray_error_free(r);
  else
    ray_release(r);
}

/* ---- q_decode_cols -------------------------------------------------- */

#define TEST_ATTR_DICT 0x02 /* q.c's RAY_ATTR_DICT, marking a keyed table */

/* q_encode v. Returns the whole message (header + body) to free(), or NULL. */
static uint8_t *encode_value(ray_t *v, int64_t *len) {
  char err[128] = {0};
  uint8_t *msg = NULL;
  if (q_encode(v, &msg, len, err, sizeof err) < 0) {
    fprintf(stderr, "cols selftest: q_encode failed: %s\n", err);
    return NULL;
  }
  return msg;
}

static uint8_t *encode_expr(const char *src, int64_t *len) {
  ray_t *v = ray_eval_str(src);
  if (v == NULL || RAY_IS_ERR(v)) {
    fprintf(stderr, "cols selftest: cannot evaluate %s\n", src);
    release_any(v);
    return NULL;
  }
  uint8_t *msg = encode_value(v, len);
  ray_release(v);
  return msg;
}

/* A keyed table as q.c decodes one: (key table; value table) + dict attr. */
static ray_t *keyed(ray_t *k, ray_t *v) {
  ray_t *kt = ray_list_new(0);
  kt = ray_list_append(kt, k);
  kt = ray_list_append(kt, v);
  kt->attrs |= TEST_ATTR_DICT;
  return kt;
}

/* q_decode_cols(got body, names) must format exactly as q_decode(want body). */
static int expect_cols(const char *what, uint8_t *got, int64_t got_len,
                       int got_compressed, const char *const *names, int n,
                       uint8_t *want, int64_t want_len, int want_compressed) {
  char err[128] = {0};
  ray_t *g =
      q_decode_cols(got, got_len, got_compressed, names, n, err, sizeof err);
  ray_t *w = q_decode(want, want_len, want_compressed, err, sizeof err);
  int ok = g != NULL && w != NULL && !RAY_IS_ERR(g) && !RAY_IS_ERR(w) &&
           fmt_eq(g, w);
  if (!ok) {
    char gs[512], ws[512];
    fmt_into(g, gs, sizeof gs);
    fmt_into(w, ws, sizeof ws);
    fprintf(stderr, "cols selftest: %s: got %s, want %s %s\n", what, gs, ws,
            err);
  }
  release_any(g);
  release_any(w);
  return ok ? 0 : 1;
}

/* Cutting a message anywhere must give an error, never a value or a crash,
 * wherever the cut falls in a skipped column. */
static int expect_cuts_fail(const char *what, uint8_t *body, int64_t len,
                            const char *const *names, int n) {
  char err[128];
  for (int64_t cut = 0; cut < len; cut++) {
    ray_t *r = q_decode_cols(body, cut, 0, names, n, err, sizeof err);
    int decoded = r != NULL && !RAY_IS_ERR(r);
    release_any(r);
    if (decoded) {
      fprintf(stderr, "cols selftest: %s cut at %lld of %lld decoded\n", what,
              (long long)cut, (long long)len);
      return 1;
    }
  }
  return 0;
}

/* q_decode_cols over q_encode'd and hand-built messages; needs no q. */
static int cols_codec_checks(void) {
  int failures = 0;
  char err[128];
  ray_t *r = NULL, *w = NULL;
  static const char *T =
      "(table ['a 's 'str 'f 'd 'ts] (list [1 2 3] ['alpha 'beta 'gamma] "
      "(list \"ab\" \"c\" \"\") [1.5 2.5 3.5] [2024.01.15 2024.01.16 "
      "2024.01.17] [2024.01.15D09:30:00.000000000 "
      "2024.01.15D09:30:01.000000000 2024.01.15D09:30:02.000000000]))";
  int64_t full_len = 0, proj_len = 0, kt_len = 0, ktf_len = 0, vec_len = 0;
  uint8_t *full = encode_expr(T, &full_len);
  uint8_t *proj = encode_expr(
      "(table ['str 'a] (list (list \"ab\" \"c\" \"\") [1 2 3]))", &proj_len);
  uint8_t *vec = encode_expr("[1 2 3]", &vec_len);
  ray_t *k = ray_eval_str("(table ['k] (list [7 8 9]))");
  ray_t *v = ray_eval_str(T);
  ray_t *vf = ray_eval_str("(table ['f] (list [1.5 2.5 3.5]))");
  ray_t *kt = NULL, *ktf = NULL;
  uint8_t *ktm = NULL, *ktfm = NULL;
  if (full == NULL || proj == NULL || vec == NULL || k == NULL ||
      RAY_IS_ERR(k) || v == NULL || RAY_IS_ERR(v) || vf == NULL ||
      RAY_IS_ERR(vf)) {
    fprintf(stderr, "cols selftest: fixtures did not build\n");
    failures++;
    goto out;
  }
  kt = keyed(k, v);
  ktf = keyed(k, vf);
  ktm = encode_value(kt, &kt_len);
  ktfm = encode_value(ktf, &ktf_len);
  if (ktm == NULL || ktfm == NULL) {
    failures++;
    goto out;
  }

  const char *str_a[] = {"str", "a"};
  failures += expect_cols("requested order", full + 8, full_len - 8, 0, str_a,
                          2, proj + 8, proj_len - 8, 0);

  /* Keyed: the key comes back whole and the names pick value columns; a
   * name the key side holds needs no value column. */
  const char *f[] = {"f"}, *f_k[] = {"f", "k"};
  failures += expect_cols("keyed", ktm + 8, kt_len - 8, 0, f, 1, ktfm + 8,
                          ktf_len - 8, 0);
  failures += expect_cols("keyed, key named", ktm + 8, kt_len - 8, 0, f_k, 2,
                          ktfm + 8, ktf_len - 8, 0);

  failures += expect_cols("no names", full + 8, full_len - 8, 0, NULL, 0,
                          full + 8, full_len - 8, 0);
  failures += expect_cols("not a table", vec + 8, vec_len - 8, 0, f, 1, vec + 8,
                          vec_len - 8, 0);

  const char *missing[] = {"a", "nope"};
  r = q_decode_cols(full + 8, full_len - 8, 0, missing, 2, err, sizeof err);
  const char *msg = r != NULL && RAY_IS_ERR(r) ? ray_error_msg() : NULL;
  if (msg == NULL || strstr(msg, "`nope`") == NULL) {
    fprintf(stderr, "cols selftest: missing column not reported: %s\n",
            msg ? msg : err);
    failures++;
  }
  release_any(r);

  const char *twice[] = {"a", "a"}, *null_name[] = {"a", NULL};
  err[0] = '\0';
  r = q_decode_cols(full + 8, full_len - 8, 0, twice, 2, err, sizeof err);
  if (r != NULL || strstr(err, "named twice") == NULL) {
    fprintf(stderr, "cols selftest: repeated name accepted: %s\n", err);
    failures++;
  }
  release_any(r);
  err[0] = '\0';
  r = q_decode_cols(full + 8, full_len - 8, 0, null_name, 2, err, sizeof err);
  if (r != NULL || strstr(err, "null column name") == NULL) {
    fprintf(stderr, "cols selftest: NULL name accepted: %s\n", err);
    failures++;
  }
  release_any(r);

  /* ([] a:1 2; n:0D00:00:01 0D00:00:02; qcolsskipped:`qcolsunseen`), by
   * hand: rayforce cannot encode a timespan. Skipping n works although
   * q_decode rejects it, and the skipped symbol column's name and values
   * are never interned. */
  uint8_t span[] = {/* table, attrs, dict; column names `a`n`qcolsskipped */
                    98, 0, 99, 11, 0, 3, 0, 0, 0, 'a', 0, 'n', 0, 'q', 'c', 'o',
                    'l', 's', 's', 'k', 'i', 'p', 'p', 'e', 'd', 0,
                    /* values: a list of 3 columns */
                    0, 0, 3, 0, 0, 0,
                    /* a: longs 1 2 */
                    7, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0,
                    0, 0,
                    /* n: timespans 1s 2s */
                    16, 0, 2, 0, 0, 0, 0x00, 0xca, 0x9a, 0x3b, 0, 0, 0, 0, 0x00,
                    0x94, 0x35, 0x77, 0, 0, 0, 0,
                    /* qcolsskipped: symbols `qcolsunseen` */
                    11, 0, 2, 0, 0, 0, 'q', 'c', 'o', 'l', 's', 'u', 'n', 's',
                    'e', 'e', 'n', 0, 0};
  const char *a[] = {"a"}, *n[] = {"n"};
  r = q_decode_cols(span, (int64_t)sizeof span, 0, a, 1, err, sizeof err);
  w = ray_eval_str("(table ['a] (list [1 2]))");
  if (r == NULL || RAY_IS_ERR(r) || !fmt_eq(r, w)) {
    fprintf(stderr, "cols selftest: timespan column was not skipped: %s\n",
            err);
    failures++;
  }
  if (ray_sym_find("qcolsunseen", 11) >= 0 ||
      ray_sym_find("qcolsskipped", 12) >= 0) {
    fprintf(stderr, "cols selftest: a skipped column was interned\n");
    failures++;
  }
  release_any(r);
  release_any(w);
  r = q_decode_cols(span, (int64_t)sizeof span, 0, n, 1, err, sizeof err);
  if (r == NULL || !RAY_IS_ERR(r) || strcmp(ray_err_code(r), "q: unsu") != 0) {
    fprintf(stderr, "cols selftest: a kept timespan was not rejected\n");
    failures++;
  }
  release_any(r);

  /* Symbols are skipped a word at a time: end a skipped symbol column at
   * every offset within a word, with the next column's bytes after it. */
  w = ray_eval_str("(table ['a] (list [7]))");
  for (int k = 1; k <= 17; k++) {
    char src[64], sym[18];
    memset(sym, 's', (size_t)k);
    sym[k] = '\0';
    snprintf(src, sizeof src, "(table ['s 'a] (list ['%s] [7]))", sym);
    int64_t m_len = 0;
    uint8_t *m = encode_expr(src, &m_len);
    r = m ? q_decode_cols(m + 8, m_len - 8, 0, a, 1, err, sizeof err) : NULL;
    if (r == NULL || RAY_IS_ERR(r) || !fmt_eq(r, w)) {
      fprintf(stderr, "cols selftest: a %d-char symbol was mis-skipped\n", k);
      failures++;
    }
    release_any(r);
    free(m);
  }
  release_any(w);

  failures += expect_cuts_fail("encoded table", full + 8, full_len - 8, f, 1);
  failures +=
      expect_cuts_fail("timespan table", span, (int64_t)sizeof span, a, 1);

out:
  free(full);
  free(proj);
  free(vec);
  free(ktm);
  free(ktfm);
  release_any(kt);
  release_any(ktf);
  release_any(k);
  release_any(v);
  release_any(vf);
  return failures;
}

static int run_codec_selftest(void) {
  int failures = 0;
  ray_runtime_t *rt = ray_runtime_create(0, NULL);
  if (rt == NULL) {
    fprintf(stderr, "codec selftest: failed to create rayforce runtime\n");
    return 1;
  }

  char err[128] = {0};
  uint8_t int_with_tail[] = {250, 42, 0, 0, 0, 0xff};
  ray_t *r = q_decode(int_with_tail, (int64_t)sizeof int_with_tail, 0, err,
                      sizeof err);
  if (r != NULL || strstr(err, "trailing bytes") == NULL) {
    fprintf(stderr, "codec selftest: trailing body bytes were not rejected\n");
    failures++;
  }
  release_any(r);

  err[0] = '\0';
  uint8_t qerr[] = {128, 'b', 'a', 'd', 0};
  r = q_decode(qerr, (int64_t)sizeof qerr, 0, err, sizeof err);
  if (r == NULL || !RAY_IS_ERR(r)) {
    fprintf(stderr, "codec selftest: Q error frame did not decode as error\n");
    failures++;
  }
  release_any(r);

  err[0] = '\0';
  uint8_t qidentity[] = {101, 0};
  r = q_decode(qidentity, (int64_t)sizeof qidentity, 0, err, sizeof err);
  if (r != RAY_NULL_OBJ) {
    fprintf(stderr, "codec selftest: Q identity did not decode as null: %s\n",
            err);
    failures++;
  }
  release_any(r);

  err[0] = '\0';
  uint8_t bad_table_marker[] = {98, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 0,
                                0,  0, 0, 0,  0, 0, 0, 0};
  r = q_decode(bad_table_marker, (int64_t)sizeof bad_table_marker, 0, err,
               sizeof err);
  ray_t *rs = r ? ray_fmt(r, 0) : NULL;
  const char *rp = rs ? ray_str_ptr(rs) : err;
  if (r == NULL || !RAY_IS_ERR(r) || strstr(rp, "q: malf") == NULL) {
    fprintf(stderr, "codec selftest: malformed table marker was accepted\n");
    failures++;
  }
  if (rs)
    ray_release(rs);
  release_any(r);

  /* A table whose one column is a long atom: ray_table_add_col rejects the
   * column and releases the table itself, so the decode must not release it
   * again, only return the error. */
  uint8_t atom_column[] = {98, 0, 99, 11, 0,    1,  0, 0, 0, 'a', 0, 0, 0,
                           1,  0, 0,  0,  0xF9, 42, 0, 0, 0, 0,   0, 0, 0};
  r = q_decode(atom_column, (int64_t)sizeof atom_column, 0, err, sizeof err);
  if (r == NULL || !RAY_IS_ERR(r)) {
    fprintf(stderr, "codec selftest: a table with an atom column decoded\n");
    failures++;
  }
  release_any(r);

  /* The most a classic header can declare is past even the client's
   * limit, so this is refused before anything is allocated. */
  uint8_t oversized_compressed[4];
  uint32_t oversized_size = UINT32_MAX;
  memcpy(oversized_compressed, &oversized_size, sizeof oversized_size);
  err[0] = '\0';
  r = q_decode(oversized_compressed, (int64_t)sizeof oversized_compressed, 1,
               err, sizeof err);
  if (r != NULL || strstr(err, "decompression failed") == NULL) {
    fprintf(stderr, "codec selftest: oversized compressed body was accepted\n");
    failures++;
  }
  release_any(r);

  if (q_connect("127.0.0.1", 70000, "", "", 1) != Q_ERR_SOCKET) {
    fprintf(stderr, "codec selftest: client accepted out-of-range port\n");
    failures++;
  }

  /* Unit-converted temporals, raw wire bytes so no q is needed: the q
   * type byte, then the little-endian payload. */
  struct {
    const char *name;
    uint8_t body[16];
    int64_t body_len;
    int8_t want_type;
    int64_t want_val; /* i32 sentinel-compared through the atom union */
  } temporal[] = {
      /* -17 minute 570 -> 09:30 = 34200000 ms */
      {"minute atom", {0xEF, 0x3A, 0x02, 0x00, 0x00}, 5, -RAY_TIME, 34200000},
      /* -18 second 34215 -> 09:30:15 = 34215000 ms */
      {"second atom", {0xEE, 0xA7, 0x85, 0x00, 0x00}, 5, -RAY_TIME, 34215000},
      /* -17 minute 0Wu (INT32_MAX) has no i32-ms form -> null time */
      {"minute inf", {0xEF, 0xFF, 0xFF, 0xFF, 0x7F}, 5, -RAY_TIME, NULL_I32},
      /* -13 month 289 (2024.02m) -> 2024.02.01 = 8797 days since 2000 */
      {"month atom", {0xF3, 0x21, 0x01, 0x00, 0x00}, 5, -RAY_DATE, 8797},
      /* -13 month -1 (1999.12m) -> 1999.12.01 = -31 days */
      {"month neg", {0xF3, 0xFF, 0xFF, 0xFF, 0xFF}, 5, -RAY_DATE, -31},
      /* -15 datetime 1e6 days (~2737 years): past the ns range -> null */
      {"datetime range",
       {0xF1, 0x00, 0x00, 0x00, 0x00, 0x80, 0x84, 0x2E, 0x41},
       9,
       -RAY_TIMESTAMP,
       NULL_I64},
  };
  for (size_t i = 0; i < sizeof temporal / sizeof temporal[0]; i++) {
    err[0] = '\0';
    r = q_decode(temporal[i].body, temporal[i].body_len, 0, err, sizeof err);
    int64_t got = 0;
    if (r != NULL && !RAY_IS_ERR(r))
      got = (r->type == -RAY_TIMESTAMP) ? r->i64 : (int64_t)r->i32;
    if (r == NULL || RAY_IS_ERR(r) || r->type != temporal[i].want_type ||
        got != temporal[i].want_val) {
      fprintf(stderr,
              "codec selftest: %s decoded wrong (type %d val %lld, want "
              "type %d val %lld) %s\n",
              temporal[i].name, r ? (int)r->type : 0, (long long)got,
              (int)temporal[i].want_type, (long long)temporal[i].want_val, err);
      failures++;
    }
    release_any(r);
  }

  /* Q timespans (KN) are durations, not timestamps. Rayforce has no
   * duration type, so reject them rather than silently changing meaning. */
  const struct {
    const char *name;
    uint8_t body[6];
    int64_t body_len;
  } unsupported_temporals[] = {
      {"timespan atom", {0xF0, 0, 0, 0, 0, 0}, 1},
      {"timespan vector", {0x10, 0, 0, 0, 0, 0}, 6},
  };
  for (size_t i = 0;
       i < sizeof unsupported_temporals / sizeof unsupported_temporals[0];
       i++) {
    err[0] = '\0';
    r = q_decode((uint8_t *)unsupported_temporals[i].body,
                 unsupported_temporals[i].body_len, 0, err, sizeof err);
    if (r == NULL || !RAY_IS_ERR(r) ||
        strcmp(ray_err_code(r), "q: unsu") != 0) {
      fprintf(stderr, "codec selftest: %s was not rejected: %s\n",
              unsupported_temporals[i].name,
              r && RAY_IS_ERR(r) ? ray_err_code(r) : err);
      failures++;
    }
    release_any(r);
  }

  failures += cols_codec_checks();

  int listener = socket(AF_INET, SOCK_STREAM, 0);
  if (listener < 0) {
    perror("codec selftest: handshake socket");
    failures++;
  } else {
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(listener, (struct sockaddr *)&addr, sizeof addr) < 0 ||
        listen(listener, 1) < 0) {
      perror("codec selftest: handshake listener");
      failures++;
      close(listener);
    } else {
      socklen_t addr_len = sizeof addr;
      if (getsockname(listener, (struct sockaddr *)&addr, &addr_len) < 0) {
        perror("codec selftest: handshake address");
        close(listener);
        failures++;
        goto handshake_done;
      }
      pid_t child = fork();
      if (child < 0) {
        perror("codec selftest: handshake fork");
        close(listener);
        failures++;
        goto handshake_done;
      } else if (child == 0) {
        int peer = accept(listener, NULL, NULL);
        if (peer >= 0) {
          uint8_t b;
          while (recv(peer, &b, 1, 0) == 1 && b != 0)
            ;
          b = 0xff;
          send(peer, &b, 1, 0);
          close(peer);
        }
        close(listener);
        _exit(0);
      }
      close(listener);
      int bad_cap =
          q_connect("127.0.0.1", ntohs(addr.sin_port), "", "", 1000);
      if (bad_cap != Q_ERR_HANDSHAKE) {
        fprintf(stderr, "codec selftest: invalid handshake capability accepted\n");
        if (bad_cap >= 0)
          q_close(bad_cap);
        failures++;
      }
      waitpid(child, NULL, 0);
    }
  }
handshake_done:;

  ray_poll_t *poll = ray_poll_create();
  if (poll == NULL) {
    fprintf(stderr, "codec selftest: failed to create poll\n");
    failures++;
  } else {
    if (q_serve(poll, -1) >= 0 || q_serve(poll, 70000) >= 0) {
      fprintf(stderr, "codec selftest: server accepted out-of-range port\n");
      failures++;
    }
    ray_poll_destroy(poll);
  }

  ray_runtime_destroy(rt);
  printf("codec selftest: %s\n", failures ? "FAIL" : "ok");
  return failures ? 1 : 0;
}

static int run_exchange_selftest(void) {
  int sv[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
    perror("exchange selftest: socketpair");
    return 1;
  }

  test_q_header_t h = {
      .endianness = 1,
      .msgtype = 1,
      .compressed = 0,
      .reserved = 0,
      .size = (uint32_t)(sizeof(test_q_header_t) + 1),
  };
  uint8_t body = 101; /* identity */
  int failures = 0;
  if (send(sv[1], &h, sizeof h, 0) != (ssize_t)sizeof h ||
      send(sv[1], &body, sizeof body, 0) != (ssize_t)sizeof body) {
    perror("exchange selftest: send");
    failures++;
  }

  uint8_t req = 0;
  uint8_t *resp = NULL;
  int64_t resp_len = 0;
  int compressed = 0;
  char err[128] = {0};
  int rc = q_exchange(sv[0], &req, 1, &resp, &resp_len, &compressed, err,
                      sizeof err);
  if (rc == 0 || strstr(err, "response message type") == NULL) {
    fprintf(stderr,
            "exchange selftest: non-response frame was not rejected: %s\n",
            err);
    failures++;
  }
  free(resp);
  close(sv[0]);
  close(sv[1]);

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
    perror("exchange selftest: socketpair closed-peer");
    failures++;
  } else {
    close(sv[1]);
    resp = NULL;
    resp_len = 0;
    compressed = 0;
    err[0] = '\0';
    rc = q_exchange(sv[0], &req, 1, &resp, &resp_len, &compressed, err,
                    sizeof err);
    if (rc == 0 || strstr(err, "send") == NULL) {
      fprintf(stderr,
              "exchange selftest: closed peer did not fail cleanly: %s\n",
              err);
      failures++;
    }
    free(resp);
    close(sv[0]);
  }

  /* Poll-attached round-trip against a peer that never answers: the recv
   * timeout on the socket (what q_connect's timeout_ms sets) must bound
   * q_conn_send and close the handle, instead of parking the event loop
   * indefinitely. */
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
    perror("exchange selftest: socketpair silent-peer");
    failures++;
  } else {
    ray_runtime_t *rt = ray_runtime_create(0, NULL);
    ray_poll_t *poll = rt ? ray_poll_create() : NULL;
    if (poll == NULL) {
      fprintf(stderr, "exchange selftest: failed to create runtime/poll\n");
      failures++;
    } else {
      struct timeval tv = {.tv_sec = 0, .tv_usec = 200 * 1000};
      setsockopt(sv[0], SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
      int64_t id = q_conn_attach(poll, sv[0]);
      if (id < 0)
        close(sv[0]);
      ray_t *msg = ray_str("1+1", 3);
      /* A regression here is an unbounded wait: let SIGALRM fail the run
       * instead of hanging it. */
      alarm(10);
      ray_t *r = id >= 0 ? q_conn_send(poll, id, msg) : NULL;
      alarm(0);
      ray_t *es = (r && RAY_IS_ERR(r)) ? ray_fmt(r, 0) : NULL;
      const char *ep = es ? ray_str_ptr(es) : "";
      if (r == NULL || !RAY_IS_ERR(r) || strstr(ep, "timeout") == NULL) {
        fprintf(stderr, "exchange selftest: silent peer did not time out: %s\n",
                ep);
        failures++;
      }
      if (ray_poll_get(poll, id) != NULL) {
        fprintf(stderr, "exchange selftest: timed-out handle was left open\n");
        failures++;
      }
      if (es)
        ray_release(es);
      release_any(r);
      ray_release(msg);
      ray_poll_destroy(poll);
    }
    if (rt)
      ray_runtime_destroy(rt);
    close(sv[1]);
  }

  printf("exchange selftest: %s\n", failures ? "FAIL" : "ok");
  return failures ? 1 : 0;
}

#if defined(TCP_KEEPIDLE)
#define TEST_TCP_KEEPIDLE TCP_KEEPIDLE
#elif defined(TCP_KEEPALIVE) /* macOS spelling */
#define TEST_TCP_KEEPIDLE TCP_KEEPALIVE
#endif

static int sockopt_int(int fd, int level, int name) {
  int v = -1;
  socklen_t n = sizeof v;
  return getsockopt(fd, level, name, &v, &n) < 0 ? -1 : v;
}

typedef struct {
  const char *what;
  int keepalive_ms; /* what q_connect_opts is given */
  int on, idle, intvl, cnt;
  int user_timeout_ms; /* 0 = not armed */
} keepalive_case_t;

static int expect_sockopt(const keepalive_case_t *c, const char *when,
                          const char *opt, int got, int want) {
  if (got == want)
    return 0;
  fprintf(stderr, "keepalive selftest: %s, %s: %s is %d, want %d\n", c->what,
          when, opt, got, want);
  return 1;
}

/* The core's mapping (ray_sock_set_keepalive): idle budget/2, interval
 * budget/6, both whole seconds of at least 1, three probes. */
static int expect_keepalive(const keepalive_case_t *c, const char *when,
                            int fd) {
  int failures = 0;
  /* BSD reports a set boolean option as its flag bit, not 1. */
  failures +=
      expect_sockopt(c, when, "SO_KEEPALIVE",
                     sockopt_int(fd, SOL_SOCKET, SO_KEEPALIVE) != 0, c->on);
  if (c->on) {
#ifdef TEST_TCP_KEEPIDLE
    failures += expect_sockopt(c, when, "the keepalive idle time",
                               sockopt_int(fd, IPPROTO_TCP, TEST_TCP_KEEPIDLE),
                               c->idle);
#endif
#ifdef TCP_KEEPINTVL
    failures +=
        expect_sockopt(c, when, "TCP_KEEPINTVL",
                       sockopt_int(fd, IPPROTO_TCP, TCP_KEEPINTVL), c->intvl);
#endif
#ifdef TCP_KEEPCNT
    failures +=
        expect_sockopt(c, when, "TCP_KEEPCNT",
                       sockopt_int(fd, IPPROTO_TCP, TCP_KEEPCNT), c->cnt);
#endif
  }
#if defined(TCP_USER_TIMEOUT)
  failures += expect_sockopt(c, when, "TCP_USER_TIMEOUT",
                             sockopt_int(fd, IPPROTO_TCP, TCP_USER_TIMEOUT),
                             c->user_timeout_ms);
#elif defined(TCP_RXT_CONNDROPTIME) /* macOS: whole seconds */
  failures += expect_sockopt(c, when, "TCP_RXT_CONNDROPTIME",
                             sockopt_int(fd, IPPROTO_TCP, TCP_RXT_CONNDROPTIME),
                             c->user_timeout_ms / 1000);
#endif
  return failures;
}

/* --keepalive-selftest HOST PORT: connect to `driver --serve` with each
 * keepalive choice and read the socket options back, before and after
 * q_conn_attach, against the core's mapping. Mirrors the core's
 * test_ipc_keepalive_sockopts. */
static int run_keepalive_selftest(const char *host, int port) {
  static const keepalive_case_t cases[] = {
      /* q_connect: the core's 60 s default, no user timeout. */
      {"q_connect", Q_KEEPALIVE_AUTO, 1, 30, 10, 3, 0},
      /* A 4 s budget: 4000/6000 truncates to 0 and the core raises it to 1. */
      {"a 4000 ms budget", 4000, 1, 2, 1, 3, 4000},
      {"Q_KEEPALIVE_OFF", Q_KEEPALIVE_OFF, 0, 0, 0, 0, 0},
  };
  ray_runtime_t *rt = ray_runtime_create(0, NULL);
  ray_poll_t *poll = rt ? ray_poll_create() : NULL;
  if (poll == NULL) {
    fprintf(stderr, "keepalive selftest: failed to create runtime/poll\n");
    if (rt)
      ray_runtime_destroy(rt);
    return 1;
  }

  int failures = 0;
  for (size_t i = 0; i < sizeof cases / sizeof cases[0]; i++) {
    const keepalive_case_t *c = &cases[i];
    int fd = c->keepalive_ms == Q_KEEPALIVE_AUTO
                 ? q_connect(host, port, "", "", 5000)
                 : q_connect_opts(host, port, "", "", 5000, c->keepalive_ms);
    if (fd < 0) {
      fprintf(stderr, "keepalive selftest: %s: cannot connect to %s:%d (%d)\n",
              c->what, host, port, fd);
      failures++;
      continue;
    }
    failures += expect_keepalive(c, "connected", fd);
    int64_t id = q_conn_attach(poll, fd);
    if (id < 0) {
      fprintf(stderr, "keepalive selftest: %s: q_conn_attach failed\n",
              c->what);
      q_close(fd);
      failures++;
      continue;
    }
    failures += expect_keepalive(c, "attached", fd);

    if (c->keepalive_ms > 0) {
      /* The link still answers with the options set. */
      ray_t *msg = ray_str("(+ 1 2)", 7);
      ray_t *r = q_conn_send(poll, id, msg);
      if (r == NULL || r->type != -RAY_I64 || r->i64 != 3) {
        char s[256];
        fmt_into(r, s, sizeof s);
        fprintf(stderr, "keepalive selftest: %s: (+ 1 2) answered %s\n",
                c->what, s);
        failures++;
      }
      release_any(r);
      ray_release(msg);
    }
  }

  /* Closes every attached connection. */
  ray_poll_destroy(poll);
  ray_runtime_destroy(rt);
  printf("keepalive selftest: %s\n", failures ? "FAIL" : "ok");
  return failures ? 1 : 0;
}

/* Send a q expression and return the raw response body to free(), or NULL. */
static uint8_t *q_fetch(int fd, const char *expr, int64_t *len,
                        int *compressed) {
  char err[128] = {0};
  ray_t *msg = ray_str(expr, strlen(expr));
  uint8_t *req = NULL, *resp = NULL;
  int64_t req_len = 0;
  int rc = q_encode(msg, &req, &req_len, err, sizeof err);
  ray_release(msg);
  if (rc == 0) {
    rc = q_exchange(fd, req, req_len, &resp, len, compressed, err, sizeof err);
    free(req);
  }
  if (rc < 0) {
    fprintf(stderr, "cols selftest: %s: %s\n", expr, err);
    return NULL;
  }
  return resp;
}

/* --cols-selftest HOST PORT: q_decode_cols over bytes real q serialized,
 * against q_decode of the same projection made by q (cols#t). */
static int run_cols_selftest(const char *host, int port) {
  int failures = 0;
  ray_runtime_t *rt = ray_runtime_create(0, NULL);
  int fd = q_connect(host, port, "", "", 5000);
  if (rt == NULL || fd < 0) {
    fprintf(stderr, "cols selftest: cannot reach q at %s:%d\n", host, port);
    if (rt)
      ray_runtime_destroy(rt);
    return 1;
  }

  /* Every q type q_decode reads, plus a timespan it rejects, with the
   * attributes an HDB carries. Columns are listed out of table order. */
  static const char *setup =
      "t:([] date:2024.01.15 2024.01.15 2024.01.16; sym:`p#`a`a`b; "
      "id:`s#1 2 3; str:(\"ab\";enlist \"c\";\"\"); mx:(1;`a;\"x\"); "
      "b:101b; x:0x0102ff; h:(1h;0Nh;3h); i:(1i;2i;0Ni); e:(1.5e;0Ne;2.5e); "
      "px:1.5 0n 3.5; c:\"xyz\"; m:(2024.01m;2024.02m;0Nm); "
      "u:(09:30;09:31;0Nu); v:(09:30:00;09:30:01;0Nv); "
      "tm:(09:30:00.000;09:30:00.001;0Nt); "
      "z:(2024.01.15T09:30:00.000;0Nz;2024.01.16T00:00:00.000); "
      "ts:2024.01.15D09:30:00+0D00:00:01*til 3; "
      "span:(0D00:00:01;0D00:00:02;0Nn); "
      "g:\"G\"$(\"d49f18a4-1969-49e8-9b8a-6bb9a4832eea\";\"\";"
      "\"00000000-0000-0000-0000-000000000001\"));"
      "kt:`id xkey t; big:1000#t; 1b";
  static const struct {
    const char *what, *full, *proj;
    const char *names[4];
    int n;
  } cases[] = {
      {"q table", "t", "`str`date`px`sym#t", {"str", "date", "px", "sym"}, 4},
      {"q keyed table", "kt", "(key kt)!`px`str#value kt", {"px", "str"}, 2},
  };

  int64_t len = 0, full_len = 0, proj_len = 0;
  int cz = 0, full_cz = 0, proj_cz = 0;
  char err[128] = {0};
  uint8_t *ok = q_fetch(fd, setup, &len, &cz);
  ray_t *okv = ok ? q_decode(ok, len, cz, err, sizeof err) : NULL;
  if (okv == NULL || RAY_IS_ERR(okv)) {
    char s[256];
    fmt_into(okv, s, sizeof s);
    fprintf(stderr, "cols selftest: q setup failed: %s %s\n", s, err);
    failures++;
  }
  release_any(okv);
  free(ok);

  for (size_t i = 0; failures == 0 && i < sizeof cases / sizeof cases[0]; i++) {
    uint8_t *full = q_fetch(fd, cases[i].full, &full_len, &full_cz);
    uint8_t *proj = q_fetch(fd, cases[i].proj, &proj_len, &proj_cz);
    if (full == NULL || proj == NULL)
      failures++;
    else
      failures +=
          expect_cols(cases[i].what, full, full_len, full_cz, cases[i].names,
                      cases[i].n, proj, proj_len, proj_cz);
    free(full);
    free(proj);
  }

  /* A compressed message: q's own -18! output, header and all. */
  if (failures == 0) {
    uint8_t *z = q_fetch(fd, "-18!big", &len, &cz);
    ray_t *zv = z ? q_decode(z, len, cz, err, sizeof err) : NULL;
    uint8_t *proj = q_fetch(fd, "`str`px#big", &proj_len, &proj_cz);
    const char *str_px[] = {"str", "px"};
    if (zv == NULL || RAY_IS_ERR(zv) || zv->type != RAY_U8 || zv->len < 9 ||
        ((uint8_t *)ray_data(zv))[2] != 1 || proj == NULL) {
      fprintf(stderr, "cols selftest: q did not return a compressed table\n");
      failures++;
    } else {
      uint8_t *msg = (uint8_t *)ray_data(zv);
      failures += expect_cols("q compressed table", msg + 8, zv->len - 8, 1,
                              str_px, 2, proj, proj_len, proj_cz);
    }
    release_any(zv);
    free(z);
    free(proj);
  }

  q_close(fd);
  ray_runtime_destroy(rt);
  printf("cols selftest: %s\n", failures ? "FAIL" : "ok");
  return failures ? 1 : 0;
}

/* ---- frame limits --------------------------------------------------- */

/* A long vector past the listener's 256 MiB limit and under the client's
 * 2 GiB: 300 MiB in runs of a thousand equal elements, so it compresses to a
 * few MiB yet a copy decompression zeroed or misplaced shows. The last
 * element breaks the pattern, so a vector cut short shows too. */
#define BIG_LONGS ((int64_t)300 * 1024 * 1024 / 8)
#define BIG_LAST 42

static int64_t big_elem(int64_t i, int64_t n) {
  return i == n - 1 ? BIG_LAST : 1 + (i / 1000) % 250;
}

/* (`upd;...): a list of two whose first item is the symbol upd. */
static const uint8_t UPD_PREFIX[] = {0, 0, 2, 0, 0, 0, 0xF5, 'u', 'p', 'd', 0};

/* (`count;...), which the listener evaluates to the vector's length. */
static const uint8_t COUNT_PREFIX[] = {0,   0,   2,   0,   0,   0, 0xF5,
                                       'c', 'o', 'u', 'n', 't', 0};

/* A RESPONSE carrying the identity. */
static const uint8_t IDENTITY_REPLY[] = {1, 2, 0, 0, 10, 0, 0, 0, 101, 0};

/* q's compression, as q_decompress reads it. A flag byte leads every 8
 * items, and an item is a literal byte or a back-reference: the xor of the
 * byte pair it repeats, which keys the pair table both sides rebuild the same
 * way, then how many bytes past the pair to copy. Writes the uncompressed
 * message size and the stream to `out`, which holds len + len / 8 + 16
 * bytes, and returns the bytes written. */
static int64_t q_compress(const uint8_t *in, int64_t len, uint8_t *out) {
  uint32_t table[256] = {0};
  uint32_t total = (uint32_t)(len + (int64_t)sizeof(test_q_header_t));
  memcpy(out, &total, sizeof total);
  int64_t s = 0, p = 0, d = 4, flags = 0;
  int bit = 0;
  while (s < len) {
    if (bit == 0) {
      flags = d++;
      out[flags] = 0;
      bit = 1;
    }
    int64_t r = s + 1 < len ? table[in[s] ^ in[s + 1]] : 0;
    int ref =
        s + 1 < len && r + 1 < s && in[r] == in[s] && in[r + 1] == in[s + 1];
    int64_t n = 0;
    if (ref) {
      while (n < 255 && s + 2 + n < len && in[r + 2 + n] == in[s + 2 + n])
        n++;
      out[flags] |= (uint8_t)bit;
      out[d++] = in[s] ^ in[s + 1];
      out[d++] = (uint8_t)n;
      s += 2;
    } else {
      out[d++] = in[s++];
    }
    for (; p < s - 1; p++)
      table[in[p] ^ in[p + 1]] = (uint32_t)p;
    if (ref) {
      s += n;
      p = s;
    }
    bit = bit == 128 ? 0 : bit * 2;
  }
  return d;
}

/* A message: the header, `body` (compressed when asked), then `tail` as is.
 * The tail is how a test queues a RESPONSE behind the frame under test.
 * Returns the message to free(), or NULL. */
static uint8_t *test_message(uint8_t msgtype, const uint8_t *body,
                             int64_t body_len, int compress,
                             const uint8_t *tail, int64_t tail_len,
                             int64_t *len) {
  int64_t hdr = (int64_t)sizeof(test_q_header_t);
  int64_t cap = hdr + body_len + (compress ? body_len / 8 + 16 : 0) + tail_len;
  uint8_t *buf = (uint8_t *)malloc((size_t)cap);
  if (buf == NULL)
    return NULL;
  int64_t n = body_len;
  if (compress)
    n = q_compress(body, body_len, buf + hdr);
  else
    memcpy(buf + hdr, body, (size_t)body_len);
  test_q_header_t h = {.endianness = 1,
                       .msgtype = msgtype,
                       .compressed = (uint8_t)compress,
                       .size = (uint32_t)(hdr + n)};
  memcpy(buf, &h, sizeof h);
  if (tail_len > 0)
    memcpy(buf + hdr + n, tail, (size_t)tail_len);
  *len = hdr + n + tail_len;
  return buf;
}

/* `prefix` then a vector of `longs` longs in big_elem's pattern. The vector
 * alone, a response's body, starts prefix_len bytes in. */
static uint8_t *call_body(const uint8_t *prefix, size_t prefix_len,
                          int64_t longs, int64_t *len) {
  int64_t n = (int64_t)prefix_len + 6 + longs * 8;
  uint8_t *b = (uint8_t *)malloc((size_t)n);
  if (b == NULL)
    return NULL;
  memcpy(b, prefix, prefix_len);
  uint8_t *v = b + prefix_len;
  uint32_t count = (uint32_t)longs;
  v[0] = 7; /* long vector, no attribute */
  v[1] = 0;
  memcpy(v + 2, &count, sizeof count);
  for (int64_t i = 0; i < longs; i++) {
    int64_t x = big_elem(i, longs);
    memcpy(v + 6 + 8 * i, &x, sizeof x);
  }
  *len = n;
  return b;
}

static int is_big_vector(ray_t *v) {
  if (v == NULL || RAY_IS_ERR(v) || v->type != RAY_I64 ||
      ray_len(v) != BIG_LONGS)
    return 0;
  const int64_t *x = (const int64_t *)ray_data(v);
  for (int64_t i = 0; i < BIG_LONGS; i++)
    if (x[i] != big_elem(i, BIG_LONGS))
      return 0;
  return 1;
}

/* An async (`upd;v) cut to half, with `tail` behind it. Compressed, the
 * decompression runs out of input and the decode is NULL. Plain, the vector
 * runs past the frame and the decode is an error object. */
static uint8_t *truncated_push(int compress, const uint8_t *tail,
                               int64_t tail_len, int64_t *len) {
  uint8_t body[sizeof UPD_PREFIX + 6 + 8 * 1000] = {0};
  memcpy(body, UPD_PREFIX, sizeof UPD_PREFIX);
  uint8_t *v = body + sizeof UPD_PREFIX;
  uint32_t count = 1000;
  v[0] = 7;
  memcpy(v + 2, &count, sizeof count);
  for (int i = 0; i < 1000; i++)
    v[6 + 8 * i] = (uint8_t)(i % 7);
  int64_t full = 0;
  uint8_t *msg =
      test_message(0, body, (int64_t)sizeof body, compress, NULL, 0, &full);
  if (msg == NULL)
    return NULL;
  int64_t hdr = (int64_t)sizeof(test_q_header_t);
  int64_t cut = hdr + (full - hdr) / 2;
  uint32_t size = (uint32_t)cut;
  memcpy(msg + 4, &size, sizeof size);
  uint8_t *out = (uint8_t *)realloc(msg, (size_t)(cut + tail_len));
  if (out == NULL) {
    free(msg);
    return NULL;
  }
  if (tail_len > 0)
    memcpy(out + cut, tail, (size_t)tail_len);
  *len = cut + tail_len;
  return out;
}

/* Fork a peer on sv[1] that writes `out`, then reads until the other end
 * closes and answers nothing. A RESPONSE the test needs is queued in `out`,
 * since a response need not wait for its request. Returns its pid, or -1. */
static pid_t fake_peer(int sv[2], const uint8_t *out, int64_t len) {
  pid_t pid = fork();
  if (pid == 0) {
    signal(SIGPIPE, SIG_IGN);
    close(sv[0]);
    for (int64_t sent = 0; sent < len;) {
      ssize_t n = send(sv[1], out + sent, (size_t)(len - sent), 0);
      if (n < 0 && errno == EINTR)
        continue;
      if (n <= 0)
        _exit(0);
      sent += n;
    }
    uint8_t sink[4096];
    while (recv(sv[1], sink, sizeof sink, 0) > 0)
      ;
    _exit(0);
  }
  close(sv[1]);
  return pid;
}

/* The plain client reads `msg`, a RESPONSE holding the big vector, through
 * q_exchange and q_decode. */
static int plain_client_reads_big(const uint8_t *msg, int64_t len,
                                  const char *what) {
  int sv[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
    perror("frame-limit selftest: socketpair");
    return 1;
  }
  pid_t pid = fake_peer(sv, msg, len);
  uint8_t req[] = {1, 1, 0, 0, 10, 0, 0, 0, 101, 0};
  uint8_t *resp = NULL;
  int64_t resp_len = 0;
  int compressed = 0;
  char err[128] = {0};
  ray_t *r = NULL;
  if (q_exchange(sv[0], req, sizeof req, &resp, &resp_len, &compressed, err,
                 sizeof err) == 0)
    r = q_decode(resp, resp_len, compressed, err, sizeof err);
  int failed = !is_big_vector(r);
  if (failed)
    fprintf(stderr, "frame-limit selftest: plain client, %s: %s\n", what,
            err[0] ? err : "wrong vector");
  release_any(r);
  free(resp);
  close(sv[0]);
  if (pid > 0)
    waitpid(pid, NULL, 0);
  return failed;
}

/* Attach a connection whose peer writes `msg`, then make one sync round-trip,
 * which dispatches every frame queued before the RESPONSE at the end of
 * `msg`. Returns the round-trip's result, and in *open whether the
 * connection outlived it. */
static ray_t *attached_round_trip(ray_poll_t *poll, const uint8_t *msg,
                                  int64_t len, int *open) {
  int sv[2];
  *open = 0;
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
    perror("frame-limit selftest: socketpair");
    return NULL;
  }
  pid_t pid = fake_peer(sv, msg, len);
  int64_t id = q_conn_attach(poll, sv[0]);
  ray_t *r = NULL;
  if (id < 0) {
    close(sv[0]);
  } else {
    ray_t *ask = ray_str("::", 2);
    /* A regression here can be an unbounded wait: let SIGALRM fail the run
     * instead of hanging it. */
    alarm(60);
    r = q_conn_send(poll, id, ask);
    alarm(0);
    ray_release(ask);
    *open = q_conn_is_handle(poll, id);
    q_conn_close(poll, id);
  }
  if (pid > 0)
    waitpid(pid, NULL, 0);
  return r;
}

/* An attached connection receives `body` pushed as an async frame, stays
 * open, and the bound upd sees the whole vector. */
static int attached_reads_big(ray_poll_t *poll, const uint8_t *body,
                              int64_t body_len, int compress,
                              const char *what) {
  int64_t len = 0;
  uint8_t *msg = test_message(0, body, body_len, compress, IDENTITY_REPLY,
                              sizeof IDENTITY_REPLY, &len);
  if (msg == NULL) {
    fprintf(stderr, "frame-limit selftest: %s: out of memory\n", what);
    return 1;
  }
  int open = 0;
  ray_t *r = attached_round_trip(poll, msg, len, &open);
  free(msg);
  ray_t *got = ray_eval_str("got");
  int failed = r == NULL || RAY_IS_ERR(r) || !open || !is_big_vector(got);
  if (failed)
    fprintf(stderr,
            "frame-limit selftest: attached, %s: upd did not see the whole "
            "vector\n",
            what);
  release_any(got);
  release_any(r);
  eval_setup("(set got %s)", "0");
  return failed;
}

/* An attached connection whose peer writes `msg` closes on it. */
static int attached_closes(ray_poll_t *poll, const uint8_t *msg, int64_t len,
                           const char *what) {
  int open = 0;
  ray_t *r = attached_round_trip(poll, msg, len, &open);
  int failed = r == NULL || !RAY_IS_ERR(r) || open;
  if (failed)
    fprintf(stderr, "frame-limit selftest: attached, %s stayed open\n", what);
  release_any(r);
  return failed;
}

/* The client side: frames on socketpairs, the peer a forked writer. */
static int frame_limit_client_checks(ray_poll_t *poll) {
  int failures = 0;
  int64_t body_len = 0;
  uint8_t *body =
      call_body(UPD_PREFIX, sizeof UPD_PREFIX, BIG_LONGS, &body_len);
  if (body == NULL) {
    fprintf(stderr, "frame-limit selftest: out of memory\n");
    return 1;
  }
  const uint8_t *vec = body + sizeof UPD_PREFIX;
  int64_t vec_len = body_len - (int64_t)sizeof UPD_PREFIX;
  for (int compress = 0; compress <= 1; compress++) {
    const char *what = compress ? "300 MiB compressed" : "300 MiB";
    int64_t len = 0;
    uint8_t *msg = test_message(2, vec, vec_len, compress, NULL, 0, &len);
    failures += msg ? plain_client_reads_big(msg, len, what) : 1;
    free(msg);
    failures += attached_reads_big(poll, body, body_len, compress, what);
  }
  free(body);

  /* The most a classic header can declare is past the client limit too. */
  uint8_t past_limit[sizeof(test_q_header_t) + sizeof IDENTITY_REPLY] = {
      1, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF};
  memcpy(past_limit + sizeof(test_q_header_t), IDENTITY_REPLY,
         sizeof IDENTITY_REPLY);
  failures += attached_closes(poll, past_limit, (int64_t)sizeof past_limit,
                              "a 4 GiB header");

  for (int compress = 0; compress <= 1; compress++) {
    int64_t cut_len = 0;
    uint8_t *cut = truncated_push(compress, IDENTITY_REPLY,
                                  (int64_t)sizeof IDENTITY_REPLY, &cut_len);
    failures += cut ? attached_closes(poll, cut, cut_len,
                                      compress ? "a compressed push cut short"
                                               : "a push cut short")
                    : 1;
    free(cut);
  }

  /* The decompression cap on its own, apart from a malformed stream: a valid
   * compressed long atom, 9 bytes, decodes at a cap of 9 and is refused at 8,
   * before anything is allocated for it. */
  uint8_t atom[9] = {0xF9};
  uint8_t packed[32];
  int64_t packed_len = q_compress(atom, (int64_t)sizeof atom, packed);
  for (int64_t cap = 9; cap >= 8; cap--) {
    char err[128] = {0};
    ray_t *r = q_decode_bounded(packed, packed_len, 1, cap, err, sizeof err);
    int decoded =
        r != NULL && !RAY_IS_ERR(r) && r->type == -RAY_I64 && r->i64 == 0;
    if (decoded != (cap == 9)) {
      fprintf(stderr,
              "frame-limit selftest: a 9-byte compressed atom at a cap of "
              "%lld: %s\n",
              (long long)cap, decoded ? "decoded" : err);
      failures++;
    }
    release_any(r);
  }
  return failures;
}

/* Send (`count;v), v of `longs` longs, to the listener on fd as a compressed
 * sync request, and return its decoded reply. */
static ray_t *listener_count(int fd, int64_t longs, char *err, size_t errlen) {
  int64_t body_len = 0, len = 0;
  uint8_t *body =
      call_body(COUNT_PREFIX, sizeof COUNT_PREFIX, longs, &body_len);
  uint8_t *msg =
      body ? test_message(1, body, body_len, 1, NULL, 0, &len) : NULL;
  free(body);
  uint8_t *resp = NULL;
  int64_t resp_len = 0;
  int compressed = 0;
  ray_t *r = NULL;
  if (msg != NULL &&
      q_exchange(fd, msg, len, &resp, &resp_len, &compressed, err, errlen) == 0)
    r = q_decode(resp, resp_len, compressed, err, errlen);
  free(msg);
  free(resp);
  return r;
}

/* A compressed request a few MiB on the wire that decompresses past 256 MiB
 * is refused before it is evaluated: the reply is an error, not the count. A
 * small request of the same shape is counted first, so the refusal is the
 * limit's and not a malformed frame's. */
static int listener_refuses_big_compressed(const char *host, int port) {
  int fd = q_connect(host, port, "", "", 5000);
  if (fd < 0) {
    fprintf(stderr, "frame-limit selftest: cannot reach %s:%d\n", host, port);
    return 1;
  }
  int failures = 0;
  char err[128] = {0};
  ray_t *r = listener_count(fd, 1000, err, sizeof err);
  if (r == NULL || RAY_IS_ERR(r) || r->type != -RAY_I64 || r->i64 != 1000) {
    fprintf(stderr,
            "frame-limit selftest: listener did not count a small compressed "
            "request: %s\n",
            err);
    failures++;
  }
  release_any(r);
  err[0] = '\0';
  r = listener_count(fd, BIG_LONGS, err, sizeof err);
  if (r == NULL || !RAY_IS_ERR(r)) {
    fprintf(stderr,
            "frame-limit selftest: listener took a request decompressing to "
            "300 MiB: %s\n",
            r == NULL ? err : "it answered the count");
    failures++;
  }
  release_any(r);
  q_close(fd);
  return failures;
}

/* The listener side, against driver --serve: a connection it accepted keeps
 * the 256 MiB limit, and an undecodable push there is dropped, not fatal. */
static int frame_limit_listener_checks(const char *host, int port) {
  int failures = 0;
  int fd = q_connect(host, port, "", "", 5000);
  if (fd < 0) {
    fprintf(stderr, "frame-limit selftest: cannot reach %s:%d\n", host, port);
    return 1;
  }
  test_q_header_t big = {
      .endianness = 1, .msgtype = 1, .size = (uint32_t)((int64_t)300 << 20)};
  uint8_t b = 0;
  ssize_t got = send(fd, &big, sizeof big, 0) == (ssize_t)sizeof big
                    ? recv(fd, &b, 1, 0)
                    : 1;
  /* Closed is EOF or a reset; a timeout means it waits for the body. */
  if (got > 0 || (got < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))) {
    fprintf(stderr, "frame-limit selftest: listener took a 300 MiB request\n");
    failures++;
  }
  q_close(fd);

  failures += listener_refuses_big_compressed(host, port);

  fd = q_connect(host, port, "", "", 5000);
  int64_t cut_len = 0;
  uint8_t *cut = truncated_push(1, NULL, 0, &cut_len);
  char err[128] = {0};
  ray_t *ask = ray_str("(+ 1 1)", 7);
  ray_t *r = NULL;
  if (fd >= 0 && cut != NULL &&
      send(fd, cut, (size_t)cut_len, 0) == (ssize_t)cut_len)
    r = q_send(fd, ask, err, sizeof err);
  if (r == NULL || RAY_IS_ERR(r)) {
    fprintf(stderr,
            "frame-limit selftest: listener closed on an undecodable push: "
            "%s\n",
            err);
    failures++;
  }
  release_any(r);
  ray_release(ask);
  free(cut);
  if (fd >= 0)
    q_close(fd);
  return failures;
}

/* --frame-limit-selftest HOST PORT: a connection rayforce-q opened takes
 * frames up to 2 GiB and closes on a push it cannot decode. One the listener
 * at HOST:PORT accepted keeps 256 MiB and survives such a push. */
static int run_frame_limit_selftest(const char *host, int port) {
  ray_runtime_t *rt = ray_runtime_create(0, NULL);
  ray_poll_t *poll = rt ? ray_poll_create() : NULL;
  if (poll == NULL) {
    fprintf(stderr, "frame-limit selftest: failed to create runtime/poll\n");
    if (rt)
      ray_runtime_destroy(rt);
    return 1;
  }
  eval_setup("(set upd (fn [x] (set got x)))", NULL);
  eval_setup("(set got %s)", "0");
  int failures = frame_limit_client_checks(poll);
  failures += frame_limit_listener_checks(host, port);
  ray_poll_destroy(poll);
  ray_runtime_destroy(rt);
  printf("frame-limit selftest: %s\n", failures ? "FAIL" : "ok");
  return failures ? 1 : 0;
}

int main(int argc, char **argv) {
  if (argc >= 2 && strcmp(argv[1], "--codec-selftest") == 0)
    return run_codec_selftest();

  if (argc >= 4 && strcmp(argv[1], "--frame-limit-selftest") == 0)
    return run_frame_limit_selftest(argv[2], atoi(argv[3]));

  if (argc >= 4 && strcmp(argv[1], "--cols-selftest") == 0)
    return run_cols_selftest(argv[2], atoi(argv[3]));

  if (argc >= 4 && strcmp(argv[1], "--keepalive-selftest") == 0)
    return run_keepalive_selftest(argv[2], atoi(argv[3]));

  if (argc >= 2 && strcmp(argv[1], "--exchange-selftest") == 0)
    return run_exchange_selftest();

  if (argc >= 2 && strcmp(argv[1], "--journal-bound-selftest") == 0)
    return run_journal_bound_selftest();
  if (argc >= 2 && strcmp(argv[1], "--q-cli-selftest") == 0)
    return run_q_cli_selftest();

  if (argc >= 3 && strcmp(argv[1], "--serve-args") == 0)
    return run_server_args(argc - 1, argv + 1);

  /* Server role: `driver --serve PORT`. */
  if (argc >= 3 && strcmp(argv[1], "--serve") == 0)
    return run_server(atoi(argv[2]));

  const char *host = "127.0.0.1";
  const char *port = "0", *authport = "0";
  const char *user = "", *pass = "";
  int use_poll = 0;
  int first = 1;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--poll") == 0) {
      /* Publish a poll into the runtime, so .q.connect puts connections on
       * an event loop instead of handing back a bare blocking fd. That is
       * the mode the rayforce binary always runs in, and the only one where
       * a peer can push to us — see test/rfl/push. */
      use_poll = 1;
      first = i + 1;
    } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
      host = argv[++i];
      first = i + 1;
    } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
      port = argv[++i];
      first = i + 1;
    } else if (strcmp(argv[i], "--authport") == 0 && i + 1 < argc) {
      authport = argv[++i];
      first = i + 1;
    } else if (strcmp(argv[i], "--user") == 0 && i + 1 < argc) {
      user = argv[++i];
      first = i + 1;
    } else if (strcmp(argv[i], "--pass") == 0 && i + 1 < argc) {
      pass = argv[++i];
      first = i + 1;
    } else {
      first = i;
      break;
    }
  }
  if (first >= argc) {
    fprintf(stderr,
            "usage: %s [--host H] [--port P] [--authport P] [--user U] "
            "[--pass P] file.rfl [file.rfl ...]\n",
            argv[0]);
    return 2;
  }

  int failures = 0, files = 0;
  for (int i = first; i < argc; i++) {
    /* Fresh runtime per file: isolates handles and `set` bindings. */
    ray_runtime_t *rt = ray_runtime_create(0, NULL);
    ray_poll_t *poll = NULL;
    if (use_poll) {
      poll = ray_poll_create();
      ray_runtime_set_poll(poll);
    }
    q_env_register();
    inject_server(host, port, authport, user, pass);
    failures += run_rfl_file(argv[i]);
    if (poll != NULL) {
      /* Destroy before the runtime: closing a selector releases ray_t state
       * held for it. Also closes whatever the file left open. */
      ray_poll_destroy(poll);
      ray_runtime_set_poll(NULL);
    }
    ray_runtime_destroy(rt);
    files++;
  }

  printf("\n%s: %d file(s), %d failure(s)\n", failures ? "FAILED" : "PASSED",
         files, failures);
  return failures;
}
