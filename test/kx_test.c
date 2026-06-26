/*
 * kx_test.c — rayfall test runner for the kx KDB+ client.
 *
 * Embeds the rayforce engine, registers the kx builtins (see kx_builtins.c),
 * and runs one or more `.rfl` test files. The assertion DSL mirrors the
 * rayforce core's own .rfl harness:
 *
 *   EXPR -- VALUE     pass if format(EXPR) == format(VALUE)
 *   EXPR !- SUBSTR    pass if EXPR raises an error whose text contains SUBSTR
 *   EXPR              raw setup line; fails the file if it raises
 *   ;; ...            comment; blank lines ignored
 *
 * A fresh runtime is created per file so connection handles and `set`
 * bindings don't leak across files. Exit status is the number of failing
 * files (0 = all pass).
 */

#include <rayforce.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void kx_register_builtins(void);

/* ---- formatting helpers (mirrors core test/main.c) ---- */

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

/* Run one .rfl file. Returns 0 on pass, 1 on failure (message printed). */
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

/* Bind the server coordinates the .rfl files connect to as `kxhost`/`kxport`
 * in the current runtime, so tests stay free of hard-coded host/port. */
static void inject_server(const char *host, const char *port) {
  char buf[256];
  snprintf(buf, sizeof buf, "(set kxhost \"%s\")", host);
  ray_t *r = ray_eval_str(buf);
  if (r && !RAY_IS_ERR(r))
    ray_release(r);
  snprintf(buf, sizeof buf, "(set kxport %s)", port);
  r = ray_eval_str(buf);
  if (r && !RAY_IS_ERR(r))
    ray_release(r);
}

int main(int argc, char **argv) {
  const char *host = "127.0.0.1";
  const char *port = "0";
  int first = 1;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
      host = argv[++i];
      first = i + 1;
    } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
      port = argv[++i];
      first = i + 1;
    } else {
      first = i;
      break;
    }
  }
  if (first >= argc) {
    fprintf(stderr,
            "usage: %s [--host H] [--port P] file.rfl [file.rfl ...]\n",
            argv[0]);
    return 2;
  }

  int failures = 0, files = 0;
  for (int i = first; i < argc; i++) {
    /* Fresh runtime per file: isolates handles and `set` bindings. */
    ray_runtime_t *rt = ray_runtime_create(0, NULL);
    kx_register_builtins();
    inject_server(host, port);
    failures += run_rfl_file(argv[i]);
    ray_runtime_destroy(rt);
    files++;
  }

  printf("\n%s: %d file(s), %d failure(s)\n", failures ? "FAILED" : "PASSED",
         files, failures);
  return failures;
}
