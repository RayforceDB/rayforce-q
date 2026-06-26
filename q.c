#define _GNU_SOURCE
/*
 * q.c — Q IPC wire-format client core (see q.h).
 *
 * Language-neutral: depends only on the rayforce core public API (`ray_t`
 * and friends), never on a specific binding runtime. Blocking sockets, no
 * event loop.
 */

#include "q.h"

#include "table/sym.h" /* RAY_SYM_W64 */

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

/* Two helpers the rayforce core public header does not (yet) export. Defined
 * here so q stays self-contained and binding-independent. */

/* Dict attr bit on ray_t->attrs (v2: a dict is RAY_LIST + this bit).
 * Value must match src/mem/heap.h. */
#ifndef RAY_ATTR_DICT
#define RAY_ATTR_DICT 0x02
#endif

/* Byte width of a fixed-size scalar/vec element, for both atom (negative) and
 * vec (positive) type codes. Returns 0 for variable-width/unknown types
 * (RAY_LIST, RAY_SYM, RAY_STR, RAY_TABLE, RAY_DICT, ...). */
static inline size_t ray_scalar_elem_size(int8_t type) {
  switch (type < 0 ? -type : type) {
  case RAY_BOOL:
  case RAY_U8:
    return 1;
  case RAY_I16:
    return 2;
  case RAY_I32:
  case RAY_DATE:
  case RAY_TIME:
  case RAY_F32:
    return 4;
  case RAY_I64:
  case RAY_F64:
  case RAY_TIMESTAMP:
    return 8;
  case RAY_GUID:
    return 16;
  default:
    return 0;
  }
}

/* ================================================================
 * Q wire-format constants
 * ================================================================ */
#define Q_KB 1  /* boolean      */
#define Q_UU 2  /* guid (16B)   */
#define Q_KG 4  /* byte         */
#define Q_KH 5  /* short        */
#define Q_KI 6  /* int          */
#define Q_KJ 7  /* long         */
#define Q_KE 8  /* real (f32)   */
#define Q_KF 9  /* float (f64)  */
#define Q_KC 10 /* char         */
#define Q_KS 11 /* symbol       */
#define Q_KP 12 /* timestamp    */
#define Q_KM 13 /* month        */
#define Q_KD 14 /* date         */
#define Q_KZ 15 /* datetime     */
#define Q_KN 16 /* timespan     */
#define Q_KU 17 /* minute       */
#define Q_KV 18 /* second       */
#define Q_KT 19 /* time         */
#define Q_XT 98 /* table        */
#define Q_XD 99 /* dict         */
#define Q_ERR (-128)

#define Q_MSG_SYNC 1

typedef struct {
  uint8_t endianness;
  uint8_t msgtype;
  uint8_t compressed;
  uint8_t reserved;
  uint32_t size;
} q_header_t;

#define Q_LITTLE_ENDIAN 1

static void q_set_err(char *err, size_t errlen, const char *msg) {
  if (err != NULL && errlen > 0)
    snprintf(err, errlen, "%s", msg);
}

/* Build a v2 table from a RAY_SYM-vec of column ids and an array of column
 * vectors. Retains each column internally; the caller keeps ownership of the
 * inputs. (Local copy so q has no binding-specific dependencies.) */
static ray_t *q_build_table(const int64_t *col_ids, ray_t *const *cols,
                            int64_t ncols) {
  ray_t *tbl = ray_table_new(ncols);
  if (tbl == NULL)
    return ray_error("table_new failed", NULL);
  if (RAY_IS_ERR(tbl))
    return tbl;
  for (int64_t i = 0; i < ncols; i++) {
    if (cols[i] == NULL) {
      ray_release(tbl);
      return ray_error("table column is null", NULL);
    }
    /* ray_table_add_col retains the column internally and may return a new
     * pointer on realloc. */
    ray_t *res = ray_table_add_col(tbl, col_ids[i], cols[i]);
    if (res == NULL || RAY_IS_ERR(res)) {
      ray_release(tbl);
      return res ? res : ray_error("table_add_col failed", NULL);
    }
    tbl = res;
  }
  return tbl;
}

/* ================================================================
 * Socket helpers
 * ================================================================ */

static ssize_t q_recv_all(int fd, void *buf, size_t n) {
  size_t total = 0;
  uint8_t *p = (uint8_t *)buf;
  while (total < n) {
    ssize_t r = recv(fd, p + total, n - total, 0);
    if (r == 0)
      return -1;
    if (r < 0) {
      if (errno == EINTR)
        continue;
      return -1;
    }
    total += (size_t)r;
  }
  return (ssize_t)total;
}

static ssize_t q_send_all(int fd, const void *buf, size_t n) {
  size_t total = 0;
  const uint8_t *p = (const uint8_t *)buf;
  while (total < n) {
    ssize_t r = send(fd, p + total, n - total, 0);
    if (r <= 0) {
      if (r < 0 && errno == EINTR)
        continue;
      return -1;
    }
    total += (size_t)r;
  }
  return (ssize_t)total;
}

/* Connect one address with an optional timeout (ms). 0/negative blocks.
 * Returns 0 on success, -1 on plain failure, -2 on timeout. */
static int q_connect_one(const struct addrinfo *p, int timeout_ms, int fd) {
  if (timeout_ms <= 0)
    return connect(fd, p->ai_addr, p->ai_addrlen) == 0 ? 0 : -1;

  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
    return -1;

  int rc = connect(fd, p->ai_addr, p->ai_addrlen);
  if (rc == 0) {
    fcntl(fd, F_SETFL, flags);
    return 0;
  }
  if (errno != EINPROGRESS)
    return -1;

  struct pollfd pfd = {.fd = fd, .events = POLLOUT};
  int pr;
  do {
    pr = poll(&pfd, 1, timeout_ms);
  } while (pr < 0 && errno == EINTR);
  if (pr == 0)
    return -2; /* timeout */
  if (pr < 0)
    return -1;

  int soerr = 0;
  socklen_t slen = sizeof soerr;
  if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &slen) < 0 || soerr != 0)
    return -1;

  fcntl(fd, F_SETFL, flags); /* back to blocking */
  return 0;
}

/* Open a TCP connection. *timed_out is set when the failure was a timeout. */
static int q_open_socket(const char *host, int port, int timeout_ms,
                         int *timed_out) {
  *timed_out = 0;
  char service[16];
  snprintf(service, sizeof(service), "%d", port);

  struct addrinfo hints = {0};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo *res = NULL;
  if (getaddrinfo(host, service, &hints, &res) != 0 || res == NULL)
    return -1;

  int fd = -1;
  int any_timeout = 0;
  for (struct addrinfo *p = res; p != NULL; p = p->ai_next) {
    fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
    if (fd < 0)
      continue;
    int rc = q_connect_one(p, timeout_ms, fd);
    if (rc == 0)
      break;
    if (rc == -2)
      any_timeout = 1;
    close(fd);
    fd = -1;
  }
  freeaddrinfo(res);
  if (fd < 0)
    *timed_out = any_timeout;
  return fd;
}

/* Apply a send/recv timeout (ms) to a connected socket. */
static void q_set_timeout(int fd, int timeout_ms) {
  if (timeout_ms <= 0)
    return;
  struct timeval tv = {.tv_sec = timeout_ms / 1000,
                       .tv_usec = (timeout_ms % 1000) * 1000};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
}

/* ================================================================
 * Wire-format size + serialize (rayforce v2 → Q)
 * ================================================================ */

static int8_t q_type_of(int8_t ray_type) {
  int sign = (ray_type < 0) ? -1 : 1;
  int t = (ray_type < 0) ? -ray_type : ray_type;
  switch (t) {
  case RAY_BOOL:
    return (int8_t)(sign * Q_KB);
  case RAY_U8:
    return (int8_t)(sign * Q_KG);
  case RAY_I16:
    return (int8_t)(sign * Q_KH);
  case RAY_I32:
    return (int8_t)(sign * Q_KI);
  case RAY_I64:
    return (int8_t)(sign * Q_KJ);
  case RAY_F32:
    return (int8_t)(sign * Q_KE);
  case RAY_F64:
    return (int8_t)(sign * Q_KF);
  case RAY_DATE:
    return (int8_t)(sign * Q_KD);
  case RAY_TIME:
    return (int8_t)(sign * Q_KT);
  case RAY_TIMESTAMP:
    return (int8_t)(sign * Q_KP);
  case RAY_GUID:
    return (int8_t)(sign * Q_UU);
  case RAY_SYM:
    return (int8_t)(sign * Q_KS);
  case RAY_STR:
    return (int8_t)(sign * Q_KC);
  case RAY_LIST:
    return 0;
  case RAY_TABLE:
    return Q_XT;
  case RAY_DICT:
    return Q_XD;
  case RAY_ERROR:
    return Q_ERR;
  default:
    return 0;
  }
}

/* Forward */
static int64_t q_size_obj(ray_t *obj);
static int64_t q_ser_obj(uint8_t *buf, ray_t *obj);
static ray_t *q_des_obj(uint8_t **buf, int64_t *len);

/* Build a v2 table from a RAY_SYM-vec of column names and a RAY_LIST of
 * column vectors. Consumes references to `keys` and `vals`. */
static ray_t *q_make_table(ray_t *keys, ray_t *vals) {
  if (keys == NULL || vals == NULL || keys->type != RAY_SYM ||
      vals->type != RAY_LIST || keys->len != vals->len) {
    if (keys)
      ray_release(keys);
    if (vals)
      ray_release(vals);
    return ray_error("q: malformed table — expected (sym-vec, list)", NULL);
  }
  ray_t *tbl = q_build_table((const int64_t *)ray_data(keys),
                             (ray_t *const *)ray_data(vals), keys->len);
  ray_release(keys);
  ray_release(vals);
  return tbl;
}

static int64_t q_size_obj(ray_t *obj) {
  if (obj == NULL || obj == RAY_NULL_OBJ)
    return 1 + 1 + 4; /* type + attrs + len(0) */

  int8_t t = obj->type;

  /* Atoms */
  if (t < 0) {
    int abs_t = -t;
    switch (abs_t) {
    case RAY_BOOL:
    case RAY_U8:
      return 1 + 1;
    case RAY_I16:
      return 1 + 2;
    case RAY_I32:
    case RAY_DATE:
    case RAY_TIME:
    case RAY_F32:
      return 1 + 4;
    case RAY_I64:
    case RAY_TIMESTAMP:
    case RAY_F64:
      return 1 + 8;
    case RAY_GUID:
      return 1 + 16;
    case RAY_SYM: {
      ray_t *s = ray_sym_str(obj->i64);
      int64_t n = s ? (int64_t)ray_str_len(s) : 0;
      if (s)
        ray_release(s);
      return 1 + n + 1; /* null-terminated */
    }
    case RAY_STR: {
      /* Single char goes as -KC atom; multi-char as KC vector. */
      int64_t n = (int64_t)ray_str_len(obj);
      if (n == 1)
        return 1 + 1;
      return 1 + 1 + 4 + n;
    }
    }
    return 0;
  }

  /* Vectors / containers */
  if (t == RAY_LIST) {
    int64_t size = 1 + 1 + 4;
    ray_t **elems = (ray_t **)ray_data(obj);
    for (int64_t i = 0; i < obj->len; i++)
      size += q_size_obj(elems[i]);
    return size;
  }
  if (t == RAY_SYM) {
    int64_t size = 1 + 1 + 4;
    int64_t *ids = (int64_t *)ray_data(obj);
    for (int64_t i = 0; i < obj->len; i++) {
      ray_t *s = ray_sym_str(ids[i]);
      size += s ? (int64_t)ray_str_len(s) + 1 : 1;
      if (s)
        ray_release(s);
    }
    return size;
  }
  if (t == RAY_TABLE) {
    /* Wire form: XT byte + attrs(0) + XD marker + KS-vec of names +
     * list-0 of column vectors. v2 RAY_TABLE is opaque — extract via the
     * public accessors instead of indexing ray_data directly. */
    int64_t ncols = ray_table_ncols(obj);
    int64_t names = 1 + 1 + 4; /* KS type + attrs + count */
    for (int64_t i = 0; i < ncols; i++) {
      ray_t *s = ray_sym_str(ray_table_col_name(obj, i));
      names += s ? (int64_t)ray_str_len(s) + 1 : 1;
      if (s)
        ray_release(s);
    }
    int64_t cols = 1 + 1 + 4; /* list type + attrs + count */
    for (int64_t i = 0; i < ncols; i++)
      cols += q_size_obj(ray_table_get_col_idx(obj, i));
    return 3 + names + cols; /* XT + attrs + XD */
  }
  if (t == RAY_ERROR) {
    const char *msg = ray_err_code(obj);
    int64_t n = msg ? (int64_t)strlen(msg) : 0;
    return 1 + n + 1; /* type byte + msg + null */
  }
  if (t == RAY_NULL)
    return 1 + 1 + 4;

  int esz = (int)ray_scalar_elem_size(t);
  if (esz == 0)
    return 0;
  return 1 + 1 + 4 + obj->len * esz;
}

static int64_t q_ser_obj(uint8_t *buf, ray_t *obj) {
  uint8_t *start = buf;

  if (obj == NULL || obj == RAY_NULL_OBJ) {
    *buf++ = 101; /* identity / null */
    *buf++ = 0;
    return buf - start;
  }

  int8_t t = obj->type;
  *buf++ = (uint8_t)q_type_of(t);

  if (t < 0) {
    int abs_t = -t;
    switch (abs_t) {
    case RAY_BOOL:
    case RAY_U8:
      *buf++ = obj->u8;
      return buf - start;
    case RAY_I16:
      memcpy(buf, &obj->i16, 2);
      return buf + 2 - start;
    case RAY_I32:
    case RAY_DATE:
    case RAY_TIME:
      memcpy(buf, &obj->i32, 4);
      return buf + 4 - start;
    case RAY_F32: {
      float f = (float)obj->f64;
      memcpy(buf, &f, 4);
      return buf + 4 - start;
    }
    case RAY_I64:
    case RAY_TIMESTAMP:
      memcpy(buf, &obj->i64, 8);
      return buf + 8 - start;
    case RAY_F64:
      memcpy(buf, &obj->f64, 8);
      return buf + 8 - start;
    case RAY_GUID:
      memcpy(buf, ray_data(obj), 16);
      return buf + 16 - start;
    case RAY_SYM: {
      ray_t *s = ray_sym_str(obj->i64);
      size_t n = s ? ray_str_len(s) : 0;
      if (s) {
        memcpy(buf, ray_str_ptr(s), n);
        ray_release(s);
      }
      buf[n] = 0;
      return buf + n + 1 - start;
    }
    case RAY_STR: {
      size_t n = ray_str_len(obj);
      if (n == 1) {
        *buf++ = (uint8_t)ray_str_ptr(obj)[0];
        return buf - start;
      }
      /* RAY_STR atom (n>1) → KC vector */
      start[0] = (uint8_t)Q_KC;
      *buf++ = 0; /* attrs */
      uint32_t len32 = (uint32_t)n;
      memcpy(buf, &len32, 4);
      buf += 4;
      memcpy(buf, ray_str_ptr(obj), n);
      return buf + n - start;
    }
    }
    return -1;
  }

  /* Containers/vectors */
  if (t == RAY_LIST) {
    *buf++ = 0;
    uint32_t len32 = (uint32_t)obj->len;
    memcpy(buf, &len32, 4);
    buf += 4;
    ray_t **elems = (ray_t **)ray_data(obj);
    for (int64_t i = 0; i < obj->len; i++) {
      int64_t r = q_ser_obj(buf, elems[i]);
      if (r < 0)
        return -1;
      buf += r;
    }
    return buf - start;
  }
  if (t == RAY_SYM) {
    *buf++ = 0;
    uint32_t len32 = (uint32_t)obj->len;
    memcpy(buf, &len32, 4);
    buf += 4;
    int64_t *ids = (int64_t *)ray_data(obj);
    for (int64_t i = 0; i < obj->len; i++) {
      ray_t *s = ray_sym_str(ids[i]);
      size_t n = s ? ray_str_len(s) : 0;
      if (s) {
        memcpy(buf, ray_str_ptr(s), n);
        ray_release(s);
      }
      buf[n] = 0;
      buf += n + 1;
    }
    return buf - start;
  }
  if (t == RAY_TABLE) {
    int64_t ncols = ray_table_ncols(obj);
    *buf++ = 0;             /* attrs */
    *buf++ = (uint8_t)Q_XD; /* table is dict-of-cols on the wire */

    /* keys: KS vector of column names */
    *buf++ = (uint8_t)Q_KS;
    *buf++ = 0; /* attrs */
    uint32_t kn = (uint32_t)ncols;
    memcpy(buf, &kn, 4);
    buf += 4;
    for (int64_t i = 0; i < ncols; i++) {
      ray_t *s = ray_sym_str(ray_table_col_name(obj, i));
      size_t n = s ? ray_str_len(s) : 0;
      if (s) {
        memcpy(buf, ray_str_ptr(s), n);
        ray_release(s);
      }
      buf[n] = 0;
      buf += n + 1;
    }

    /* values: general list of column vectors */
    *buf++ = 0; /* list type */
    *buf++ = 0; /* attrs */
    memcpy(buf, &kn, 4);
    buf += 4;
    for (int64_t i = 0; i < ncols; i++) {
      int64_t r = q_ser_obj(buf, ray_table_get_col_idx(obj, i));
      if (r < 0)
        return -1;
      buf += r;
    }
    return buf - start;
  }
  /* RAY_DICT (type code 99) is never produced by v2; dicts present as
   * RAY_LIST + RAY_ATTR_DICT and are serialized through the RAY_LIST path
   * above (losing the dict-ness on the wire). */
  if (t == RAY_ERROR) {
    const char *msg = ray_err_code(obj);
    size_t n = msg ? strlen(msg) : 0;
    if (n)
      memcpy(buf, msg, n);
    buf[n] = 0;
    return buf + n + 1 - start;
  }
  if (t == RAY_NULL) {
    *buf++ = 0;
    memset(buf, 0, 4);
    return buf + 4 - start;
  }

  int esz = (int)ray_scalar_elem_size(t);
  if (esz == 0)
    return -1;
  *buf++ = 0;
  uint32_t len32 = (uint32_t)obj->len;
  memcpy(buf, &len32, 4);
  buf += 4;
  size_t n = (size_t)obj->len * esz;
  memcpy(buf, ray_data(obj), n);
  return buf + n - start;
}

/* ================================================================
 * Wire-format deserialize (Q → rayforce v2)
 * ================================================================ */

#define Q_NEED(n)                                                              \
  do {                                                                         \
    if (*len < (int64_t)(n))                                                   \
      return ray_error("q: buffer underflow", NULL);                           \
  } while (0)

/* Decode a width-byte atom and re-tag as the requested ray_type. Works
 * because v2's atom union shares storage by width: BOOL/U8 → u8;
 * I16 → i16; I32/DATE/TIME → i32; I64/TIMESTAMP → i64. */
static ray_t *q_des_atom_i(uint8_t **buf, int64_t *len, int8_t ray_type,
                           int width) {
  Q_NEED(width);
  ray_t *o = NULL;
  switch (width) {
  case 1:
    o = ray_u8(**buf);
    break;
  case 2: {
    int16_t v;
    memcpy(&v, *buf, 2);
    o = ray_i16(v);
    break;
  }
  case 4: {
    int32_t v;
    memcpy(&v, *buf, 4);
    o = ray_i32(v);
    break;
  }
  case 8: {
    int64_t v;
    memcpy(&v, *buf, 8);
    o = ray_i64(v);
    break;
  }
  }
  if (o)
    o->type = -ray_type; /* tag with the requested ray atom type */
  *buf += width;
  *len -= width;
  return o;
}

/* Read a q vector header: 1 byte attrs + 4 bytes int32 length.
 * Advances *buf by 5 and decrements *len. Returns -1 on buffer underflow. */
static int q_read_vec_header(uint8_t **buf, int64_t *len, int32_t *out_n) {
  if (*len < 5)
    return -1;
  (*buf)++;
  *len -= 1; /* attrs */
  memcpy(out_n, *buf, 4);
  *buf += 4;
  *len -= 4;
  return 0;
}

static ray_t *q_des_vec_i(uint8_t **buf, int64_t *len, int8_t ray_type,
                          int width) {
  int32_t n;
  if (q_read_vec_header(buf, len, &n) < 0)
    return ray_error("q: buffer underflow", NULL);
  if (n < 0)
    return ray_error("q: negative vector length", NULL);
  int64_t bytes = (int64_t)n * width;
  if (*len < bytes)
    return ray_error("q: buffer underflow (vec body)", NULL);

  ray_t *vec = ray_vec_new(ray_type, n);
  if (vec == NULL || RAY_IS_ERR(vec)) {
    if (vec)
      ray_release(vec);
    return ray_error("q: vector alloc failed", NULL);
  }
  memcpy(ray_data(vec), *buf, bytes);
  vec->len = n;
  *buf += bytes;
  *len -= bytes;
  return vec;
}

static ray_t *q_des_obj(uint8_t **buf, int64_t *len) {
  if (*len < 1)
    return ray_error("q: buffer underflow (type)", NULL);

  int8_t type = (int8_t)**buf;
  (*buf)++;
  (*len)--;

  switch (type) {
  /* Atoms (negative type). The deserializer reads raw bytes then re-tags. */
  case -Q_KB:
    return q_des_atom_i(buf, len, RAY_BOOL, 1);
  case -Q_KG:
    return q_des_atom_i(buf, len, RAY_U8, 1);
  case -Q_KH:
    return q_des_atom_i(buf, len, RAY_I16, 2);
  case -Q_KI:
    return q_des_atom_i(buf, len, RAY_I32, 4);
  case -Q_KJ:
    return q_des_atom_i(buf, len, RAY_I64, 8);
  case -Q_KP:
  case -Q_KN:
    return q_des_atom_i(buf, len, RAY_TIMESTAMP, 8);
  case -Q_KD:
  case -Q_KM:
    return q_des_atom_i(buf, len, RAY_DATE, 4);
  case -Q_KT:
  case -Q_KU:
  case -Q_KV:
    return q_des_atom_i(buf, len, RAY_TIME, 4);
  case -Q_KE: {
    Q_NEED(4);
    float f;
    memcpy(&f, *buf, 4);
    *buf += 4;
    *len -= 4;
    return ray_f64((double)f);
  }
  case -Q_KF: {
    Q_NEED(8);
    double f;
    memcpy(&f, *buf, 8);
    *buf += 8;
    *len -= 8;
    return ray_f64(f);
  }
  case -Q_KC: {
    Q_NEED(1);
    char c = (char)**buf;
    *buf += 1;
    *len -= 1;
    return ray_str(&c, 1);
  }
  case -Q_KS: {
    int64_t n = 0;
    while (n < *len && (*buf)[n] != '\0')
      n++;
    if (n >= *len)
      return ray_error("q: symbol not null-terminated", NULL);
    int64_t id = ray_sym_intern((const char *)*buf, (size_t)n);
    *buf += n + 1;
    *len -= n + 1;
    return (id < 0) ? ray_error("q: symbol intern failed", NULL) : ray_sym(id);
  }
  case -Q_UU: {
    Q_NEED(16);
    ray_t *g = ray_guid(*buf);
    *buf += 16;
    *len -= 16;
    return g;
  }

  /* Vectors */
  case Q_KB:
    return q_des_vec_i(buf, len, RAY_BOOL, 1);
  case Q_KG:
    return q_des_vec_i(buf, len, RAY_U8, 1);
  case Q_KH:
    return q_des_vec_i(buf, len, RAY_I16, 2);
  case Q_KI:
    return q_des_vec_i(buf, len, RAY_I32, 4);
  case Q_KJ:
    return q_des_vec_i(buf, len, RAY_I64, 8);
  case Q_KP:
  case Q_KN:
    return q_des_vec_i(buf, len, RAY_TIMESTAMP, 8);
  case Q_KD:
  case Q_KM:
    return q_des_vec_i(buf, len, RAY_DATE, 4);
  case Q_KT:
  case Q_KU:
  case Q_KV:
    return q_des_vec_i(buf, len, RAY_TIME, 4);
  case Q_KZ:
    return q_des_vec_i(buf, len, RAY_TIMESTAMP, 8);
  case Q_KE: {
    /* Real (4-byte float) vector — convert to F64 vec for v2. */
    int32_t n;
    if (q_read_vec_header(buf, len, &n) < 0)
      return ray_error("q: buffer underflow", NULL);
    if (n < 0)
      return ray_error("q: negative real-vec length", NULL);
    int64_t bytes = (int64_t)n * 4;
    if (*len < bytes)
      return ray_error("q: buffer underflow (real-vec)", NULL);
    ray_t *vec = ray_vec_new(RAY_F64, n);
    if (vec == NULL || RAY_IS_ERR(vec)) {
      if (vec)
        ray_release(vec);
      return ray_error("q: vector alloc failed", NULL);
    }
    double *out = (double *)ray_data(vec);
    for (int32_t i = 0; i < n; i++) {
      float f;
      memcpy(&f, *buf + i * 4, 4);
      out[i] = (double)f;
    }
    vec->len = n;
    *buf += bytes;
    *len -= bytes;
    return vec;
  }
  case Q_KF:
    return q_des_vec_i(buf, len, RAY_F64, 8);
  case Q_UU:
    return q_des_vec_i(buf, len, RAY_GUID, 16);
  case Q_KC: {
    /* KC vector → RAY_STR atom of length n */
    int32_t n;
    if (q_read_vec_header(buf, len, &n) < 0)
      return ray_error("q: buffer underflow", NULL);
    if (n < 0)
      return ray_error("q: negative char-vec length", NULL);
    if (*len < n)
      return ray_error("q: buffer underflow (char-vec)", NULL);
    ray_t *s = ray_str((const char *)*buf, (size_t)n);
    *buf += n;
    *len -= n;
    return s;
  }
  case Q_KS: {
    int32_t n;
    if (q_read_vec_header(buf, len, &n) < 0)
      return ray_error("q: buffer underflow", NULL);
    if (n < 0)
      return ray_error("q: negative symbol-vec length", NULL);
    ray_t *vec = ray_sym_vec_new(RAY_SYM_W64, n);
    if (vec == NULL || RAY_IS_ERR(vec)) {
      if (vec)
        ray_release(vec);
      return ray_error("q: symbol vector alloc failed", NULL);
    }
    int64_t *ids = (int64_t *)ray_data(vec);
    for (int32_t i = 0; i < n; i++) {
      int64_t k = 0;
      while (k < *len && (*buf)[k] != '\0')
        k++;
      if (k >= *len) {
        ray_release(vec);
        return ray_error("q: symbol not null-terminated in vec", NULL);
      }
      int64_t id = ray_sym_intern((const char *)*buf, (size_t)k);
      if (id < 0) {
        ray_release(vec);
        return ray_error("q: symbol intern failed", NULL);
      }
      ids[i] = id;
      *buf += k + 1;
      *len -= k + 1;
    }
    vec->len = n;
    return vec;
  }

  case 0: { /* general list */
    int32_t n;
    if (q_read_vec_header(buf, len, &n) < 0)
      return ray_error("q: buffer underflow", NULL);
    if (n < 0)
      return ray_error("q: negative list length", NULL);
    ray_t *list = ray_list_new(0);
    for (int32_t i = 0; i < n; i++) {
      ray_t *elem = q_des_obj(buf, len);
      if (elem == NULL || RAY_IS_ERR(elem)) {
        ray_release(list);
        return elem ? elem : ray_error("q: list element decode failed", NULL);
      }
      list = ray_list_append(list, elem);
      ray_release(elem); /* append retains its own ref; drop ours */
      if (list == NULL || RAY_IS_ERR(list))
        return list ? list : ray_error("q: list append failed", NULL);
    }
    return list;
  }

  case Q_XT: { /* table = attrs(0) + dict_marker(99) + keys + values */
    Q_NEED(2);
    (*buf) += 2;
    *len -= 2;
    ray_t *keys = q_des_obj(buf, len);
    if (keys == NULL || RAY_IS_ERR(keys))
      return keys;
    ray_t *vals = q_des_obj(buf, len);
    if (vals == NULL || RAY_IS_ERR(vals)) {
      ray_release(keys);
      return vals;
    }
    return q_make_table(keys, vals);
  }

  case Q_XD: { /* dict = keys + values; could be a keyed table */
    ray_t *keys = q_des_obj(buf, len);
    if (keys == NULL || RAY_IS_ERR(keys))
      return keys;
    ray_t *vals = q_des_obj(buf, len);
    if (vals == NULL || RAY_IS_ERR(vals)) {
      ray_release(keys);
      return vals;
    }
    /* A plain dict (e.g. `a`b!1 2) becomes a native rayforce dict. A keyed
     * table arrives as a dict whose key and value are both tables; rayforce
     * has no keyed-table type here, so keep that as a 2-element
     * RAY_LIST + ATTR_DICT (key-table, value-table). */
    if (keys->type == RAY_TABLE && vals->type == RAY_TABLE) {
      ray_t *kt = ray_list_new(0);
      kt = ray_list_append(kt, keys);
      ray_release(keys); /* append retains; drop our ref */
      kt = ray_list_append(kt, vals);
      ray_release(vals);
      if (kt && !RAY_IS_ERR(kt))
        kt->attrs |= RAY_ATTR_DICT;
      return kt;
    }
    return ray_dict_new(keys, vals); /* consumes both refs */
  }

  case Q_ERR: {
    /* Error string is NUL-terminated on the wire; ensure the terminator is
     * within the remaining buffer before handing it to ray_error (#H4 OOB). */
    const char *s = (const char *)*buf;
    int64_t i = 0;
    while (i < *len && s[i] != '\0')
      i++;
    if (i == *len)
      return ray_error("q: malformed error frame", NULL);
    /* Put the q error text in both the code (short, shown by ray_fmt) and the
     * message (full), so bindings reading the message field get the whole
     * string even though the displayed code is length-capped. */
    return ray_error(s, "%s", s);
  }

  default:
    return ray_error("q: unsupported wire type", NULL);
  }
}

/* ================================================================
 * IPC decompression for compressed Q responses.
 * ================================================================ */

static int q_decompress(const uint8_t *src, int64_t src_len, uint8_t **out_buf,
                        int64_t *out_len) {
  if (src_len < 4)
    return -1;

  uint32_t header_size;
  memcpy(&header_size, src, 4);
  int64_t out_size = (int64_t)header_size - (int64_t)sizeof(q_header_t);
  if (out_size <= 0)
    return -1;

  uint32_t buffer[256] = {0};
  uint8_t *result = (uint8_t *)malloc((size_t)out_size);
  if (result == NULL)
    return -1;

  int64_t i = 0, n = 0, f = 0, s = 0, p = 0, d = 4;
  while (s < out_size) {
    if (i == 0) {
      if (d >= src_len) {
        free(result);
        return -1;
      }
      f = src[d++];
      i = 1;
    }
    if (f & i) {
      if (d >= src_len) {
        free(result);
        return -1;
      }
      int64_t r = buffer[src[d++]];
      result[s++] = result[r++];
      result[s++] = result[r++];
      if (d >= src_len) {
        free(result);
        return -1;
      }
      n = src[d++];
      for (int64_t m = 0; m < n; m++)
        result[s + m] = result[r + m];
    } else {
      if (d >= src_len) {
        free(result);
        return -1;
      }
      result[s++] = src[d++];
    }
    while (p < s - 1) {
      int64_t pp = p++;
      buffer[result[pp] ^ result[p]] = (uint32_t)pp;
    }
    if (f & i) {
      s += n;
      p = s;
    }
    i *= 2;
    if (i == 256)
      i = 0;
  }
  *out_buf = result;
  *out_len = out_size;
  return 0;
}

/* ================================================================
 * Public entry points (see q.h)
 * ================================================================ */

int q_connect(const char *host, int port, const char *user,
              const char *password, int timeout_ms) {
  int timed_out = 0;
  int fd = q_open_socket(host, port, timeout_ms, &timed_out);
  if (fd < 0)
    return timed_out ? Q_ERR_TIMEOUT : Q_ERR_SOCKET;

  q_set_timeout(fd, timeout_ms);

  /* Q login: send "user:password" + capability byte (0x03) + 0x00, then
   * read one capability byte back. A server that rejects the credentials
   * closes the socket, so the read fails -> Q_ERR_HANDSHAKE. Empty
   * credentials degrade to the original no-auth handshake ({0x03, 0x00}). */
  size_t ulen = user ? strlen(user) : 0;
  size_t plen = password ? strlen(password) : 0;
  size_t creds = ulen + (plen ? 1 + plen : 0);
  uint8_t stackbuf[256];
  uint8_t *login = stackbuf;
  if (creds + 2 > sizeof stackbuf) {
    login = (uint8_t *)malloc(creds + 2);
    if (login == NULL) {
      close(fd);
      return Q_ERR_HANDSHAKE;
    }
  }
  size_t off = 0;
  if (ulen) {
    memcpy(login + off, user, ulen);
    off += ulen;
    if (plen) {
      login[off++] = ':';
      memcpy(login + off, password, plen);
      off += plen;
    }
  }
  login[off++] = 0x03; /* capability: compression + timestamp + GUID + ... */
  login[off++] = 0x00;

  int sent_ok = q_send_all(fd, login, off) >= 0;
  if (login != stackbuf)
    free(login);

  uint8_t cap;
  if (!sent_ok || q_recv_all(fd, &cap, 1) < 0) {
    close(fd);
    return Q_ERR_HANDSHAKE;
  }
  return fd;
}

int q_close(int fd) {
  if (fd < 0)
    return -1;
  return close(fd) == 0 ? 0 : -1;
}

int q_encode(ray_t *msg, uint8_t **req, int64_t *req_len, char *err,
             size_t errlen) {
  int64_t body_size = q_size_obj(msg);
  if (body_size <= 0) {
    q_set_err(err, errlen, "q: cannot serialize message");
    return -1;
  }
  int64_t total = (int64_t)sizeof(q_header_t) + body_size;
  if (total > (int64_t)UINT32_MAX) {
    q_set_err(err, errlen, "q: message too large for the wire (>4GiB)");
    return -1;
  }
  uint8_t *buf = (uint8_t *)malloc((size_t)total);
  if (buf == NULL) {
    q_set_err(err, errlen, "q: out of memory");
    return -1;
  }
  int64_t written = q_ser_obj(buf + sizeof(q_header_t), msg);
  if (written < 0) {
    free(buf);
    q_set_err(err, errlen, "q: serialization failed");
    return -1;
  }
  q_header_t header = {
      .endianness = Q_LITTLE_ENDIAN,
      .msgtype = Q_MSG_SYNC,
      .compressed = 0,
      .reserved = 0,
      .size = (uint32_t)(written + (int64_t)sizeof(q_header_t)),
  };
  memcpy(buf, &header, sizeof header);
  *req = buf;
  *req_len = (int64_t)header.size;
  return 0;
}

int q_exchange(int fd, const uint8_t *req, int64_t req_len, uint8_t **resp,
               int64_t *resp_len, int *compressed, char *err, size_t errlen) {
  if (fd < 0) {
    q_set_err(err, errlen, "q: invalid handle");
    return -1;
  }
  if (q_send_all(fd, req, (size_t)req_len) < 0) {
    q_set_err(err, errlen,
              (errno == EAGAIN || errno == EWOULDBLOCK) ? "q: send timed out"
              : (errno == EBADF || errno == ENOTSOCK)   ? "q: invalid handle"
                                                        : "q: send failed");
    return -1;
  }

  q_header_t header;
  if (q_recv_all(fd, &header, sizeof header) < 0) {
    q_set_err(err, errlen,
              (errno == EAGAIN || errno == EWOULDBLOCK)
                  ? "q: recv timed out"
                  : "q: recv header failed");
    return -1;
  }
  if (header.endianness != Q_LITTLE_ENDIAN) {
    q_set_err(err, errlen, "q: big-endian peer not supported");
    return -1;
  }
  int64_t body_len = (int64_t)header.size - (int64_t)sizeof header;
  if (body_len <= 0) {
    q_set_err(err, errlen, "q: empty response body");
    return -1;
  }
  uint8_t *body = (uint8_t *)malloc((size_t)body_len);
  if (body == NULL) {
    q_set_err(err, errlen, "q: out of memory");
    return -1;
  }
  if (q_recv_all(fd, body, (size_t)body_len) < 0) {
    free(body);
    q_set_err(err, errlen,
              (errno == EAGAIN || errno == EWOULDBLOCK)
                  ? "q: recv timed out"
                  : "q: recv body failed");
    return -1;
  }
  *resp = body;
  *resp_len = body_len;
  *compressed = header.compressed;
  return 0;
}

ray_t *q_decode(uint8_t *resp, int64_t resp_len, int compressed, char *err,
                size_t errlen) {
  uint8_t *decoded = resp;
  int64_t decoded_len = resp_len;
  uint8_t *decompressed = NULL;
  if (compressed) {
    if (q_decompress(resp, resp_len, &decompressed, &decoded_len) < 0) {
      q_set_err(err, errlen, "q: decompression failed");
      return NULL;
    }
    decoded = decompressed;
  }
  uint8_t *cursor = decoded;
  int64_t remaining = decoded_len;
  ray_t *result = q_des_obj(&cursor, &remaining);
  if (decompressed)
    free(decompressed);
  if (result == NULL)
    q_set_err(err, errlen, "q: deserialization returned null");
  return result;
}

ray_t *q_send(int fd, ray_t *msg, char *err, size_t errlen) {
  uint8_t *req = NULL;
  int64_t req_len = 0;
  if (q_encode(msg, &req, &req_len, err, errlen) < 0)
    return NULL;

  uint8_t *resp = NULL;
  int64_t resp_len = 0;
  int compressed = 0;
  int rc =
      q_exchange(fd, req, req_len, &resp, &resp_len, &compressed, err, errlen);
  free(req);
  if (rc < 0)
    return NULL;

  ray_t *result = q_decode(resp, resp_len, compressed, err, errlen);
  free(resp);
  return result;
}
