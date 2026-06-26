#ifndef RAYFORCE_KX_H
#define RAYFORCE_KX_H
/*
 * rayforce-kx — KDB+ IPC wire-format client for RayforceDB.
 *
 * Language-neutral C core shared across RayforceDB language bindings
 * (Python, Rust, ...). It speaks the documented KDB+ wire format over
 * blocking sockets and converts between the wire and rayforce core
 * `ray_t` objects. There is no binding-specific (e.g. CPython) code here;
 * each binding wraps these three entry points in its own glue.
 *
 * Supported KDB+ wire types
 *   atoms:   -KB -KG -KH -KI -KJ -KE -KF -KC -KS -KP -KM -KD -KN -KU -KV
 *            -KT -UU
 *   vectors: KB UU KG KH KI KJ KE KF KC KS KP KM KD KZ KN KU KV KT
 *   nested:  0 (general list), XT (table), XD (dict)
 *   error:   -128
 */

#include <rayforce.h>
#include <stddef.h>

/* kx_connect failure codes, returned in place of a connection slot. */
#define KX_ERR_SOCKET (-1)    /* could not open/connect the TCP socket    */
#define KX_ERR_HANDSHAKE (-2) /* socket connected but handshake failed    */
#define KX_ERR_FULL (-3)      /* the connection table has no free slots   */

/* Open a TCP connection to a KDB+ server and perform the handshake.
 * Returns a non-negative connection slot, or one of the KX_ERR_* codes. */
int kx_connect(const char *host, int port);

/* Close a connection slot. Returns 0 on success, -1 on an invalid handle. */
int kx_close(int slot);

/* Send `msg` to the server on `slot`, wait for the response, and decode it
 * into a freshly-owned rayforce object. The returned object may itself be a
 * RAY_ERROR carrying a KDB+ error string (a server-side error).
 *
 * On a transport- or serialization-level failure returns NULL and, when
 * `err`/`errlen` are non-NULL, writes a short human-readable reason there. */
ray_t *kx_send(int slot, ray_t *msg, char *err, size_t errlen);

#endif /* RAYFORCE_KX_H */
