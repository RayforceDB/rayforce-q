# rayforce-kx tests

Integration tests for the kx KDB+ client, written in the **rayfall** language
and run against a real `q` (kdb+) server, wired into GitHub CI. Bindings only
ever compile `kx.c` / `kx.h`, so this scaffolding is inert for them.

## How it works

The pinned rayforce core has no runtime `.so` loading, so to drive `kx_*` from
rayfall we compile it into a small rayforce-linked binary:

1. `make` clones (or copies) the native rayforce core and builds it as a static
   library (`librayforce.a`).
2. `kx.c` + [`kx_builtins.c`](kx_builtins.c) + [`kx_test.c`](kx_test.c) are
   compiled and linked against that library into a `kx_test` driver.
   `kx_builtins.c` registers `kx_connect`/`kx_send`/`kx_close` as the rayfall
   builtins `kxconnect` / `kxsend` / `kxclose` via the runtime `ray_env_set`
   API. (This is the "dynamic" registration path — the engine cannot `dlopen`
   a prebuilt object, so the functions are compiled in and bound at startup.)
3. [`run_tests.sh`](run_tests.sh) starts a throwaway `q` server, and the driver
   runs every [`rfl/*.rfl`](rfl) file against it.

## Running

```sh
cd test

# clone the core from GitHub, build, and run:
make test

# or build against a local rayforce checkout (no network):
RAYFORCE_LOCAL_PATH=/path/to/rayforce make test
```

`q` is autodetected (`$Q_BINARY`, `~/q/m64/q`, or `q` on `PATH`); if it is not
found the run **skips** (exit 0) rather than failing, so CI without kdb+ is fine.

## The `.rfl` assertion DSL

Mirrors the rayforce core's own test harness (`kx_test.c` implements it):

```
EXPR -- VALUE     pass if format(EXPR) == format(VALUE)
EXPR !- SUBSTR    pass if EXPR raises an error whose text contains SUBSTR
EXPR              raw setup line; fails the file if it raises
;; ...            comment
```

`kxhost` / `kxport` are bound by the driver so the tests don't hard-code the
server address. Each `.rfl` file runs under a fresh runtime, so handles and
`set` bindings don't leak between files.

## Coverage

| File                  | What it covers                                             |
| --------------------- | ---------------------------------------------------------- |
| `01_connection.rfl`   | connect / send / close lifecycle, closed-handle + dead-port errors |
| `02_atoms.rfl`        | long/int/short, float/real, bool, byte, char, symbol atoms |
| `03_vectors.rfl`      | int/float/bool/byte/symbol/char vectors, lengths, large (100k) response |
| `04_collections.rfl`  | general list, tables (whole-value), native dict            |
| `05_temporal.rfl`     | date / time / timestamp (epoch + width compatibility)      |
| `06_errors.rfl`       | server-side error decode (`-128`), post-error reuse        |
| `07_auth.rfl`         | login with credentials, wrong-password + no-credentials rejection |
| `08_nulls.rfl`        | typed null atoms / nulls in vectors, empty typed vectors   |

The auth tests need a second q started with `-u`; `run_tests.sh` handles both
servers and injects `kxhost`/`kxport`/`kxauthport`/`kxuser`/`kxpass`.
