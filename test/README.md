# rayforce-q tests

Integration tests for both halves of rayforce-q — the Q **client** and the **server** — written in the **rayfall** language. Bindings compile the reusable cores (`q.c`, and `q_server.c` if they host a server), so this scaffolding is inert for them

## How it works

The pinned rayforce core has no runtime `.so` loading, so to drive the Q verbs from rayfall we compile them into one small rayforce-linked binary, [`driver`](driver.c).

1. `make` clones the native rayforce core and builds it as a static library (`librayforce.a`).
2. `q.c` + `q_server.c` + [`../embed/rayforce_q.c`](../embed/rayforce_q.c) + [`driver.c`](driver.c) are compiled and linked against it into `test/driver`.
   The driver registers the **shipped** `.q.connect` / `.q.send` / `.q.close` verbs — it compiles in `embed/rayforce_q.c`.
3. [`run.sh`](run.sh) drives the suite in both directions (see below).

## The two roles of `driver`

```sh
driver --serve PORT                        # BE a Q server (q_serve)
driver [--host H --port P ...] file.rfl …  # be a CLIENT, running .rfl files
```

`--serve` stands in for `rayforce -q PORT` so the round-trip suite needs no full release build

## What `run.sh` runs

1. **Server leg** (always): start `driver --serve` and run [`rfl/server/*.rfl`](rfl/server) against it — Rayfall payloads the server evaluates with `ray_eval_str`
2. **real-q interop** (if `q` present): drive that same server from Q (`hopen`), proving we speak Q's wire format
3. **Client leg** (if `q` present): start throwaway `q` servers (one open, one authenticated) and run [`rfl/client/*.rfl`](rfl/client) — q-language payloads — against them

`q` is autodetected (`$Q_BINARY`, `~/q/m64/q`, `~/q/l64/q`, or `q` on `PATH`); if absent, legs 2–3 **skip** (exit 0), so CI without q still passes.

## Running

```sh
# clone the core from GitHub, build, and run:
make test

# or build against a local rayforce checkout:
RAYFORCE_LOCAL_PATH=/path/to/rayforce make test
```

## The `.rfl` assertion DSL

```
EXPR -- VALUE     pass if format(EXPR) == format(VALUE)
EXPR !- SUBSTR    pass if EXPR raises an error whose text contains SUBSTR
EXPR              raw setup line; fails the file if it raises
;; ...            comment
```

`qhost` / `qport` (and `qauthport` / `quser` / `qpass` for the client leg) are bound by the driver so the tests don't hard-code the server address. Each
`.rfl` file runs under a fresh runtime, so handles and `set` bindings don't leak between files.
