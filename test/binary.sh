#!/usr/bin/env bash
# Checks on the `rayforce` binary that `make rayforce` builds (the .rfl suite
# drives test/driver, which never runs the patched main()).
#   make check-binary        # after `make rayforce`
set -uo pipefail

BIN="${1:-./rayforce}"
[[ -x "$BIN" ]] || { echo "no $BIN — run 'make rayforce' first" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

fail=0
check() {  # DESCRIPTION STATUS
  if [[ "$2" == ok ]]; then echo "  ok    $1"; else echo "  FAIL  $1" >&2; fail=1; fi
}

# python3 is already what test/run.sh uses for ports and connects
free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
}

can_connect() {  # PORT — a TCP connect succeeds
  python3 -c "import socket,sys; socket.create_connection(('127.0.0.1',int(sys.argv[1])),0.5).close()" "$1" 2>/dev/null
}

# serves FLAG [SCRIPT] — start the binary with listener FLAG on a free port
# (stdin from /dev/null, stdout to a file, no -i), optionally with a startup
# script. ok when the port accepts a connection and the process is still alive
# a second after that.
serves() {
  local flag="$1" script="${2:-}" port pid i up=no
  port="$(free_port)"
  "$BIN" "$flag" "$port" ${script:+"$script"} </dev/null >"$WORK/out.log" 2>&1 &
  pid=$!
  for i in $(seq 1 50); do
    if can_connect "$port"; then up=yes; break; fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  sleep 1
  if [[ "$up" == yes ]] && kill -0 "$pid" 2>/dev/null && can_connect "$port"; then echo ok; else echo no; fi
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# fails_fast FLAG SCRIPT — the process exits non-zero by itself within 5 s.
fails_fast() {
  local flag="$1" script="$2" pid i rc
  "$BIN" "$flag" "$(free_port)" "$script" </dev/null >"$WORK/out.log" 2>&1 &
  pid=$!
  for i in $(seq 1 50); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    echo no
    return
  fi
  wait "$pid" 2>/dev/null; rc=$?
  if [[ "$rc" -ne 0 ]]; then echo ok; else echo no; fi
}

echo '(set x 1)' > "$WORK/s.rfl"
echo '(nosuchfn 1)' > "$WORK/bad.rfl"

echo "listener plus startup script, no terminal and no -i:"
check "-p keeps serving" "$(serves -p "$WORK/s.rfl")"
check "-q keeps serving" "$(serves -q "$WORK/s.rfl")"
echo "listener, no script, no terminal:"
check "-p serves" "$(serves -p)"
check "-q serves" "$(serves -q)"
# the same run's stderr: the core says so when it skips the REPL
if grep -q 'server-only mode' "$WORK/out.log"; then st=ok; else st=no; fi
check "-q takes the server-only path (no REPL on a closed stdin)" "$st"
echo "listener plus a startup script that fails:"
check "-p exits non-zero" "$(fails_fast -p "$WORK/bad.rfl")"
check "-q exits non-zero" "$(fails_fast -q "$WORK/bad.rfl")"

echo "version (needs the core's tags: network when the binary was built):"
echo '(println (.sys.build))' > "$WORK/b.rfl"
build="$("$BIN" "$WORK/b.rfl" </dev/null 2>&1)"
echo "  $build"
case "$build" in
  *'version:"0.0.0"'*) check "(.sys.build) reports the core's version" no ;;
  *'version:"'[0-9]*.[0-9]*.[0-9]*'"'*) check "(.sys.build) reports the core's version" ok ;;
  *) check "(.sys.build) reports the core's version" no ;;
esac

exit "$fail"
