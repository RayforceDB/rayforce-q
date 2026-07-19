#!/usr/bin/env bash
# Copyright (c) 2026 RayforceDB Team
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

set -euo pipefail

DRIVER="${1:?usage: run.sh <driver> <rfl-dir>}"
RFL_DIR="${2:?usage: run.sh <driver> <rfl-dir>}"
HOST="${Q_HOST:-127.0.0.1}"
Q_USER="qtest"
Q_PASS="secret"

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
}

# wait_for_port HOST PORT — return 0 once a TCP connect succeeds, 1 on timeout.
wait_for_port() {
  local host="$1" port="$2"
  for _ in $(seq 1 100); do
    if python3 -c "import socket; socket.create_connection(('$host',$port),0.2).close()" 2>/dev/null; then
      return 0
    fi
    sleep 0.05
  done
  return 1
}

# Locate the q binary (optional). Sets QBIN, or leaves it empty.
find_q() {
  QBIN="${Q_BINARY:-}"
  if [[ -z "$QBIN" ]]; then
    for c in "$HOME/q/m64/q" "$HOME/q/l64/q" "$(command -v q || true)"; do
      if [[ -n "$c" && -x "$c" ]]; then QBIN="$c"; break; fi
    done
  fi
  [[ -n "$QBIN" && -x "$QBIN" ]] || QBIN=""
}

PIDS=()
FDS=()
WORK="$(mktemp -d)"
cleanup() {
  for fd in "${FDS[@]:-}"; do eval "exec $fd>&-" 2>/dev/null || true; done
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]:-}"; do wait "$pid" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

# ---- Leg 1: Rayforce server
SERVERPORT="${SERVERPORT:-$(free_port)}"
"$DRIVER" --serve "$SERVERPORT" &
PIDS+=("$!")
wait_for_port "$HOST" "$SERVERPORT" || { echo "driver --serve did not come up on $HOST:$SERVERPORT" >&2; exit 1; }
echo "server up on $HOST:$SERVERPORT; running server round-trip tests..."
"$DRIVER" --host "$HOST" --port "$SERVERPORT" "$RFL_DIR"/server/*.rfl

# ---- Leg 2: real-q interop against Rayforce server
find_q
if [[ -n "$QBIN" ]]; then
  echo "real-q interop: driving the server from $QBIN ..."
  QSCRIPT="$WORK/interop.q"   # q's loader requires a .q suffix
  # Drive Rayforce server (which evaluates the payloads as Rayfall) from real Q
  # and compare each Q-decoded reply against the native q value
  cat > "$QSCRIPT" <<'QEOF'
port:"I"$getenv`RFPORT
h:hopen `$":127.0.0.1:",string port
names:();oks:()
T:{[nm;b] names,:enlist nm; oks,:enlist b}
/ scalars + vectors
T["i64 atom";    3~h"(+ 1 2)"]
T["i64 expr";    42~h"(* 6 7)"]
T["i64 vec";     (0 1 2 3 4)~h"(til 5)"]
T["i64 vec expr";(10 11 12)~h"(+ 10 (til 3))"]
T["bool atom";   1b~h"true"]
T["bool vec";    101b~h"[true false true]"]
T["short atom";  42h~h"42h"]
T["int atom";    42i~h"42i"]
T["byte atom";   0x07~h"0x07"]
T["short vec";   (1 2 3h)~h"[1h 2h 3h]"]
T["int vec";     (10 20i)~h"[10i 20i]"]
T["float atom";  3.14~h"3.14"]
T["float vec";   (1 2.5 3f)~h"[1.0 2.5 3.0]"]
/ symbols, chars, strings
T["sym atom";    `abc~h"'abc"]
T["sym vec";     (`a`b`c)~h"['a 'b 'c]"]
T["char atom";   "x"~h"\"x\""]
T["string";      "hello"~h"\"hello\""]
T["string col";  (enlist "a";"bb";"ccc")~h"(as 'STR [\"a\" \"bb\" \"ccc\"])"]
/ temporal
T["date";        2024.01.15~h"2024.01.15"]
T["time";        09:30:00.000~h"09:30:00.000"]
T["timestamp";   2024.01.15D09:30:00.000000000~h"2024.01.15D09:30:00.000000000"]
/ guid (atom child-block path + vector path)
T["guid atom";   ("G"$"d49f18a4-1969-49e8-9b8a-6bb9a4832eea")~h"(as 'guid \"d49f18a4-1969-49e8-9b8a-6bb9a4832eea\")"]
T["guid vec";    (enlist "G"$"d49f18a4-1969-49e8-9b8a-6bb9a4832eea")~h"(as 'GUID (list \"d49f18a4-1969-49e8-9b8a-6bb9a4832eea\"))"]
/ table
T["table";       ([] a:1 2 3; b:`x`y`z)~h"(table [a b] (list [1 2 3] ['x 'y 'z]))"]
/ typed nulls
T["null i64";    0N~h"0Nl"]
T["null i32";    0Ni~h"0Ni"]
T["null f64";    0n~h"0Nf"]
T["null sym";    (`)~h"(as 'sym \"\")"]
/ server-side parse error comes back as a Q error frame
T["parse err";   @[h;"(+ 1 2";{x like "*parse*"}]]
hclose h
bad:names where not oks
if[count bad; -2 "real-q interop FAILED: ",", " sv bad; exit 1]
-1 "real-q interop ok (",(string count names)," assertions)"
exit 0
QEOF
  RFPORT="$SERVERPORT" QHOME="$(dirname "$(dirname "$QBIN")")" "$QBIN" "$QSCRIPT" -q < /dev/null
else
  echo "SKIP: no q binary found (set Q_BINARY) — skipping real-q interop + client tests"
  exit 0
fi

# ---- Leg 3: client tests against real Q server
export QHOME="${QHOME:-$(dirname "$(dirname "$QBIN")")}"
export QLIC="${QLIC:-$QHOME}"
PORT="${PORT:-$(free_port)}"
AUTHPORT="${AUTHPORT:-$(free_port)}"
USERFILE="$WORK/users.txt"
printf '%s:%s\n' "$Q_USER" "$Q_PASS" > "$USERFILE"

# Start a q server
start_q() {
  local fd="$1" port="$2"; shift 2
  local fifo; fifo="$WORK/fifo.$port"; mkfifo "$fifo"
  "$QBIN" -p "$port" "$@" <"$fifo" >/dev/null 2>&1 &
  PIDS+=("$!")
  eval "exec $fd>\"$fifo\""   # keep the write end open -> q's stdin stays open
  FDS+=("$fd")
}

start_q 9 "$PORT"                     # open server
start_q 8 "$AUTHPORT" -u "$USERFILE"  # auth server (07_auth.rfl)

for p in "$PORT" "$AUTHPORT"; do
  wait_for_port "$HOST" "$p" || { echo "q server on $HOST:$p did not come up" >&2; exit 1; }
done

echo "q servers up: open=$HOST:$PORT auth=$HOST:$AUTHPORT; running client tests..."
"$DRIVER" \
  --host "$HOST" --port "$PORT" \
  --authport "$AUTHPORT" --user "$Q_USER" --pass "$Q_PASS" \
  "$RFL_DIR"/client/*.rfl
