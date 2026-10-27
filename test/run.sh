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

q_home_for() {
  local qbin="$1" qdir parent
  if command -v python3 >/dev/null 2>&1; then
    qbin="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$qbin")"
  fi
  qdir="$(cd "$(dirname "$qbin")" && pwd)"
  parent="$(cd "$qdir/.." && pwd)"
  if [[ -f "$qdir/q.k" ]]; then
    printf '%s\n' "$qdir"
  elif [[ -f "$parent/q.k" ]]; then
    printf '%s\n' "$parent"
  else
    printf '%s\n' "$parent"
  fi
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

echo "running codec selftest..."
"$DRIVER" --codec-selftest

echo "running exchange selftest..."
"$DRIVER" --exchange-selftest

echo "running journal bound selftest..."
"$DRIVER" --journal-bound-selftest

echo "running hooks teardown selftest..."
"$DRIVER" --hooks-teardown-selftest

echo "running connection identity selftest..."
"$DRIVER" --conn-identity-selftest

echo "running partial send selftest..."
"$DRIVER" --partial-send-selftest

echo "running Q CLI argument selftest..."
"$DRIVER" --q-cli-selftest

# ---- Bind address of the `-q` listener: loopback unless asked otherwise.
# q_serve_from_args is what the rayforce binary runs for `-q`; the driver's
# --serve-args drives it directly. Reads the kernel's listening table, so it
# needs /proc/net/tcp (Linux) and skips elsewhere.
if [[ -r /proc/net/tcp ]]; then
  echo "checking the -q listener's bind address..."
  listen_addr() {  # PORT -> local address of its LISTEN socket, e.g. 127.0.0.1
    python3 - "$1" <<'PY'
import socket, struct, sys
port = int(sys.argv[1])
for line in open("/proc/net/tcp").read().splitlines()[1:]:
    f = line.split()
    addr, p = f[1].split(":")
    if f[3] == "0A" and int(p, 16) == port:
        print(socket.inet_ntoa(struct.pack("<I", int(addr, 16))))
        break
PY
  }
  check_bind() {  # WANT PORT-spec [env assignments...] -- start a listener, compare
    local want="$1" spec="$2" port; shift 2
    port="${spec##*:}"
    env "$@" "$DRIVER" --serve-args -q "$spec" 2>/dev/null &
    local pid=$!
    PIDS+=("$pid")
    wait_for_port 127.0.0.1 "$port" || { echo "bind test: -q $spec did not listen" >&2; exit 1; }
    local got; got="$(listen_addr "$port")"
    kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
    if [[ "$got" != "$want" ]]; then
      echo "bind test: -q $spec (env: $*) bound $got, expected $want" >&2; exit 1
    fi
  }
  BP="$(free_port)"
  check_bind 127.0.0.1 "$BP" RAYFORCE_Q_BIND_HOST=              # no host given: loopback
  check_bind 0.0.0.0 "$BP" RAYFORCE_Q_BIND_HOST=0.0.0.0          # env var opts in
  check_bind 127.0.0.1 "127.0.0.1:$BP" RAYFORCE_Q_BIND_HOST=0.0.0.0   # HOST:PORT wins over env
  check_bind 0.0.0.0 "0.0.0.0:$BP" RAYFORCE_Q_BIND_HOST=127.0.0.1
  echo "bind address checks ok (loopback by default)"
fi

# ---- Leg 1: Rayforce server
SERVERPORT="${SERVERPORT:-$(free_port)}"
"$DRIVER" --serve "$SERVERPORT" &
PIDS+=("$!")
wait_for_port "$HOST" "$SERVERPORT" || { echo "driver --serve did not come up on $HOST:$SERVERPORT" >&2; exit 1; }
echo "server up on $HOST:$SERVERPORT; running server round-trip tests..."
"$DRIVER" --host "$HOST" --port "$SERVERPORT" "$RFL_DIR"/server/*.rfl

echo "checking client keepalive socket options..."
"$DRIVER" --keepalive-selftest "$HOST" "$SERVERPORT"

echo "checking frame limits, client and listener..."
"$DRIVER" --frame-limit-selftest "$HOST" "$SERVERPORT"

echo "checking malformed Q frame handling..."
python3 - "$HOST" "$SERVERPORT" <<'PY'
import socket
import struct
import sys

host, port = sys.argv[1], int(sys.argv[2])

def recv_closed(sock):
    try:
        return sock.recv(1) == b''
    except ConnectionResetError:
        return True

with socket.create_connection((host, port), 1.0) as s:
    s.sendall(bytes([3, 0]))
    if s.recv(1) != bytes([3]):
        raise SystemExit("bad Q handshake response")
    s.sendall(struct.pack("<BBBBI", 1, 1, 1, 0, 9) + b"x")
    resp = s.recv(64)

if len(resp) < 9:
    raise SystemExit("malformed-frame test: server closed without Q error")
if resp[:4] != bytes([1, 2, 0, 0]):
    raise SystemExit(f"malformed-frame test: unexpected header {resp.hex()}")
if resp[8] != 0x80:
    raise SystemExit(f"malformed-frame test: expected Q error, got {resp.hex()}")

with socket.create_connection((host, port), 1.0) as s:
    s.sendall(bytes([3, 0]))
    if s.recv(1) != bytes([3]):
        raise SystemExit("bad Q handshake response")
    s.sendall(struct.pack("<BBBBI", 1, 1, 2, 0, 10) + bytes([101, 0]))
    if not recv_closed(s):
        raise SystemExit("invalid compressed flag was answered")

with socket.create_connection((host, port), 1.0) as s:
    s.sendall(bytes([3, 0]))
    if s.recv(1) != bytes([3]):
        raise SystemExit("bad Q handshake response")
    s.sendall(struct.pack("<BBBBI", 1, 1, 0, 0, 8))
    if not recv_closed(s):
        raise SystemExit("empty sync frame was answered")
PY

echo "checking unknown Q message type handling..."
python3 - "$HOST" "$SERVERPORT" <<'PY'
import socket
import struct
import sys

host, port = sys.argv[1], int(sys.argv[2])

def recv_closed(sock):
    try:
        return sock.recv(1) == b''
    except ConnectionResetError:
        return True

with socket.create_connection((host, port), 1.0) as s:
    s.sendall(bytes([3, 0]))
    if s.recv(1) != bytes([3]):
        raise SystemExit("bad Q handshake response")
    # Q identity is a valid body; only the message type is invalid.
    s.sendall(struct.pack("<BBBBI", 1, 9, 0, 0, 10) + bytes([101, 0]))
    if not recv_closed(s):
        raise SystemExit("unknown message type was executed or answered")

with socket.create_connection((host, port), 1.0) as s:
    s.sendall(bytes([3, 0]))
    if s.recv(1) != bytes([3]):
        raise SystemExit("bad Q handshake response")
    # A header-only frame must reject the unknown message type before treating
    # the empty body as an identity request.
    s.sendall(struct.pack("<BBBBI", 1, 9, 0, 0, 8))
    if not recv_closed(s):
        raise SystemExit("empty unknown message type was answered")

with socket.create_connection((host, port), 1.0) as s:
    s.sendall(bytes([3, 0]))
    if s.recv(1) != bytes([3]):
        raise SystemExit("bad Q handshake response")
    # RESPONSE frames belong only on outbound q_conn_attach handles. A client
    # connected to the server listener must not be allowed to continue after
    # sending one.
    s.sendall(struct.pack("<BBBBI", 1, 2, 0, 0, 10) + bytes([101, 0]))
    if not recv_closed(s):
        raise SystemExit("inbound response frame was accepted")
PY

echo "checking multi-chunk Q frame decoding..."
python3 - "$HOST" "$SERVERPORT" <<'PY'
import socket
import struct
import sys

host, port = sys.argv[1], int(sys.argv[2])
count = 40_000
values = list(range(count))
request = bytes([6, 0]) + struct.pack("<I", count) + struct.pack(
    f"<{count}i", *values
)

def recv_exact(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        part = sock.recv(size - len(chunks))
        if not part:
            raise SystemExit("multi-chunk test: server closed early")
        chunks.extend(part)
    return bytes(chunks)

with socket.create_connection((host, port), 1.0) as s:
    s.sendall(bytes([3, 0]))
    if recv_exact(s, 1) != bytes([3]):
        raise SystemExit("bad Q handshake response")
    s.sendall(struct.pack("<BBBBI", 1, 1, 0, 0, len(request) + 8))
    s.sendall(request)
    header = recv_exact(s, 8)
    if header[:4] != bytes([1, 2, 0, 0]):
        raise SystemExit(f"multi-chunk test: unexpected response header {header.hex()}")
    body = recv_exact(s, struct.unpack_from("<I", header, 4)[0] - 8)
    if body != request:
        raise SystemExit("multi-chunk test: vector changed during round-trip")
PY

if [[ -r "/proc/${PIDS[0]}/status" ]]; then
  echo "checking partial large-frame memory use..."
  python3 - "$HOST" "$SERVERPORT" "${PIDS[0]}" <<'PY'
import socket
import struct
import sys
import time

host, port, pid = sys.argv[1], int(sys.argv[2]), sys.argv[3]
status_path = f"/proc/{pid}/status"

def vm_size_kib():
    with open(status_path, encoding="ascii") as f:
        for line in f:
            if line.startswith("VmSize:"):
                return int(line.split()[1])
    raise RuntimeError("VmSize missing from server status")

before = vm_size_kib()
with socket.create_connection((host, port), 1.0) as s:
    s.sendall(bytes([3, 0]))
    if s.recv(1) != bytes([3]):
        raise SystemExit("bad Q handshake response")
    # Advertise a 128 MiB body, then hold the connection without sending it.
    total_size = 128 * 1024 * 1024
    s.sendall(struct.pack("<BBBBI", 1, 1, 0, 0, total_size))
    time.sleep(0.25)
    delta_kib = vm_size_kib() - before
    if delta_kib > 32 * 1024:
        raise SystemExit(
            f"partial-frame test: server reserved {delta_kib} KiB before body arrived"
        )

print(f"partial-frame memory check ok ({delta_kib} KiB growth)")
PY
fi

# ---- Leg 2: real-q interop against Rayforce server
find_q
if [[ -n "$QBIN" ]]; then
  RAISE_API=1
  EVAL_H="$(dirname "$0")/tmp/rayforce-c/src/lang/eval.h"
  if [[ -f "$EVAL_H" ]] && ! grep -q ray_raise_render "$EVAL_H"; then RAISE_API=0; fi
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
/ dict
T["dict";        (`a`b!1 2)~h"(dict ['a 'b] [1 2])"]
/ typed nulls
T["null i64";    0N~h"0Nl"]
T["null i32";    0Ni~h"0Ni"]
T["null f64";    0n~h"0Nf"]
T["null sym";    (`)~h"(as 'sym \"\")"]
/ Engine errors reach q as "category: message". A user raise reaches it as
/ the raised text alone, matching kdb+. A core without ray_raise_render
/ (before v3) cannot recover that text, so there the category is the contract.
RA:not"0"=first getenv`RAISE_API
T["raise text";      $[RA;"custom failure"~@[h;"(raise \"custom failure\")";{x}];@[h;"(raise \"custom failure\")";{x like "domain*"}]]]
T["raise symbol";    $[RA;"TIME"~@[h;"(raise 'TIME)";{x}];@[h;"(raise 'TIME)";{x like "domain*"}]]]
T["name text";       @[h;"(undefined_fn 1)";{(x like "name: *")&0<count x ss "undefined_fn"}]]
T["builtin text";    @[h;"(+ 1 \"a\")";{x like "type: *"}]]
T["no message";      "parse"~@[h;"(+ 1 2";{x}]]
r:h"(do (set boom (fn [x] (raise \"custom failure\"))) 1)"
T["raise via call";  $[RA;"custom failure"~@[h;(`boom;::);{x}];@[h;(`boom;::);{x like "domain*"}]]]
/ a result with no q value (a lambda): the whole text arrives, not its first 7 characters
T["encode failure text"; @[h;"(fn [m] m)";{(x like "type: *")&0<count x ss "cannot"}]]
/ an empty or blank request is the identity (::), kdb's ping/flush
T["empty request";   (::)~h""]
T["blank request";   (::)~h"  "]
T["still evaluates"; 3~h"(+ 1 2)"]
/ server-side parse error comes back as a Q error frame
T["parse err";   @[h;"(+ 1 2";{x like "*parse*"}]]
hclose h
bad:names where not oks
if[count bad; -2 "real-q interop FAILED: ",", " sv bad; exit 1]
-1 "real-q interop ok (",(string count names)," assertions)"
exit 0
QEOF
  RAISE_API="$RAISE_API" RFPORT="$SERVERPORT" QHOME="${QHOME:-$(q_home_for "$QBIN")}" "$QBIN" "$QSCRIPT" -q < /dev/null
else
  echo "SKIP: no q binary found (set Q_BINARY) — skipping real-q interop + client tests"
  exit 0
fi

# ---- Leg 2b: q-wire frames are journaled like native ones when a WAL is open
# core/ipc.c logs every inbound native SYNC frame before evaluating it; the q
# listener has to do the same or a Rayfall service fed over -q journals nothing.
# Process A opens a managed journal and takes q frames; process B is a fresh
# runtime that replays the log, so the state it rebuilds is what a restart sees.
echo "real-q WAL: journaling inbound q frames, replaying in a fresh runtime ..."
WALBASE="$WORK/wal/node"
HASSCOPE=0
grep -q RAY_IPC_HAS_SET_AUTO_JOURNAL_EVAL "${CORE_SRC:-/nonexistent}/core/ipc.h" 2>/dev/null && HASSCOPE=1
mkdir -p "$WORK/wal"
if [ "$HASSCOPE" != "1" ]; then
  # A core without ray_ipc_set_auto_journal_eval cannot give the evaluation
  # the auto-journal scope, so with a journal open every sync request must be
  # refused before it is appended or evaluated.
  WALPORT="$(free_port)"
  "$DRIVER" --serve "$WALPORT" 2>"$WORK/wal_a.err" &
  WALPID=$!
  PIDS+=("$WALPID")
  wait_for_port "$HOST" "$WALPORT" || { echo "WAL server did not come up" >&2; exit 1; }
  cat > "$WORK/wal_n.q" <<'QEOF'
port:"I"$getenv`RFPORT
base:getenv`WALBASE
h:hopen `$":127.0.0.1:",string port
names:();oks:()
T:{[nm;b] names,:enlist nm; oks,:enlist b}
/ every sync request is refused while the journal is open, so the log file
/ itself is what shows nothing was appended
sz:{hcount `$":",base,".log"}
r:h"(.log.open 'sync \"",base,"\")"
s0:sz[]
r:@[h;"(set n 1)";{x}]
T["refused without the scope API"; r like "domain*"]
T["refused request not journaled"; s0=sz[]]
r:hclose h
bad:names where not oks
if[count bad; -2 "q WAL (no scope API) FAILED: ",", " sv bad; exit 1]
-1 "q WAL refused without scope API ok (",(string count names)," assertions)"
exit 0
QEOF
  RFPORT="$WALPORT" WALBASE="$WALBASE" QHOME="${QHOME:-$(q_home_for "$QBIN")}" "$QBIN" "$WORK/wal_n.q" -q < /dev/null
  kill "$WALPID" 2>/dev/null || true
  wait "$WALPID" 2>/dev/null || true
else
WALPORT="$(free_port)"
"$DRIVER" --serve "$WALPORT" 2>"$WORK/wal_a.err" &
WALPID=$!
PIDS+=("$WALPID")
wait_for_port "$HOST" "$WALPORT" || { echo "WAL server A did not come up" >&2; exit 1; }
cat > "$WORK/wal_a.q" <<'QEOF'
port:"I"$getenv`RFPORT
base:getenv`WALBASE
scope:getenv`HASSCOPE
h:hopen `$":127.0.0.1:",string port
names:();oks:()
T:{[nm;b] names,:enlist nm; oks,:enlist b}
chunks:{first h"(.log.validate \"",base,".log\")"}
/ `a` and `as` are also globals/builtins on the server: they must stay literal
r:h"(set tags (list))"
r:h"(set a 99)"
r:h"(do (set upd (fn [m] (set tags (concat tags (list m))))) 1)"
r:h"(.log.open 'sync \"",base,"\")"
c0:chunks[]            / counts itself: a sync frame is journaled before it runs
r:h(`upd;(`a;1i;"s"))
c1:chunks[]
T["sync frame journaled";    c1=c0+2]
neg[h](`upd;(`qasync;2i)); r:h"1"
c2:chunks[]
T["async frame not journaled (core rule)"; c2=c1+2]  / the h"1" and chunks[] syncs
r:h(`upd;(`b;`a;3))
/ a LIST argument is data on both paths, even when its head names a callable
r:h(`upd;(`in;1 2))
r:h(`upd;(`sum;1 2))
T["live state";   ((`a;1i;"s");(`qasync;2i);(`b;`a;3);(`in;1 2);(`sum;1 2))~h"tags"]
T["journal grew"; chunks[]>c2]
/ a frame the journal could not replay (native payload above the 256 MiB the
/ core's replay accepts) is refused before anything is written or evaluated:
/ a LIST [symbol; byte vector of 256 MiB - 17] is a q body of exactly 256 MiB
c3:chunks[]
r:@[h;(`upd;268435439#0x00);{x}]
T["oversize frame refused";  r like "limit*"]
T["oversize not journaled";  chunks[]=c3+1]
T["oversize not evaluated";  5=count h"tags"]
/ the core refuses .log.write inside an auto-journaled eval (the frame is
/ already in the log); the q path must run its eval in the same scope
r:h"(do (set n 0) (set bump (fn [] (set n (+ n 1)))) 1)"
r:@[h;"(do (.log.write (list 'bump)) (bump) (bump))";{x}]
T["nested .log.write refused"; $[scope~"1"; r like "domain*"; 1b]]
T["nested .log.write: no effect"; $[scope~"1"; 0~h"n"; 1b]]
r:hclose h
bad:names where not oks
if[count bad; -2 "q WAL (journal) FAILED: ",", " sv bad; exit 1]
-1 "q WAL journal ok (",(string count names)," assertions)"
exit 0
QEOF
RFPORT="$WALPORT" WALBASE="$WALBASE" HASSCOPE="$HASSCOPE" QHOME="${QHOME:-$(q_home_for "$QBIN")}" "$QBIN" "$WORK/wal_a.q" -q < /dev/null
kill "$WALPID" 2>/dev/null || true
wait "$WALPID" 2>/dev/null || true

WALPORT2="$(free_port)"
"$DRIVER" --serve "$WALPORT2" 2>"$WORK/wal_b.err" &
WALPID2=$!
PIDS+=("$WALPID2")
wait_for_port "$HOST" "$WALPORT2" || { echo "WAL server B did not come up" >&2; exit 1; }
cat > "$WORK/wal_b.q" <<'QEOF'
port:"I"$getenv`RFPORT
base:getenv`WALBASE
scope:getenv`HASSCOPE
h:hopen `$":127.0.0.1:",string port
names:();oks:()
T:{[nm;b] names,:enlist nm; oks,:enlist b}
r:h"(set tags (list))"
r:h"(set a 1234)"          / different value: replay must not resolve the symbols
r:h"(do (set n 0) (set bump (fn [] (set n (+ n 1)))) 1)"
r:h"(do (set upd (fn [m] (set tags (concat tags (list m))))) 1)"
n:h"(.log.replay \"",base,".log\")"
T["replayed chunks"; n>=2]
T["replay: nested-write frame left no effect"; $[scope~"1"; 0~h"n"; 1b]]
T["sync frames rebuilt (async not journaled)"; ((`a;1i;"s");(`b;`a;3);(`in;1 2);(`sum;1 2))~h"tags"]
r:hclose h
bad:names where not oks
if[count bad; -2 "q WAL (replay) FAILED: ",", " sv bad; exit 1]
-1 "q WAL replay ok (",(string count names)," assertions)"
exit 0
QEOF
RFPORT="$WALPORT2" WALBASE="$WALBASE" HASSCOPE="$HASSCOPE" QHOME="${QHOME:-$(q_home_for "$QBIN")}" "$QBIN" "$WORK/wal_b.q" -q < /dev/null
kill "$WALPID2" 2>/dev/null || true
wait "$WALPID2" 2>/dev/null || true
fi

# ---- Leg 2b': a sync hook and the WAL together
# A sync hook replaces the default evaluation, so a journaled request that ran
# through a hook must replay through it too. With a hook installed the request
# is journaled as (.q.dispatch_sync (quote <request>)), which applies whatever
# hook is installed at replay time, or evaluates by default when there is none.
# Each case compares a live process against fresh processes that replay its log.
if [ "$HASSCOPE" = "1" ]; then
echo "real-q WAL + hooks: live vs fresh-process replay ..."
WHDIR="$WORK/walhook"; mkdir -p "$WHDIR"
WH_PIDS=()
wh_start() { # wh_start NAME -> sets WH_PORT_<NAME>
  local port; port="$(free_port)"
  "$DRIVER" --serve "$port" 2>"$WHDIR/$1.err" &
  WH_PIDS+=("$!"); PIDS+=("$!")
  wait_for_port "$HOST" "$port" || { echo "WAL+hook server $1 did not come up" >&2; exit 1; }
  eval "WH_PORT_$1=$port"
}
for nm in A1 B1 A2 B2 C2; do wh_start "$nm"; done
cat > "$WHDIR/walhook.q" <<'QEOF'
op:{hopen `$":127.0.0.1:",getenv x}
names:();oks:()
T:{[nm;b] names,:enlist nm; oks,:enlist b}
/ -- 1. a hook that REPLACES evaluation, installed after the journal opened
base1:getenv`WH_BASE1
a:op`WH_PORT_A1
r:a"(set n 0)"
r:a"(.log.open 'sync \"",base1,"\")"
r:a"(.q.on 'sync (fn [r] (set n (+ n 1))))"
r:a"1"; r:a"1"; r:a"1"
neg[a]"(.q.on 'sync 0)"          / async: not journaled, not hooked
T["live: the hook answered each request"; 3~a"n"]
hclose a
b:op`WH_PORT_B1
r:b"(set n 0)"
r:b"(.log.replay \"",base1,".log\")"
neg[b]"(.q.on 'sync 0)"
T["replay: the hooked requests ran the hook"; 3~b"n"]
hclose b
/ -- 2. the hook is installed before the journal opens and before the replay
base2:getenv`WH_BASE2
hk:"(do (set seen (list)) (set n 0) (.q.on 'sync (fn [r] (do (set seen (concat seen (list r))) (if (== (type r) 'str) (eval (parse r)) r)))) 1)"
a:op`WH_PORT_A2
r:a hk
r:a"(.log.open 'sync \"",base2,"\")"
r:a"(set n (+ n 1))"
r:a"(set n (+ n 10))"
r:a(`ev;1 2)
na:a"n"                          / a sync read is journaled and hooked too
s1:a"seen"
T["live: hook saw every request"; 6=count s1]
T["live: state"; 11~na]
hclose a
rc:"(.log.replay \"",base2,".log\")"
b:op`WH_PORT_B2
r:b hk
r:b rc
sb:b"seen"
T["replay with the hook: it saw the same requests"; sb~(enlist rc),(1_ s1),enlist"seen"]
T["replay with the hook: same state"; 11~b"n"]
hclose b
c:op`WH_PORT_C2
r:c"(do (set seen (list)) (set n 0) 1)"
r:c rc
T["replay without the hook: evaluated by default"; 11~c"n"]
T["replay without the hook: it was never called"; 0=count c"seen"]
hclose c
bad:names where not oks
if[count bad; -2 "q WAL + hooks FAILED: ",", " sv bad; exit 1]
-1 "q WAL + hooks ok (",(string count names)," assertions)"
exit 0
QEOF
WH_PORT_A1="$WH_PORT_A1" WH_PORT_B1="$WH_PORT_B1" WH_PORT_A2="$WH_PORT_A2" WH_PORT_B2="$WH_PORT_B2" WH_PORT_C2="$WH_PORT_C2" \
  WH_BASE1="$WHDIR/one" WH_BASE2="$WHDIR/two" \
  QHOME="${QHOME:-$(q_home_for "$QBIN")}" "$QBIN" "$WHDIR/walhook.q" -q < /dev/null
for p in "${WH_PIDS[@]}"; do kill "$p" 2>/dev/null || true; wait "$p" 2>/dev/null || true; done
fi

# ---- Leg 2c: inbound connections -- .q.handle, .q.post, .q.on hooks
# A kdb-compatible pub/sub server on the embedded runtime: a sync hook answers
# `(.net.sub;5)` (a reserved-looking name arriving as the head symbol of a q
# list, which the default eval could not call), records the subscriber's handle,
# and another client makes the server push `(`upd;data)` async frames back
# with .q.post. A dedicated server keeps the hooks away from the other legs.
echo "real-q hooks: .q.handle / .q.post / .q.on on inbound connections ..."
HKPORT="$(free_port)"
"$DRIVER" --serve "$HKPORT" 2>"$WORK/hooks.err" &
HKPID=$!
PIDS+=("$HKPID")
wait_for_port "$HOST" "$HKPORT" || { echo "hooks server did not come up" >&2; exit 1; }
cat > "$WORK/hooks.q" <<'QEOF'
port:"I"$getenv`RFPORT
h:hopen `$":127.0.0.1:",string port
h2:hopen `$":127.0.0.1:",string port
names:();oks:()
T:{[nm;b] names,:enlist nm; oks,:enlist b}
got:()
upd:{got,::enlist x}
r:h"(do (set subs (list)) (set closed (list)) (set seen (list)) (.q.on 'sync (fn [r] (if (== (type r) 'str) (eval (parse r)) (if (== (first r) '.net.sub) (do (set subs (concat subs (list (.q.handle)))) 5) r)))) (.q.on 'async (fn [r] (set seen (concat seen (list (list r (.q.handle))))))) (.q.on 'close (fn [x] (set closed (concat closed (list (list x (.q.handle))))))) 1)"
T["reserved name reaches sync hook"; 5~h(`.net.sub;5)]
sh:h"(first subs)"
T["handle is the inbound conn"; (sh~h"(.q.handle)")]
T["hook value is the reply"; (`foo;`a`b!1 2)~h(`foo;`a`b!1 2)]
T["string reaches hook unparsed"; 7~h"(+ 3 4)"]
r:h2"(.q.post (first subs) (list 'upd (dict ['a 'b] [1 2])))"
T["post returns null"; (::)~r]
r:h"1"
T["pushed async arrives"; got~enlist `a`b!1 2]
neg[h](`ping;3); r:h"1"
T["async hook sees request + handle"; (enlist((`ping;3);sh))~h2"seen"]
T["post to bad handle"; @[h2;"(.q.post 99999 1)";{x like "handle*"}]]
hclose h
system"sleep 0.3"
T["close hook fired once"; (enlist(sh;sh))~h2"closed"]
r:h2"(.q.on 'sync (fn [r] (if (== r \"boom\") (raise \"hook failed\") (eval (parse r)))))"
/ the raised text needs a core with ray_raise_render; before it the category alone arrives
RA:not"0"=first getenv`RAISE_API
T["hook error reaches client"; $[RA;"hook failed"~@[h2;"boom";{x}];@[h2;"boom";{x like "domain*"}]]]
r:h2"(do (.q.on 'sync 0) (.q.on 'async 0) (.q.on 'close 0) 1)"
T["hook cleared"; 3~h2"(+ 1 2)"]
hclose h2
bad:names where not oks
if[count bad; -2 "q hooks FAILED: ",", " sv bad; exit 1]
-1 "q hooks ok (",(string count names)," assertions)"
exit 0
QEOF
RAISE_API="$RAISE_API" RFPORT="$HKPORT" QHOME="${QHOME:-$(q_home_for "$QBIN")}" "$QBIN" "$WORK/hooks.q" -q < /dev/null
kill "$HKPID" 2>/dev/null || true
wait "$HKPID" 2>/dev/null || true

# ---- Leg 3: client tests against real Q server
export QHOME="${QHOME:-$(q_home_for "$QBIN")}"
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

echo "running q_decode_cols selftest against q..."
"$DRIVER" --cols-selftest "$HOST" "$PORT"

# ---- Leg 4: the same client tests with connections on an event loop, plus
# the push tests that only exist in that mode. Re-running client/ is the point:
# the poll path must be a drop-in for every request/response case, not just a
# new feature bolted next to it.
echo "running client + push tests with --poll ..."
"$DRIVER" --poll \
  --host "$HOST" --port "$PORT" \
  --authport "$AUTHPORT" --user "$Q_USER" --pass "$Q_PASS" \
  "$RFL_DIR"/client/*.rfl "$RFL_DIR"/push/*.rfl
