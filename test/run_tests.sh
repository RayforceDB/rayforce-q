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

#!/usr/bin/env bash
# run_tests.sh — start throwaway q servers (one open, one with
# authentication), run the q .rfl tests against them, then tear them down.
#
#   ./run_tests.sh ./q_test rfl
#
# Args: $1 = path to the q_test driver, $2 = directory of .rfl files.
# Env:  Q_BINARY (q path), QHOME, PORT / AUTHPORT (default: free ports), Q_HOST.
set -euo pipefail

DRIVER="${1:?usage: run_tests.sh <driver> <rfl-dir>}"
RFL_DIR="${2:?usage: run_tests.sh <driver> <rfl-dir>}"
HOST="${Q_HOST:-127.0.0.1}"
Q_USER="qtest"
Q_PASS="secret"

# Locate the q binary.
Q_BINARY="${Q_BINARY:-}"
if [[ -z "$Q_BINARY" ]]; then
  for c in "$HOME/q/m64/q" "$(command -v q || true)"; do
    if [[ -n "$c" && -x "$c" ]]; then Q_BINARY="$c"; break; fi
  done
fi
if [[ -z "$Q_BINARY" || ! -x "$Q_BINARY" ]]; then
  echo "SKIP: q binary not found — set Q_BINARY or install q" >&2
  exit 0
fi
export QHOME="${QHOME:-$(dirname "$(dirname "$Q_BINARY")")}"
export QLIC="${QLIC:-$QHOME}"

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
}
PORT="${PORT:-$(free_port)}"
AUTHPORT="${AUTHPORT:-$(free_port)}"

WORK="$(mktemp -d)"
USERFILE="$WORK/users.txt"
printf '%s:%s\n' "$Q_USER" "$Q_PASS" > "$USERFILE"

PIDS=()
FDS=()
# Start a q server. $1=held-open fd number, $2=port, $3..=extra q args. q exits
# at stdin EOF in non-interactive mode, so hold its stdin open via a fifo for
# the run. A fixed fd number is used (not bash 4's {fd}>) for macOS bash 3.2.
start_q() {
  local fd="$1" port="$2"; shift 2
  local fifo; fifo="$WORK/fifo.$port"; mkfifo "$fifo"
  "$Q_BINARY" -p "$port" "$@" <"$fifo" >/dev/null 2>&1 &
  PIDS+=("$!")
  eval "exec $fd>\"$fifo\""   # keep the write end open -> q's stdin stays open
  FDS+=("$fd")
}

cleanup() {
  for fd in "${FDS[@]:-}"; do eval "exec $fd>&-" 2>/dev/null || true; done
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]:-}"; do wait "$pid" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

start_q 9 "$PORT"                     # open server (existing tests)
start_q 8 "$AUTHPORT" -u "$USERFILE"  # auth server (07_auth.rfl)

# Wait for both servers to accept connections.
for p in "$PORT" "$AUTHPORT"; do
  ok=0
  for _ in $(seq 1 100); do
    if python3 -c "import socket; socket.create_connection(('$HOST',$p),0.2).close()" 2>/dev/null; then
      ok=1; break
    fi
    sleep 0.05
  done
  [[ "$ok" == 1 ]] || { echo "q server on $HOST:$p did not come up" >&2; exit 1; }
done

echo "q servers up: open=$HOST:$PORT auth=$HOST:$AUTHPORT; running q tests..."
exec "$DRIVER" \
  --host "$HOST" --port "$PORT" \
  --authport "$AUTHPORT" --user "$Q_USER" --pass "$Q_PASS" \
  "$RFL_DIR"/*.rfl
