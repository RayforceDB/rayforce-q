#!/usr/bin/env bash
# run_tests.sh — start a throwaway q (kdb+) server, run the kx .rfl tests
# against it, then tear the server down.
#
#   ./run_tests.sh ./kx_test rfl
#
# Args: $1 = path to the kx_test driver, $2 = directory of .rfl files.
# Env:  Q_BINARY (q path), QHOME, PORT (default: a free port), KX_HOST.
set -euo pipefail

DRIVER="${1:?usage: run_tests.sh <driver> <rfl-dir>}"
RFL_DIR="${2:?usage: run_tests.sh <driver> <rfl-dir>}"
HOST="${KX_HOST:-127.0.0.1}"

# Locate the q binary.
Q_BINARY="${Q_BINARY:-}"
if [[ -z "$Q_BINARY" ]]; then
  for c in "$HOME/q/m64/q" "$(command -v q || true)"; do
    if [[ -n "$c" && -x "$c" ]]; then Q_BINARY="$c"; break; fi
  done
fi
if [[ -z "$Q_BINARY" || ! -x "$Q_BINARY" ]]; then
  echo "SKIP: q (kdb+) binary not found — set Q_BINARY or install kdb+" >&2
  exit 0
fi
export QHOME="${QHOME:-$(dirname "$(dirname "$Q_BINARY")")}"
export QLIC="${QLIC:-$QHOME}"

# Pick a free TCP port unless one was given.
PORT="${PORT:-$(
  python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
)}"

# q exits at stdin EOF in non-interactive mode, so hold its stdin open via a
# fifo for the lifetime of the test run.
FIFO="$(mktemp -u)"; mkfifo "$FIFO"
"$Q_BINARY" -p "$PORT" <"$FIFO" >/dev/null 2>&1 &
QPID=$!
exec 9>"$FIFO"   # keep the write end open -> q's stdin stays open

cleanup() {
  exec 9>&- 2>/dev/null || true
  kill "$QPID" 2>/dev/null || true
  wait "$QPID" 2>/dev/null || true
  rm -f "$FIFO"
}
trap cleanup EXIT

# Wait for the server to accept connections.
for _ in $(seq 1 100); do
  if python3 -c "import socket,sys; socket.create_connection(('$HOST',$PORT),0.2).close()" 2>/dev/null; then
    break
  fi
  sleep 0.05
done

echo "q server up on $HOST:$PORT (pid $QPID); running kx tests..."
# The driver binds --host/--port as kxhost/kxport for each .rfl file.
exec "$DRIVER" --host "$HOST" --port "$PORT" "$RFL_DIR"/*.rfl
