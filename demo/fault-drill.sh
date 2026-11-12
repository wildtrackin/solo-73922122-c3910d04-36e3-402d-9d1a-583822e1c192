#!/usr/bin/env bash
# Local fault-drill helpers for the WASM plugin sandbox.
#
# Every scenario only signals the *API worker* inside the still-running
# container.  The container and its PID-1 supervisor are never restarted.
#
# Usage:
#   demo/fault-drill.sh pause      # SIGSTOP the API, run the loop plugin, resume
#   demo/fault-drill.sh kill       # SIGKILL the API; watch the purge + restart
#   demo/fault-drill.sh status     # show supervisor, API and runner processes
#   demo/fault-drill.sh classify   # one normal classification call
set -euo pipefail

COMPOSE="docker compose"
SERVICE="wasm-plugin-sandbox"

api_pid() {
  # The API worker is the child of the PID-1 supervisor whose command line
  # contains uvicorn.  Select it by walking /proc inside the container.
  $COMPOSE exec -T "$SERVICE" python - <<'PY'
import os
for name in os.listdir("/proc"):
    if not name.isdigit():
        continue
    try:
        with open(f"/proc/{name}/cmdline", "rb") as f:
            cmd = f.read().replace(b"\x00", b" ").decode(errors="replace")
        with open(f"/proc/{name}/stat") as f:
            ppid = int(f.read().split()[3])
    except OSError:
        continue
    if ppid == 1 and "uvicorn" in cmd:
        print(name)
        break
PY
}

case "${1:-status}" in
  status)
    echo "== processes in container =="
    $COMPOSE exec -T "$SERVICE" ps -eo pid,ppid,stat,args | \
      grep -E "supervisor|uvicorn|runner.py" | grep -v grep || true
    echo "== runtime tree (control socket + staging) =="
    $COMPOSE exec -T "$SERVICE" sh -c 'ls -la /tmp/wasm-sandbox 2>/dev/null; ls -la /tmp/wasm-sandbox/runs 2>/dev/null || true'
    ;;

  classify)
    curl -sS \
      -F 'module=@demo/classify.wasm;type=application/wasm' \
      -F 'log=@demo/sample-log.json;type=application/json' \
      http://localhost:8000/api/v1/analyze
    echo
    ;;

  pause)
    pid="$(api_pid)"
    echo "SIGSTOP API worker pid $pid (container stays up)"
    $COMPOSE exec -T "$SERVICE" kill -STOP "$pid"
    echo "Posting dead-loop plugin demo/loop.wasm against the stopped API..."
    set +e
    curl -sS -m 15 \
      -F 'module=@demo/loop.wasm;type=application/wasm' \
      -F 'log=@demo/sample-log.json;type=application/json' \
      http://localhost:8000/api/v1/analyze
    rc=$?
    set -e
    echo
    echo "curl exit: $rc (28 = timed out while the API was frozen)"
    echo "Staging while frozen (the supervisor removes it at the 2s deadline):"
    $COMPOSE exec -T "$SERVICE" sh -c 'ls -la /tmp/wasm-sandbox/runs 2>/dev/null || echo "  (empty)"'
    echo "Resuming API worker with SIGCONT..."
    $COMPOSE exec -T "$SERVICE" kill -CONT "$pid"
    sleep 1
    echo "Re-issuing the same dead-loop call after resume -> expected 504 timeout:"
    curl -sS \
      -F 'module=@demo/loop.wasm;type=application/wasm' \
      -F 'log=@demo/sample-log.json;type=application/json' \
      http://localhost:8000/api/v1/analyze
    echo
    ;;

  kill)
    pid="$(api_pid)"
    echo "Launching three dead-loop runs, then SIGKILL API worker pid $pid"
    for _ in 1 2 3; do
      curl -sS -m 15 \
        -F 'module=@demo/loop.wasm;type=application/wasm' \
        -F 'log=@demo/sample-log.json;type=application/json' \
        http://localhost:8000/api/v1/analyze >/dev/null 2>&1 &
    done
    sleep 1
    echo "Runners before kill:"
    $COMPOSE exec -T "$SERVICE" ps -eo pid,ppid,stat,args | grep runner.py | grep -v grep || true
    $COMPOSE exec -T "$SERVICE" kill -KILL "$pid"
    echo "SIGKILL sent. Waiting <=1s for the supervisor purge..."
    sleep 1
    echo "Runners after purge (expect none):"
    $COMPOSE exec -T "$SERVICE" ps -eo pid,ppid,stat,args | grep runner.py | grep -v grep || echo "  (no runners)"
    echo "Staging after purge (expect empty):"
    $COMPOSE exec -T "$SERVICE" sh -c 'ls -la /tmp/wasm-sandbox/runs 2>/dev/null || echo "  (empty)"'
    echo "Waiting for the supervisor to restart the API worker..."
    for _ in $(seq 1 30); do
      if curl -fsS http://localhost:8000/healthz >/dev/null 2>&1; then break; fi
      sleep 0.5
    done
    echo "New API worker:"
    $COMPOSE exec -T "$SERVICE" ps -eo pid,ppid,stat,args | grep uvicorn | grep -v grep
    echo "A fresh classification after restart:"
    curl -sS \
      -F 'module=@demo/classify.wasm;type=application/wasm' \
      -F 'log=@demo/sample-log.json;type=application/json' \
      http://localhost:8000/api/v1/analyze
    echo
    ;;

  *)
    echo "usage: $0 {status|classify|pause|kill}" >&2
    exit 2
    ;;
esac
