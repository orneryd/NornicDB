#!/usr/bin/env bash
set -euo pipefail

# Python-based E2E compatibility test using the real qdrant-client SDK (gRPC).
#
# This script:
# - starts NornicDB with Qdrant gRPC enabled on a random port
# - creates a temporary venv
# - installs qdrant-client + grpc deps
# - runs scripts/qdrantgrpc_e2e_python.py against the server
#
# Notes:
# - We set NORNICDB_EMBEDDING_ENABLED=false so the Qdrant API can mutate vectors.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

pick_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nornicdb-qdrantgrpc-py-e2e.XXXXXX")"
VENV_DIR="${DATA_DIR}/venv"
SERVER_LOG="${DATA_DIR}/server.log"

HTTP_PORT="$(pick_port)"
BOLT_PORT="$(pick_port)"
GRPC_PORT="$(pick_port)"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${DATA_DIR}" || true
}
trap cleanup EXIT

export NORNICDB_QDRANT_GRPC_ENABLED=true
export NORNICDB_QDRANT_GRPC_LISTEN_ADDR="127.0.0.1:${GRPC_PORT}"
export NORNICDB_EMBEDDING_ENABLED=false
export NORNICDB_MCP_ENABLED=false
export NORNICDB_HEIMDALL_ENABLED=false

echo "Data dir: ${DATA_DIR}"
echo "HTTP:     127.0.0.1:${HTTP_PORT}"
echo "Bolt:     127.0.0.1:${BOLT_PORT}"
echo "gRPC:     127.0.0.1:${GRPC_PORT}"
echo

echo "Starting NornicDB..."
go run ./cmd/nornicdb serve \
  --data-dir "${DATA_DIR}" \
  --address 127.0.0.1 \
  --http-port "${HTTP_PORT}" \
  --bolt-port "${BOLT_PORT}" \
  --no-auth \
  --headless \
  >"${SERVER_LOG}" 2>&1 &
SERVER_PID="$!"

echo "Waiting for gRPC port to open..."
python3 - <<PY
import socket, time, sys
addr=("127.0.0.1", int("${GRPC_PORT}"))
deadline=time.time()+45
while time.time() < deadline:
    s=socket.socket()
    s.settimeout(0.2)
    try:
        s.connect(addr)
        s.close()
        sys.exit(0)
    except Exception:
        s.close()
        time.sleep(0.2)
print("Timed out waiting for gRPC to open. Server log:")
print(open("${SERVER_LOG}","r",encoding="utf-8",errors="replace").read())
sys.exit(1)
PY

echo "Creating venv + installing qdrant-client..."
python3 -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"
python -m pip install --upgrade pip >/dev/null

# Pinning keeps the test stable across breaking API changes.
python -m pip install "qdrant-client==1.9.2" "grpcio>=1.60.0" >/dev/null

echo "Running Python qdrant-client E2E..."
python scripts/qdrantgrpc_e2e_python.py --host 127.0.0.1 --grpc-port "${GRPC_PORT}"

echo
echo "OK: Python qdrant-client E2E verification passed."

