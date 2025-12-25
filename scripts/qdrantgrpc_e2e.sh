#!/usr/bin/env bash
set -euo pipefail

# qdrantgrpc_e2e.sh
#
# End-to-end verification for NornicDB's Qdrant-compatible gRPC endpoint.
#
# What it verifies (high level):
# - Core server starts with Qdrant gRPC enabled (feature flag)
# - Collections: create/info/list/update/exists/delete
# - Points: upsert/get/search/search_batch/scroll/count/recommend/payload ops/vector ops/delete
# - Snapshots: create/list/delete (writes snapshot artifacts)
#
# Requirements:
# - Go toolchain installed
# - Run from the repo root (or execute this script directly)
#
# Important:
# - To allow Qdrant clients to manage vectors via gRPC, set:
#     NORNICDB_EMBEDDING_ENABLED=false
#   If NORNICDB-managed embeddings are enabled (true), vector mutation endpoints
#   return FailedPrecondition to avoid conflicting sources of truth.

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

DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nornicdb-qdrantgrpc-e2e.XXXXXX")"
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

# Allow gRPC clients to manage vectors (embeddings) directly.
export NORNICDB_EMBEDDING_ENABLED=false

# Keep startup minimal for the E2E run.
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

echo "Running gRPC E2E client..."
go run -tags qdrantgrpc_e2e ./scripts/qdrantgrpc_e2e_client.go --addr "127.0.0.1:${GRPC_PORT}"

echo
echo "OK: Qdrant gRPC E2E verification passed."
