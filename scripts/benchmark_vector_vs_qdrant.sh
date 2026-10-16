#!/usr/bin/env bash
#
# benchmark_vector_vs_qdrant.sh
#
# Serialized vector ANN benchmark, NornicDB vs Qdrant, on a corpus of real documentation and code:
#
#   1. Build NornicDB from this checkout (noui, nolocalllm) and fetch the pinned Qdrant release
#      binary (checksum verified). Both run natively, never the locally installed NornicDB.
#   2. Build the corpus once (testing/benchmarks/vector_ann/corpus.py): chunk the tracked Markdown
#      and source files of VECTOR_CORPUS_ROOTS, embed them (Titan V2 on Bedrock by default; cached
#      by chunk content), hold out queries and compute exact ground truth. Reused while present.
#   3. Run every mode (testing/benchmarks/vector_ann/run.py): per engine and mode, a fresh server on
#      a wiped data directory, the corpus loaded over the Qdrant gRPC API, recall@K against exact
#      ground truth, single-client latency, concurrent throughput and an hnsw_ef sweep. One server
#      at a time. A mode with no equivalent on an engine is reported N/A.
#   4. Write the reports (testing/benchmarks/vector_ann/report.py):
#        reports/<timestamp>/comparison.md   NornicDB vs Qdrant per mode, ef sweeps, footprint
#        reports/<timestamp>/nornicdb.md     every NornicDB run
#        reports/<timestamp>/qdrant.md       every Qdrant run
#
# Modes (run.py MODES):
#   exact            NornicDB CPU brute force          | Qdrant exact=true
#   hnsw-fast        NornicDB ANN quality fast         | Qdrant HNSW m=16 ef_construct=100
#   hnsw-balanced    NornicDB ANN quality balanced     | Qdrant HNSW m=16 ef_construct=200
#   hnsw-accurate    NornicDB ANN quality accurate     | Qdrant HNSW m=32 ef_construct=400
#   ivf-hnsw         NornicDB k-means routing (IVF)    | N/A
#   ivf-pq           NornicDB compressed (IVF-PQ)      | N/A
#   sq-int8          N/A                               | Qdrant HNSW + scalar int8 quantization
#   pq-x16           N/A                               | Qdrant HNSW + product quantization x16
#   bq               N/A                               | Qdrant HNSW + binary quantization
#
# Configuration via env (with defaults):
#   VECTOR_CORPUS_ROOTS       colon-separated git repositories to chunk (default: this repository)
#   VECTOR_CORPUS_DIR         corpus directory (default bench-data/vector_ann/corpus)
#   VECTOR_CORPUS_MAX_CHUNKS  sample at most this many chunks (default 100000; 0 = all)
#   VECTOR_EMBEDDER           titan (Amazon Titan Text Embeddings V2; AWS credentials from the
#                             usual chain, e.g. AWS_PROFILE, and AWS_REGION, default us-east-1)
#   VECTOR_QUERIES=1000       held-out queries
#   VECTOR_K=10               recall@K and Search limit
#   VECTOR_WARMUP=100         warm-up queries per measurement (not recorded)
#   VECTOR_CONCURRENCY=8      concurrent clients for the throughput measurement
#   VECTOR_EF_SWEEP           hnsw_ef values swept on HNSW modes (default 16,32,64,128,256,512)
#   VECTOR_MODES              modes to run (default: all)
#   VECTOR_ENGINES            nornicdb,qdrant (default both)
#   NORNIC_GPU=0              1 leaves NornicDB's GPU brute force and GPU HNSW build on
#   QDRANT_VERSION=v1.19.2    pinned Qdrant release (QDRANT_SHA256 must match its archive)
#   REPORT_DIR                parent of the timestamped reports (default testing/benchmarks/vector_ann/reports)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

BENCH_DIR="testing/benchmarks/vector_ann"
WORK_DIR="${VECTOR_WORK_DIR:-bench-data/vector_ann}"
VECTOR_CORPUS_DIR="${VECTOR_CORPUS_DIR:-${WORK_DIR}/corpus}"
VECTOR_CORPUS_ROOTS="${VECTOR_CORPUS_ROOTS:-${REPO_ROOT}}"
QDRANT_VERSION="${QDRANT_VERSION:-v1.19.2}"
QDRANT_SHA256="${QDRANT_SHA256:-d24dfd25f4584684b337dd30ee016472cd25f53fc2501f1cd058ab3d7ee0b019}"
REPORT_DIR="${REPORT_DIR:-${BENCH_DIR}/reports}"
STAMP="$(date +%Y%m%d_%H%M%S)"
OUT="${REPORT_DIR}/${STAMP}"

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) QDRANT_ASSET="qdrant-aarch64-apple-darwin.tar.gz" ;;
  Darwin-x86_64) QDRANT_ASSET="qdrant-x86_64-apple-darwin.tar.gz" ;;
  Linux-x86_64) QDRANT_ASSET="qdrant-x86_64-unknown-linux-musl.tar.gz" ;;
  Linux-aarch64) QDRANT_ASSET="qdrant-aarch64-unknown-linux-musl.tar.gz" ;;
  *) echo "unsupported platform $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

mkdir -p "${WORK_DIR}" "${OUT}"

log "building NornicDB (noui, nolocalllm)"
NORNIC_BIN="${WORK_DIR}/nornicdb"
go build -tags "noui nolocalllm" -o "${NORNIC_BIN}" ./cmd/nornicdb

QDRANT_DIR="${WORK_DIR}/qdrant-${QDRANT_VERSION}"
QDRANT_BIN="${QDRANT_DIR}/qdrant"
if [[ ! -x "${QDRANT_BIN}" ]]; then
  log "fetching Qdrant ${QDRANT_VERSION} (${QDRANT_ASSET})"
  mkdir -p "${QDRANT_DIR}"
  curl -fsSL -o "${QDRANT_DIR}/qdrant.tar.gz" \
    "https://github.com/qdrant/qdrant/releases/download/${QDRANT_VERSION}/${QDRANT_ASSET}"
  echo "${QDRANT_SHA256}  ${QDRANT_DIR}/qdrant.tar.gz" | shasum -a 256 -c - >/dev/null
  tar -xzf "${QDRANT_DIR}/qdrant.tar.gz" -C "${QDRANT_DIR}"
fi

VENV="${WORK_DIR}/venv"
if [[ ! -x "${VENV}/bin/python" ]]; then
  log "creating the Python environment"
  python3 -m venv "${VENV}"
  "${VENV}/bin/pip" install -q -r "${BENCH_DIR}/requirements.txt"
fi
PY="${VENV}/bin/python"

if [[ ! -f "${VECTOR_CORPUS_DIR}/meta.json" ]]; then
  log "building the corpus from ${VECTOR_CORPUS_ROOTS}"
  "${PY}" "${BENCH_DIR}/corpus.py" --roots "${VECTOR_CORPUS_ROOTS}" --out "${VECTOR_CORPUS_DIR}" \
    --queries "${VECTOR_QUERIES:-1000}"
else
  log "reusing the corpus in ${VECTOR_CORPUS_DIR}"
fi

RUN_ARGS=(--corpus "${VECTOR_CORPUS_DIR}" --out "${OUT}" --data-root "${WORK_DIR}/data"
          --nornic-bin "${NORNIC_BIN}" --qdrant-bin "${QDRANT_BIN}"
          --queries "${VECTOR_QUERIES:-1000}" --k "${VECTOR_K:-10}" --warmup "${VECTOR_WARMUP:-100}"
          --concurrency "${VECTOR_CONCURRENCY:-8}" --ef-sweep "${VECTOR_EF_SWEEP:-16,32,64,128,256,512}"
          --engines "${VECTOR_ENGINES:-nornicdb,qdrant}")
[[ -n "${VECTOR_MODES:-}" ]] && RUN_ARGS+=(--modes "${VECTOR_MODES}")
[[ "${NORNIC_GPU:-0}" == "1" ]] && RUN_ARGS+=(--nornic-gpu)

log "running the benchmark into ${OUT}"
"${PY}" -u "${BENCH_DIR}/run.py" "${RUN_ARGS[@]}" 2>&1 | tee "${OUT}/run.log"

cp "${VECTOR_CORPUS_DIR}/meta.json" "${OUT}/corpus.json"
"${PY}" "${BENCH_DIR}/report.py" --results "${OUT}" --corpus "${VECTOR_CORPUS_DIR}"
log "done: ${OUT}/comparison.md"
