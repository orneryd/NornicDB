#!/usr/bin/env bash
#
# benchmark_northwind_vs_neo4j.sh
#
# Serialized Northwind benchmark for NornicDB, Neo4j, FalkorDB, Memgraph and
# LadybugDB (embedded, Kuzu fork):
#
#   0. Wipe every engine's data directory up front so each run starts from a
#      fresh store. Any stale Neo4j JVM is SIGKILL'd before the wipe.
#   1. Start NornicDB, sample powermetrics during seed+benchmark,
#      measure on-disk data size, stop NornicDB.
#      Step 1 runs once per NornicDB parser mode (NORNIC_PARSER_MODES, default
#      "nornic antlr"), each from a freshly wiped data directory and serialized
#      like every other run. The default (nornic) mode keeps the historical
#      `nornicdb.*` file names; the ANTLR mode writes `nornicdb-antlr.*`.
#   2. Start local Neo4j, sample powermetrics during seed+benchmark,
#      measure on-disk data size, stop Neo4j.
#   3. Start FalkorDB (docker), sample powermetrics, measure data dir, stop it.
#   4. Start Memgraph (docker), same isolated envelope.
#   5. Run LadybugDB embedded in the benchmark runner against a fresh data
#      directory, same isolated envelope.
#   6. Generate the Markdown reports:
#        - reports/<timestamp>/<engine>.md for every engine that ran
#        - reports/<timestamp>/comparison.md      (NornicDB vs Neo4j)
#        - reports/<timestamp>/parser-modes.md    (default vs ANTLR)
#        - reports/<timestamp>/sweep.md           (all engines in one table)
#
# Requires: sudo (for powermetrics), Neo4j installed locally (brew install neo4j),
# docker (for FalkorDB/Memgraph), Go toolchain, Python 3. Invokes `sudo -v` up
# front so powermetrics can run non-interactively.
#
# Configuration via env (with defaults):
#   ITERATIONS=30           iterations per query (per-DB, per-query)
#   WARMUP=5                warmup iterations (not recorded)
#   BATCH_SIZE=500          rows per UNWIND seed batch
#   SEED_PARALLEL=4         concurrent Bolt sessions per seed phase
#   PRODUCTS=2000           products in seed
#   ORDERS=2000             orders in seed
#   NORNIC_DATA_DIR         NornicDB data dir (default ./bench-data/nornic)
#   NEO4J_HOME              Neo4j install dir (default /opt/homebrew/opt/neo4j)
#   NEO4J_DATA_DIR          Neo4j data dir (default /opt/homebrew/var/neo4j/data)
#   NEO4J_PASSWORD          Neo4j password (default "testpass123")
#   FALKOR_IMAGE            FalkorDB docker image (default falkordb/falkordb:latest)
#   FALKOR_BOLT_PORT        host Bolt port for FalkorDB (default 17688)
#   FALKOR_USER/FALKOR_PASS FalkorDB Bolt credentials (default falkordb/falkordb;
#                           set FALKOR_AUTH=none to use no-auth Bolt)
#   FALKOR_DATA_DIR         FalkorDB data dir (default ./bench-data/falkor)
#   MEMGRAPH_IMAGE          Memgraph docker image (default memgraph/memgraph:latest)
#   MEMGRAPH_BOLT_PORT      host Bolt port for Memgraph (default 17689)
#   MEMGRAPH_DATA_DIR       Memgraph data dir (default ./bench-data/memgraph)
#   LADYBUG_DATA_DIR        LadybugDB data dir (default ./bench-data/ladybug)
#   LADYBUG_LIB_DIR         LadybugDB precompiled lib dir (default ./lib-ladybug)
#   REPORT_DIR              Parent dir for timestamped reports (default scripts/benchmark_reports)
#   NORNIC_PARSER_MODES     NornicDB parser modes to benchmark, in order (default
#                           "nornic antlr"). Run "antlr nornic" as well to see
#                           whether run order biases the comparison.
#   SKIP_POWERMETRICS=1     do not sample power (no sudo needed); power rows read 0.
#   SKIP_NEO4J=1            benchmark NornicDB only (Neo4j is neither required nor run);
#                           the parser-mode report is still generated.
#   SKIP_FALKOR=1           skip the FalkorDB run (image neither pulled nor required).
#   SKIP_MEMGRAPH=1         skip the Memgraph run.
#   SKIP_LADYBUG=1          skip the embedded LadybugDB run (no library download).
#
# CLI flags (for testing a single engine without running the rest of the sweep):
#   --falkor-only           run only the FalkorDB phase
#   --memgraph-only         run only the Memgraph phase
#   --ladybug-only          run only the embedded LadybugDB phase
#
#   GRAPH_ONLY=1            (default 1) Disable BM25 fulltext + vector ANN index
#                           build/maintenance for the NornicDB run via the per-DB
#                           --search-bm25-enabled=false / --search-vector-enabled=false
#                           startup flags, and disable memory-decay access scoring
#                           via NORNICDB_MEMORY_DECAY_ENABLED=false. The Northwind
#                           benchmark performs no text/vector search and no
#                           decay-aware reads, so leaving these on costs seed-time
#                           CPU on every node/edge access (decay scoring was
#                           profiled at ~29% of seed wall time when enabled)
#                           without affecting query results. Set GRAPH_ONLY=0 to
#                           include search index build + decay scoring cost in the
#                           comparison.
#   NORNIC_ASYNC_WRITES=1   (default 1) Enable the adaptive write-behind commit
#                           buffer for the NornicDB run
#                           (NORNICDB_ASYNC_WRITES_ENABLED=true): committed
#                           statements are acknowledged in memory and replayed
#                           by a background flusher whose rotation delay and
#                           buffer size auto-scale to the measured drain latency
#                           and throughput. Durable synchronous commits are the
#                           product default; set NORNIC_ASYNC_WRITES=0 to benchmark
#                           the durable baseline instead.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ITERATIONS="${ITERATIONS:-30}"
WARMUP="${WARMUP:-5}"
CATEGORIES="${CATEGORIES:-96}"
SUPPLIERS="${SUPPLIERS:-144}"
CUSTOMERS="${CUSTOMERS:-1200}"
PRODUCTS="${PRODUCTS:-48000}"
ORDERS="${ORDERS:-48000}"
ORDER_LINES_MIN="${ORDER_LINES_MIN:-1}"
ORDER_LINES_MAX="${ORDER_LINES_MAX:-6}"
# Batch sweep at 48K products/48K orders (2026-10-10, M2 Max, write-behind on):
#   500: 7.43s  1000: 8.03s  2000: 7.41s (best)  4000: 8.55s  8000: 9.63s  12000: 11.43s
# Larger batches contend on the server write pipeline; 2000 is the sweet spot.
BATCH_SIZE="${BATCH_SIZE:-2000}"
SEED_PARALLEL="${SEED_PARALLEL:-4}"
SEED="${SEED:-42}"
NORNIC_DATA_DIR="${NORNIC_DATA_DIR:-${REPO_ROOT}/bench-data/nornic}"
NEO4J_HOME="${NEO4J_HOME:-/opt/homebrew/opt/neo4j}"
NEO4J_DATA_DIR="${NEO4J_DATA_DIR:-/opt/homebrew/var/neo4j/data}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-testpass123}"
REPORT_PARENT="${REPORT_DIR:-${SCRIPT_DIR}/benchmark_reports}"
GRAPH_ONLY="${GRAPH_ONLY:-1}"
NORNIC_PARSER_MODES="${NORNIC_PARSER_MODES:-nornic antlr}"
SKIP_POWERMETRICS="${SKIP_POWERMETRICS:-0}"
SKIP_NEO4J="${SKIP_NEO4J:-0}"
FALKOR_IMAGE="${FALKOR_IMAGE:-falkordb/falkordb:latest}"
# FalkorDB v6+ serves queries over native RESP (the production protocol); the
# experimental Bolt listener of the legacy C engine no longer exists, so the
# sweep talks RESP via the official falkordb-go client.
FALKOR_PORT="${FALKOR_PORT:-17690}"
FALKOR_AUTH="${FALKOR_AUTH:-none}"
FALKOR_USER="${FALKOR_USER:-falkordb}"
FALKOR_PASS="${FALKOR_PASS:-falkordb}"
FALKOR_DATA_DIR="${FALKOR_DATA_DIR:-${REPO_ROOT}/bench-data/falkor}"
FALKOR_CONTAINER_DATA_DIR="${FALKOR_CONTAINER_DATA_DIR:-/data}"
FALKOR_DATABASE="${FALKOR_DATABASE:-falkor}"
MEMGRAPH_IMAGE="${MEMGRAPH_IMAGE:-memgraph/memgraph:latest}"
MEMGRAPH_BOLT_PORT="${MEMGRAPH_BOLT_PORT:-17689}"
MEMGRAPH_DATA_DIR="${MEMGRAPH_DATA_DIR:-${REPO_ROOT}/bench-data/memgraph}"
MEMGRAPH_DATABASE="${MEMGRAPH_DATABASE:-memgraph}"
LADYBUG_DATA_DIR="${LADYBUG_DATA_DIR:-${REPO_ROOT}/bench-data/ladybug}"
LADYBUG_LIB_DIR="${LADYBUG_LIB_DIR:-${REPO_ROOT}/lib-ladybug}"
LADYBUG_BENCH_BIN="${LADYBUG_BENCH_BIN:-${REPO_ROOT}/northwind_power_bench_ladybug}"
SKIP_FALKOR="${SKIP_FALKOR:-0}"
SKIP_MEMGRAPH="${SKIP_MEMGRAPH:-0}"
SKIP_LADYBUG="${SKIP_LADYBUG:-0}"

# Per-engine-only CLI flags: run exactly one phase for quick isolated testing.
# Any env-provided SKIP_* / parser-mode settings are overridden so the selected
# engine is the only thing that runs.
ONLY_ENGINE=""
for arg in "$@"; do
  case "${arg}" in
    --falkor-only)   ONLY_ENGINE="falkor" ;;
    --memgraph-only) ONLY_ENGINE="memgraph" ;;
    --ladybug-only)  ONLY_ENGINE="ladybug" ;;
    -*)              die "unknown option: ${arg}" ;;
    *)               die "unexpected argument: ${arg}" ;;
  esac
done
if [[ -n "${ONLY_ENGINE}" ]]; then
  NORNIC_PARSER_MODES=""
  SKIP_NEO4J=1
  SKIP_FALKOR=1
  SKIP_MEMGRAPH=1
  SKIP_LADYBUG=1
  case "${ONLY_ENGINE}" in
    falkor)   SKIP_FALKOR=0 ;;
    memgraph) SKIP_MEMGRAPH=0 ;;
    ladybug)  SKIP_LADYBUG=0 ;;
  esac
fi
for mode in ${NORNIC_PARSER_MODES}; do
  case "${mode}" in
    nornic|antlr) ;;
    *) printf 'NORNIC_PARSER_MODES entries must be "nornic" or "antlr", got %q\n' "${mode}" >&2; exit 1 ;;
  esac
done

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
REPORT_DIR="${REPORT_PARENT}/${TIMESTAMP}"
mkdir -p "${REPORT_DIR}"

NORNIC_BIN="${REPO_ROOT}/nornicdb"
BENCH_BIN="${REPO_ROOT}/northwind_power_bench"
NORNIC_HTTP_PORT=17474
NORNIC_BOLT_PORT=17687
NORNIC_DATABASE="${NORNIC_DATABASE:-nornic}"
NEO4J_BOLT_PORT=7687
NEO4J_HTTP_PORT=7474
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"

log() { printf '[\033[36m%s\033[0m] %s\n' "$(date +%H:%M:%S)" "$*"; }
err() { printf '[\033[31m%s\033[0m] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { err "$@"; exit 1; }

cleanup() {
  local rc=$?
  set +e
  if [[ -n "${NORNIC_PID:-}" ]] && kill -0 "${NORNIC_PID}" 2>/dev/null; then
    log "cleanup: killing NornicDB (pid ${NORNIC_PID})"
    kill -KILL "${NORNIC_PID}" 2>/dev/null || true
    # Reap the job so bash doesn't report "Killed: 9" at exit.
    wait "${NORNIC_PID}" 2>/dev/null || true
  fi
  if [[ -n "${POWER_PID:-}" ]]; then
    sudo kill -KILL "${POWER_PID}" 2>/dev/null || true
    wait "${POWER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${VMSTAT_PID:-}" ]]; then
    kill -KILL "${VMSTAT_PID}" 2>/dev/null || true
    wait "${VMSTAT_PID}" 2>/dev/null || true
  fi
  if [[ -n "${NEO4J_PID:-}" ]] && kill -0 "${NEO4J_PID}" 2>/dev/null; then
    log "cleanup: killing Neo4j (pid ${NEO4J_PID})"
    kill -KILL "${NEO4J_PID}" 2>/dev/null || true
    wait "${NEO4J_PID}" 2>/dev/null || true
  fi
  for container in northwind-falkor northwind-memgraph; do
    if command -v docker >/dev/null 2>&1 && docker ps -a --format '{{.Names}}' | grep -qx "${container}"; then
      log "cleanup: removing container ${container}"
      docker rm -f "${container}" >/dev/null 2>&1 || true
    fi
  done
  exit "$rc"
}
trap cleanup EXIT INT TERM

require() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
require go
require python3
require lsof
require nc
if [[ "${SKIP_POWERMETRICS}" != "1" ]]; then
  require sudo
  [[ -x /usr/bin/powermetrics ]] || die "/usr/bin/powermetrics not found"
fi
if [[ "${SKIP_NEO4J}" != "1" ]]; then
  require "${NEO4J_HOME}/bin/neo4j"
  CYPHER_SHELL="${CYPHER_SHELL:-$(command -v cypher-shell || true)}"
  [[ -x "${CYPHER_SHELL}" ]] || die "cypher-shell not found on PATH (set CYPHER_SHELL=/path/to/cypher-shell)"
fi

log "config: iterations=${ITERATIONS} warmup=${WARMUP}"
log "config: seed_batch_size=${BATCH_SIZE} seed_parallel=${SEED_PARALLEL}"
log "config: categories=${CATEGORIES} suppliers=${SUPPLIERS} customers=${CUSTOMERS}"
log "config: products=${PRODUCTS} orders=${ORDERS} order_lines=${ORDER_LINES_MIN}..${ORDER_LINES_MAX} seed=${SEED}"
log "config: report_dir=${REPORT_DIR}"
log "config: nornicdb parser modes=${NORNIC_PARSER_MODES} skip_powermetrics=${SKIP_POWERMETRICS} skip_neo4j=${SKIP_NEO4J}"
log "config: skip_falkor=${SKIP_FALKOR} skip_memgraph=${SKIP_MEMGRAPH} skip_ladybug=${SKIP_LADYBUG}"
if [[ -n "${ONLY_ENGINE}" ]]; then
  log "only-engine mode: running ${ONLY_ENGINE} and skipping every other phase"
fi
if [[ "${GRAPH_ONLY}" == "1" ]]; then
  log "config: GRAPH_ONLY=1 — NornicDB will run with BM25 + vector indexes disabled (graph-only mode)"
else
  log "config: GRAPH_ONLY=0 — NornicDB will run with BM25 + vector indexes enabled (default mode)"
fi

if [[ "${SKIP_POWERMETRICS}" == "1" ]]; then
  log "SKIP_POWERMETRICS=1 — not sampling power, no sudo needed"
elif [[ $EUID -ne 0 ]]; then
  log "priming sudo for powermetrics (single prompt up front)…"
  sudo -v
  # Keep the sudo timestamp alive while the script runs.
  ( while true; do sudo -n true 2>/dev/null; sleep 50; done ) &
  SUDO_KEEPALIVE_PID=$!
  trap 'kill ${SUDO_KEEPALIVE_PID} 2>/dev/null || true; cleanup' EXIT INT TERM
else
  log "running as root — skipping sudo prime"
fi

wipe_nornic_data_dir() {
  if nc -z 127.0.0.1 "${NORNIC_BOLT_PORT}" 2>/dev/null; then
    die "NornicDB benchmark Bolt port ${NORNIC_BOLT_PORT} is already in use"
  fi
  # Never remove an open database directory. A live server keeps its WAL file
  # descriptor after rm -rf, then fails the next snapshot or write.
  if [[ -d "${NORNIC_DATA_DIR}" ]] && lsof -nP +D "${NORNIC_DATA_DIR}" 2>/dev/null | awk 'NR > 1 {found=1} END {exit !found}'; then
    die "NornicDB data directory is open by another process: ${NORNIC_DATA_DIR}"
  fi
  # NornicDB: remove the whole data dir. If a prior run left it root-owned
  # (sudo invocation), fall through to a sudo rm so the wipe actually succeeds.
  if [[ -d "${NORNIC_DATA_DIR}" ]]; then
    if ! rm -rf "${NORNIC_DATA_DIR}" 2>/dev/null; then
      sudo rm -rf "${NORNIC_DATA_DIR}"
    fi
  fi
  mkdir -p "${NORNIC_DATA_DIR}"
}

log "wiping data directories before run (NornicDB + Neo4j databases/transactions)…"
if [[ "${SKIP_NEO4J}" != "1" ]] && pgrep -f 'org\.neo4j\.server\.' >/dev/null 2>&1; then
  die "Neo4j is running; stop it before benchmarking (its data directory would be wiped)"
fi
# Neo4j listens on its default ports. Anything already there (typically the
# local NornicDB server, which uses the same 7474/7687) would pass the script's
# "port is up" readiness check and get benchmarked as if it were Neo4j, so fail
# now rather than after the NornicDB runs.
if [[ "${SKIP_NEO4J}" != "1" ]]; then
  for port in "${NEO4J_BOLT_PORT}" "${NEO4J_HTTP_PORT}"; do
    if nc -z 127.0.0.1 "${port}" 2>/dev/null; then
      die "port ${port} is in use, but the Neo4j phase needs it. Stop whatever holds it (is a local NornicDB or Neo4j server running? try: lsof -nP -iTCP:${port} -sTCP:LISTEN), or set SKIP_NEO4J=1."
    fi
  done
fi
wipe_nornic_data_dir

# Neo4j: remove the ephemeral `databases/` + `transactions/` subtrees (leave
# the parent alone so the brew-managed config/logs directories persist).
if [[ "${SKIP_NEO4J}" != "1" && -x "${NEO4J_HOME}/bin/neo4j" ]]; then
  if [[ -d "${NEO4J_DATA_DIR}/databases" || -d "${NEO4J_DATA_DIR}/transactions" ]]; then
    if ! rm -rf "${NEO4J_DATA_DIR}/databases" "${NEO4J_DATA_DIR}/transactions" 2>/dev/null; then
      sudo rm -rf "${NEO4J_DATA_DIR}/databases" "${NEO4J_DATA_DIR}/transactions"
    fi
  fi
fi

log "building nornicdb binary…"
go build -o "${NORNIC_BIN}" ./cmd/nornicdb

log "building northwind benchmark runner…"
go build -o "${BENCH_BIN}" ./testing/benchmarks/northwind_power

# ------------------------------------------------------------------------
# Power sampling helpers — powermetrics runs in background, dumps plist every
# second into a log file. Parser extracts CPU/GPU/ANE/combined power
# averages in milliwatts.
# ------------------------------------------------------------------------

start_powermetrics() {
  local log_file="$1"
  # Interval 1s, plist format for robust parsing.
  sudo /usr/bin/powermetrics \
    --samplers cpu_power,gpu_power \
    -i 1000 \
    -f plist \
    -o "${log_file}" \
    >/dev/null 2>&1 &
  echo $!
}

stop_powermetrics() {
  local pid="$1"
  # powermetrics flushes its plist on SIGINT (graceful, needed to get final
  # sample into the file). Give it a short grace window, then SIGKILL.
  sudo kill -INT "${pid}" 2>/dev/null || true
  for _ in {1..12}; do
    kill -0 "${pid}" 2>/dev/null || return 0
    sleep 0.25
  done
  sudo kill -KILL "${pid}" 2>/dev/null || true
}

# Memory sampling helpers — vm_stat runs in background at 1-second intervals,
# dumping page counts. The Python report parser reads these to compute peak
# and average memory pressure.

start_vmstat() {
  local log_file="$1"
  vm_stat 1 > "${log_file}" 2>&1 &
  echo $!
}

stop_vmstat() {
  local pid="$1"
  kill -INT "${pid}" 2>/dev/null || true
  for _ in {1..6}; do
    kill -0 "${pid}" 2>/dev/null || return 0
    sleep 0.25
  done
  kill -KILL "${pid}" 2>/dev/null || true
}

# Hard-kill a PID and wait for it to actually disappear from the process
# table. Used instead of SIGTERM+wait because `wait` blocks indefinitely
# when a process ignores SIGTERM or stalls on flush/shutdown.
kill_pid() {
  local pid="$1"
  kill -KILL "${pid}" 2>/dev/null || true
  for _ in {1..40}; do
    kill -0 "${pid}" 2>/dev/null || return 0
    sleep 0.25
  done
  err "pid ${pid} still alive after SIGKILL"
}

# stop_pid_graceful sends SIGTERM, waits up to `timeout` seconds for the
# process to exit (so the storage layer can flush its in-memory memtables,
# compact pending writes, and fsync — critical for a meaningful on-disk
# size measurement), then falls back to SIGKILL if the deadline passes.
#
# Use this (NOT kill_pid) when the process owns a write cache that only
# lands on disk during its shutdown handler (e.g. BadgerDB memtable flush,
# WAL sync, value-log rewrite). SIGKILL leaves the data directory bloated
# with preallocated memtables / unflushed vlog entries, producing
# misleadingly large `du` readings.
stop_pid_graceful() {
  local pid="$1"
  local timeout="${2:-20}"
  kill -TERM "${pid}" 2>/dev/null || true
  local waited=0
  while kill -0 "${pid}" 2>/dev/null; do
    if (( waited >= timeout )); then
      err "pid ${pid} did not exit within ${timeout}s of SIGTERM — forcing SIGKILL (disk size may be inflated)"
      kill -KILL "${pid}" 2>/dev/null || true
      sleep 1
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
}

# ------------------------------------------------------------------------
# NornicDB run
# ------------------------------------------------------------------------

NORNIC_RUNS_DONE=0

# run_nornic <mode>   mode: nornic (the default parser) | antlr
#
# Each mode is an independent, serialized run from a freshly wiped data
# directory, so neither mode inherits the other's warmed caches or data. The
# default mode keeps the historical `nornicdb.*` file names so existing reports
# and tooling are unchanged; the others write `nornicdb-<mode>.*`.
run_nornic() {
  local mode="${1:-nornic}"
  local label="nornicdb"
  [[ "${mode}" == "nornic" ]] || label="nornicdb-${mode}"
  log "=== NornicDB run (parser=${mode}, label=${label}) ==="
  # The data dir was wiped at script start; later runs wipe it again.
  if (( NORNIC_RUNS_DONE > 0 )); then
    log "wiping NornicDB data directory for a clean ${mode} run"
    wipe_nornic_data_dir
  fi
  NORNIC_RUNS_DONE=$((NORNIC_RUNS_DONE + 1))
  mkdir -p "${NORNIC_DATA_DIR}"

  # Powermetrics wraps the entire DB lifecycle — startup, seed, benchmark,
  # shutdown — so the report captures the full energy envelope, not just the
  # query window.
  if [[ "${SKIP_POWERMETRICS}" != "1" ]]; then
    log "starting powermetrics sampler (covers startup + benchmark + shutdown)"
    POWER_PID=$(start_powermetrics "${REPORT_DIR}/${label}.powermetrics.plist")
  fi
  VMSTAT_PID=$(start_vmstat "${REPORT_DIR}/${label}.vmstat.log")
  local t0=$(date +%s.%N)

  # Optional graph-only mode: disable BM25 + vector index build at startup
  # and memory-decay access scoring. The Northwind benchmark performs zero
  # text/vector search and no decay-aware reads, so these are pure overhead
  # on every node/edge access. Toggle via GRAPH_ONLY (default 1); set
  # GRAPH_ONLY=0 to include the index-build + decay-scoring cost.
  local nornic_extra_flags=()
  local nornic_extra_env=()
  if [[ "${GRAPH_ONLY}" == "1" ]]; then
    nornic_extra_flags=(
      --search-bm25-enabled=false
      --search-vector-enabled=false
    )
    # Decay has no CLI flag; the env var is the only switch. Profile-verified:
    # decay scoring costs ~29% of seed wall time when enabled (11.7s -> 8.3s
    # for the default seed size) via per-entity AccessMeta reads + scoring.
    nornic_extra_env=(NORNICDB_MEMORY_DECAY_ENABLED=false)
  fi
  # The benchmark measures the auto-scaling write-behind path: committed
  # statements are acknowledged in memory and replayed by a background
  # flusher whose rotation delay and buffer size adapt to measured drain
  # latency and throughput (docs/performance/write-behind-adaptive-buffer.md).
  # Durable synchronous commits are the product default; NORNIC_ASYNC_WRITES=0
  # benchmarks that baseline instead.
  if [[ "${NORNIC_ASYNC_WRITES:-1}" != "0" ]]; then
    nornic_extra_env+=(NORNICDB_ASYNC_WRITES_ENABLED=true)
  fi

  log "starting NornicDB (bolt=${NORNIC_BOLT_PORT} http=${NORNIC_HTTP_PORT}) graph_only=${GRAPH_ONLY} parser=${mode}"
  # Note: ${arr[@]+"${arr[@]}"} guards against `set -u` tripping on an empty
  # array expansion. macOS ships bash 3.2 which is strict here. Extra env
  # vars ride through `env` because bash does NOT recognize a word produced
  # by expansion as an assignment prefix — it would run it as a command.
  # NORNICDB_PARSER is set explicitly for every run so an ambient value in the
  # caller's environment can never leak into the "default" mode.
  NORNICDB_NO_AUTH=true NORNICDB_EMBEDDING_ENABLED=false \
    env NORNICDB_PARSER="${mode}" ${nornic_extra_env[@]+"${nornic_extra_env[@]}"} \
    "${NORNIC_BIN}" serve \
      --bolt-port "${NORNIC_BOLT_PORT}" \
      --http-port "${NORNIC_HTTP_PORT}" \
      --data-dir "${NORNIC_DATA_DIR}" \
      --no-auth \
      ${nornic_extra_flags[@]+"${nornic_extra_flags[@]}"} \
      >"${REPORT_DIR}/${label}.stdout.log" 2>"${REPORT_DIR}/${label}.stderr.log" &
  NORNIC_PID=$!

  for i in {1..30}; do
    if nc -z 127.0.0.1 "${NORNIC_BOLT_PORT}" 2>/dev/null; then break; fi
    sleep 1
    if ! kill -0 "${NORNIC_PID}" 2>/dev/null; then
      die "NornicDB crashed on startup; see ${REPORT_DIR}/${label}.stderr.log"
    fi
  done
  nc -z 127.0.0.1 "${NORNIC_BOLT_PORT}" 2>/dev/null || die "NornicDB bolt port never came up"
  log "NornicDB ready (pid ${NORNIC_PID})"

  "${BENCH_BIN}" \
    -uri "bolt://localhost:${NORNIC_BOLT_PORT}" \
    -no-auth \
    -database "${NORNIC_DATABASE}" \
    -categories "${CATEGORIES}" \
    -suppliers "${SUPPLIERS}" \
    -customers "${CUSTOMERS}" \
    -products "${PRODUCTS}" \
    -orders "${ORDERS}" \
    -order-lines-min "${ORDER_LINES_MIN}" \
    -order-lines-max "${ORDER_LINES_MAX}" \
    -batch-size "${BATCH_SIZE}" \
    -parallel "${SEED_PARALLEL}" \
    -seed "${SEED}" \
    -iterations "${ITERATIONS}" \
    -warmup "${WARMUP}" \
    -label "${label}" \
    -out "${REPORT_DIR}/${label}.results.json" \
    2>"${REPORT_DIR}/${label}.bench.log" || die "NornicDB (${mode}) benchmark failed — see ${REPORT_DIR}/${label}.bench.log"

  # Graceful shutdown so BadgerDB gets a chance to flush its in-memory
  # memtables and compact/rewrite vlog segments. Without this the
  # subsequent `du` measurement includes a full 8 MiB preallocated
  # memtable and every write still sitting in the memtable — producing
  # absurd sizes like 50+ MiB for 10 kB of actual data.
  log "stopping NornicDB gracefully (SIGTERM, flushing storage)"
  stop_pid_graceful "${NORNIC_PID}" 30
  NORNIC_PID=""

  local t1=$(date +%s.%N)
  if [[ -n "${POWER_PID:-}" ]]; then
    log "stopping powermetrics sampler"
    stop_powermetrics "${POWER_PID}"
    POWER_PID=""
  fi
  stop_vmstat "${VMSTAT_PID}"
  VMSTAT_PID=""

  python3 -c "print(f'{float(${t1}) - float(${t0}):.3f}')" > "${REPORT_DIR}/${label}.wall_seconds.txt"

  # Flush OS page cache for this directory before measuring so `du` sees
  # what actually landed in the filesystem.
  sync
  log "measuring NornicDB on-disk size"
  du -sk "${NORNIC_DATA_DIR}" | awk '{print $1 * 1024}' > "${REPORT_DIR}/${label}.disk_bytes.txt"
  du -sh "${NORNIC_DATA_DIR}" > "${REPORT_DIR}/${label}.disk_human.txt" || true
  echo "${NORNIC_DATA_DIR}" > "${REPORT_DIR}/${label}.data_dir.txt"

  log "NornicDB run complete (parser=${mode})"
}

# ------------------------------------------------------------------------
# Neo4j run
# ------------------------------------------------------------------------

configure_neo4j_password() {
  # If NEO4J_AUTH is unchanged (default 'neo4j/neo4j'), first login is forced
  # to change password. Use neo4j-admin dbms set-initial-password for
  # non-interactive setup on a fresh DB. Safe to re-run; if already set, it's
  # a no-op returning nonzero which we tolerate.
  #
  # Run under the same user account as neo4j itself so the auth file ends up
  # with the expected ownership.
  local owner
  owner=$(stat -f "%Su" "${NEO4J_HOME}")
  local prefix=()
  if [[ "$(whoami)" != "${owner}" ]]; then
    prefix=("sudo" "-u" "${owner}")
  fi
  ${prefix[@]+"${prefix[@]}"} "${NEO4J_HOME}/bin/neo4j-admin" dbms set-initial-password "${NEO4J_PASSWORD}" \
    >/dev/null 2>&1 || true
}

run_neo4j() {
  log "=== Neo4j run ==="

  # Neo4j refuses to run as root and has file-ownership checks that warn when
  # launched by a different user than the brew cellar owner. Resolve the
  # owning user of the install and always run under that account via sudo -u.
  local neo4j_owner
  neo4j_owner=$(stat -f "%Su" "${NEO4J_HOME}")
  NEO4J_RUN_PREFIX=("sudo" "-u" "${neo4j_owner}")
  if [[ "$(whoami)" == "${neo4j_owner}" ]]; then
    NEO4J_RUN_PREFIX=()
  fi

  if pgrep -f 'org\.neo4j\.server\.' >/dev/null 2>&1; then
    die "Neo4j started during the NornicDB phase; refusing to benchmark or stop a different server"
  fi

  # Data dir already wiped at script start (including any prior Neo4j
  # `databases/` + `transactions/` subtrees). Just re-set the password on
  # the fresh store.
  configure_neo4j_password

  # Powermetrics wraps the entire DB lifecycle.
  log "starting powermetrics sampler (covers startup + benchmark + shutdown)"
  POWER_PID=$(start_powermetrics "${REPORT_DIR}/neo4j.powermetrics.plist")
  VMSTAT_PID=$(start_vmstat "${REPORT_DIR}/neo4j.vmstat.log")
  local t0=$(date +%s.%N)

  log "starting Neo4j (as user ${neo4j_owner})"
  # Don't abort the script on nonzero exit — we poll for the Bolt port and
  # surface a useful error ourselves.
  set +e
  ${NEO4J_RUN_PREFIX[@]+"${NEO4J_RUN_PREFIX[@]}"} "${NEO4J_HOME}/bin/neo4j" start >"${REPORT_DIR}/neo4j.start.log" 2>&1
  local start_rc=$?
  set -e
  if (( start_rc != 0 )); then
    err "neo4j start returned ${start_rc} — see ${REPORT_DIR}/neo4j.start.log"
  fi

  for i in {1..60}; do
    if nc -z 127.0.0.1 "${NEO4J_BOLT_PORT}" 2>/dev/null; then break; fi
    sleep 1
  done
  nc -z 127.0.0.1 "${NEO4J_BOLT_PORT}" 2>/dev/null || die "Neo4j bolt port never came up — see ${REPORT_DIR}/neo4j.start.log"
  for i in {1..30}; do
    if "${CYPHER_SHELL}" -u neo4j -p "${NEO4J_PASSWORD}" -d neo4j "RETURN 1" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  # Capture the actual Java PID so cleanup can SIGKILL it directly.
  NEO4J_PID=$(pgrep -f "org\.neo4j\.server\." | head -1 || true)
  log "Neo4j ready (pid ${NEO4J_PID:-unknown})"

  "${BENCH_BIN}" \
    -uri "bolt://localhost:${NEO4J_BOLT_PORT}" \
    -user neo4j \
    -pass "${NEO4J_PASSWORD}" \
    -database "${NEO4J_DATABASE}" \
    -categories "${CATEGORIES}" \
    -suppliers "${SUPPLIERS}" \
    -customers "${CUSTOMERS}" \
    -products "${PRODUCTS}" \
    -orders "${ORDERS}" \
    -order-lines-min "${ORDER_LINES_MIN}" \
    -order-lines-max "${ORDER_LINES_MAX}" \
    -batch-size "${BATCH_SIZE}" \
    -parallel "${SEED_PARALLEL}" \
    -seed "${SEED}" \
    -iterations "${ITERATIONS}" \
    -warmup "${WARMUP}" \
    -label "neo4j" \
    -out "${REPORT_DIR}/neo4j.results.json" \
    2>"${REPORT_DIR}/neo4j.bench.log" || die "Neo4j benchmark failed — see ${REPORT_DIR}/neo4j.bench.log"

  # Graceful shutdown via `neo4j stop` so Neo4j flushes its
  # transaction log and page cache. SIGKILL would leave the store in a
  # recovery-pending state and inflate the `du` reading with uncompacted
  # transactions. Fall back to SIGTERM + SIGKILL only if the wrapper hangs.
  log "stopping Neo4j gracefully (neo4j stop, flushing stores)"
  local stop_rc
  set +e
  ${NEO4J_RUN_PREFIX[@]+"${NEO4J_RUN_PREFIX[@]}"} "${NEO4J_HOME}/bin/neo4j" stop >"${REPORT_DIR}/neo4j.stop.log" 2>&1
  stop_rc=$?
  set -e
  if (( stop_rc != 0 )); then
    err "neo4j stop returned ${stop_rc} — falling back to SIGTERM"
    if [[ -n "${NEO4J_PID}" ]]; then
      stop_pid_graceful "${NEO4J_PID}" 30
    else
      pkill -TERM -f "org\.neo4j\.server\." 2>/dev/null || true
      sleep 5
      pkill -KILL -f "org\.neo4j\.server\." 2>/dev/null || true
    fi
  fi
  NEO4J_PID=""

  local t1=$(date +%s.%N)
  log "stopping powermetrics sampler"
  stop_powermetrics "${POWER_PID}"
  POWER_PID=""
  stop_vmstat "${VMSTAT_PID}"
  VMSTAT_PID=""

  python3 -c "print(f'{float(${t1}) - float(${t0}):.3f}')" > "${REPORT_DIR}/neo4j.wall_seconds.txt"

  # Flush OS page cache before measuring so `du` reflects bytes actually
  # on disk, not dirty buffers the kernel hasn't written yet.
  sync
  log "measuring Neo4j on-disk size"
  du -sk "${NEO4J_DATA_DIR}" | awk '{print $1 * 1024}' > "${REPORT_DIR}/neo4j.disk_bytes.txt"
  du -sh "${NEO4J_DATA_DIR}" > "${REPORT_DIR}/neo4j.disk_human.txt" || true
  echo "${NEO4J_DATA_DIR}" > "${REPORT_DIR}/neo4j.data_dir.txt"
  log "Neo4j run complete"
}

# ------------------------------------------------------------------------
# FalkorDB / Memgraph (docker) and LadybugDB (embedded) install + runs
# ------------------------------------------------------------------------

# Memgraph wants vm.max_map_count >= 524288 and warns "is too low" below that; Docker Desktop's Linux VM
# ships 262144 and forgets any change when it restarts. Raise it for the run (a privileged container
# reaches the VM's sysctl through nsenter). Failing to do so only warns: it is Memgraph's own check.
ensure_docker_max_map_count() {
	local want=524288 have
	have=$(docker run --rm --privileged --pid=host alpine:latest nsenter -t 1 -m -u -n -i cat /proc/sys/vm/max_map_count 2>/dev/null | tail -1)
	if [[ "${have}" =~ ^[0-9]+$ ]] && (( have >= want )); then
		return 0
	fi
	log "raising the Docker VM's vm.max_map_count (${have:-unknown} -> ${want}) for Memgraph"
	docker run --rm --privileged --pid=host alpine:latest nsenter -t 1 -m -u -n -i sysctl -w "vm.max_map_count=${want}" >/dev/null 2>&1 \
		|| log "WARNING: could not raise vm.max_map_count; Memgraph may warn or fail. Run: docker run --rm --privileged --pid=host alpine nsenter -t 1 -m -u -n -i sysctl -w vm.max_map_count=${want}"
}

install_engines() {
	if [[ "${SKIP_FALKOR}" != "1" || "${SKIP_MEMGRAPH}" != "1" ]]; then
		require docker
		docker info >/dev/null 2>&1 || die "docker daemon is not running (needed for FalkorDB/Memgraph; set SKIP_FALKOR=1 SKIP_MEMGRAPH=1 to skip the docker engines)"
	fi
	if [[ "${SKIP_FALKOR}" != "1" ]]; then
		log "pulling ${FALKOR_IMAGE}…"
		docker pull "${FALKOR_IMAGE}"
	fi
	if [[ "${SKIP_MEMGRAPH}" != "1" ]]; then
		log "pulling ${MEMGRAPH_IMAGE}…"
		docker pull "${MEMGRAPH_IMAGE}"
		ensure_docker_max_map_count
	fi
	if [[ "${SKIP_LADYBUG}" != "1" ]]; then
		install_ladybug
	fi
}

install_ladybug() {
	if ! ls "${LADYBUG_LIB_DIR}"/liblbug*.dylib >/dev/null 2>&1 && ! ls "${LADYBUG_LIB_DIR}"/liblbug.so* >/dev/null 2>&1; then
		log "downloading LadybugDB precompiled library into ${LADYBUG_LIB_DIR}…"
		mkdir -p "${LADYBUG_LIB_DIR}"
		curl -fsSL https://raw.githubusercontent.com/LadybugDB/ladybug/refs/heads/main/scripts/download-liblbug.sh \
			| LBUG_TARGET_DIR="${LADYBUG_LIB_DIR}" bash || die "LadybugDB library download failed"
	else
		log "LadybugDB library already present in ${LADYBUG_LIB_DIR}"
	fi
	# The release tarball ships `liblbug.dylib -> liblbug.0.dylib` but not the
	# versioned SONAME link itself, so `-llbug` cannot resolve until
	# liblbug.0.dylib points at the real library. Recreate it pointing at the
	# versioned file whenever one exists (idempotent). `find` is used instead
	# of a glob so `set -euo pipefail` cannot kill the script when the glob
	# has no match.
	local versioned=""
	if [[ "$(uname)" == "Darwin" ]]; then
		versioned=$(find "${LADYBUG_LIB_DIR}" -maxdepth 1 -name 'liblbug.[0-9]*.dylib' 2>/dev/null | head -1)
	else
		versioned=$(find "${LADYBUG_LIB_DIR}" -maxdepth 1 -name 'liblbug.so.[0-9]*' 2>/dev/null | head -1)
	fi
	if [[ -z "${versioned}" ]]; then
		versioned="${LADYBUG_LIB_DIR}/liblbug.dylib"
	fi
	ln -sf "$(basename "${versioned}")" "${LADYBUG_LIB_DIR}/liblbug.0.dylib"
	if [[ "$(uname)" != "Darwin" ]]; then
		ln -sf "$(basename "${versioned}")" "${LADYBUG_LIB_DIR}/liblbug.so.0"
	fi
	log "fetching go-ladybug binding (records go.mod/go.sum changes)…"
	go get github.com/LadybugDB/go-ladybug@v0.17.0 || die "go get github.com/LadybugDB/go-ladybug failed"
}

# run_docker_engine <label> <image> <host_port> <container_port> <data_dir>
#                  <container_data_dir> <database> <auth_mode> <driver_mode>
#                  [extra docker args...]
#
# auth_mode: "none" for no-auth, or "user:pass". driver_mode: "bolt" runs the
# BENCH_BIN over Bolt (Memgraph), "falkor" runs it over native RESP
# (falkordb-go). Extra args are passed to `docker run` (e.g.
# `-e REDIS_ARGS=...` or `--also-log-to-stderr`).
# True once the engine itself (not just Docker's port forwarder) answers: a Bolt handshake (a live server
# replies with the 4 bytes of the version it picked) or a RESP PING.
engine_answers() {
	local driver_mode="$1" host_port="$2" reply
	case "${driver_mode}" in
		bolt)
			reply=$(printf '\x60\x60\xb0\x17\x00\x00\x04\x04\x00\x00\x03\x04\x00\x00\x02\x04\x00\x00\x01\x04' \
				| nc -w 2 127.0.0.1 "${host_port}" 2>/dev/null | od -An -tx1 | tr -d ' \n')
			[[ -n "${reply}" ]]
			;;
		falkor)
			reply=$(printf 'PING\r\n' | nc -w 2 127.0.0.1 "${host_port}" 2>/dev/null | tr -d '\r\n')
			[[ "${reply}" == *PONG* || "${reply}" == *NOAUTH* ]]
			;;
		*) nc -z 127.0.0.1 "${host_port}" 2>/dev/null ;;
	esac
}

# Keep the container's own output next to the report: the cleanup trap removes the container, and a
# crash message such as "data dir owned by root" is otherwise lost.
save_container_logs() {
	docker logs "$1" >"${REPORT_DIR}/$2.container.log" 2>&1 || true
}

run_docker_engine() {
	local label="$1" image="$2" host_port="$3" container_port="$4" data_dir="$5" container_data_dir="$6" database="$7" auth_mode="$8" driver_mode="$9"
	shift 9
	local container="northwind-${label}"
	log "=== ${label} run (docker ${image}) ==="

	if nc -z 127.0.0.1 "${host_port}" 2>/dev/null; then
		die "${label} port ${host_port} is already in use"
	fi
	docker rm -f "${container}" >/dev/null 2>&1 || true
	if [[ -d "${data_dir}" ]]; then
		if ! rm -rf "${data_dir}" 2>/dev/null; then
			sudo rm -rf "${data_dir}"
		fi
	fi
	mkdir -p "${data_dir}"
	# Run under sudo, mkdir makes this directory root-owned. Docker Desktop writes bind mounts as the macOS
	# user who started it, so even root inside the container then gets "Permission denied" (Memgraph:
	# "Failed to open /var/lib/memgraph/.lock", exit 133). Hand the directory to that user.
	if [[ -n "${SUDO_USER:-}" ]]; then
		chown -R "${SUDO_USER}" "${data_dir}"
	fi

	if [[ "${SKIP_POWERMETRICS}" != "1" ]]; then
		log "starting powermetrics sampler (covers startup + benchmark + shutdown)"
		POWER_PID=$(start_powermetrics "${REPORT_DIR}/${label}.powermetrics.plist")
	fi
	VMSTAT_PID=$(start_vmstat "${REPORT_DIR}/${label}.vmstat.log")
	local t0=$(date +%s.%N)

	log "starting ${label} container (port=${host_port})"
	docker run -d --name "${container}" \
		-p "127.0.0.1:${host_port}:${container_port}" \
		-v "${data_dir}:${container_data_dir}" \
		"$@" \
		"${image}" >"${REPORT_DIR}/${label}.docker.log" 2>&1

	# Docker's port forwarder accepts connections as soon as the container starts, long before the engine
	# inside is listening (or after it has already crashed), so a bare port check is not readiness. Require
	# the container to be running AND the engine to answer its own protocol: a Bolt handshake for Bolt
	# engines, PING for RESP (FalkorDB).
	local ready=0
	for i in {1..90}; do
		if ! docker ps --format '{{.Names}}' | grep -qx "${container}"; then
			save_container_logs "${container}" "${label}"
			die "${label} container exited during startup; see ${REPORT_DIR}/${label}.container.log"
		fi
		if engine_answers "${driver_mode}" "${host_port}"; then ready=1; break; fi
		sleep 1
	done
	if [[ "${ready}" != "1" ]]; then
		save_container_logs "${container}" "${label}"
		die "${label} never answered on port ${host_port}; see ${REPORT_DIR}/${label}.container.log"
	fi
	log "${label} ready (container ${container})"

	local bench_args=(
		-database "${database}"
		-categories "${CATEGORIES}"
		-suppliers "${SUPPLIERS}"
		-customers "${CUSTOMERS}"
		-products "${PRODUCTS}"
		-orders "${ORDERS}"
		-order-lines-min "${ORDER_LINES_MIN}"
		-order-lines-max "${ORDER_LINES_MAX}"
		-batch-size "${BATCH_SIZE}"
		-parallel "${SEED_PARALLEL}"
		-seed "${SEED}"
		-iterations "${ITERATIONS}"
		-warmup "${WARMUP}"
		-label "${label}"
		-out "${REPORT_DIR}/${label}.results.json"
	)
	case "${driver_mode}" in
		bolt)
			bench_args=(-uri "bolt://localhost:${host_port}" "${bench_args[@]}")
			;;
		falkor)
			bench_args=(-driver falkor -uri "falkor://localhost:${host_port}" "${bench_args[@]}")
			;;
		*)
			die "unknown driver mode: ${driver_mode}"
			;;
	esac
	if [[ "${auth_mode}" == "none" ]]; then
		"${BENCH_BIN}" "${bench_args[@]}" -no-auth 2>"${REPORT_DIR}/${label}.bench.log" \
			|| { save_container_logs "${container}" "${label}"; die "${label} benchmark failed — see ${REPORT_DIR}/${label}.bench.log and ${REPORT_DIR}/${label}.container.log"; }
	else
		local engine_user="${auth_mode%%:*}" engine_pass="${auth_mode#*:}"
		"${BENCH_BIN}" "${bench_args[@]}" -user "${engine_user}" -pass "${engine_pass}" 2>"${REPORT_DIR}/${label}.bench.log" \
			|| { save_container_logs "${container}" "${label}"; die "${label} benchmark failed — see ${REPORT_DIR}/${label}.bench.log and ${REPORT_DIR}/${label}.container.log"; }
	fi

	# Graceful stop so the engine flushes its write cache before `du`.
	log "stopping ${label} container gracefully"
	docker stop "${container}" >/dev/null 2>&1 || true
	docker rm -f "${container}" >/dev/null 2>&1 || true

	local t1=$(date +%s.%N)
	if [[ -n "${POWER_PID:-}" ]]; then
		log "stopping powermetrics sampler"
		stop_powermetrics "${POWER_PID}"
		POWER_PID=""
	fi
	stop_vmstat "${VMSTAT_PID}"
	VMSTAT_PID=""

	python3 -c "print(f'{float(${t1}) - float(${t0}):.3f}')" > "${REPORT_DIR}/${label}.wall_seconds.txt"

	sync
	log "measuring ${label} on-disk size"
	du -sk "${data_dir}" | awk '{print $1 * 1024}' > "${REPORT_DIR}/${label}.disk_bytes.txt"
	du -sh "${data_dir}" > "${REPORT_DIR}/${label}.disk_human.txt" || true
	echo "${data_dir}" > "${REPORT_DIR}/${label}.data_dir.txt"
	log "${label} run complete"
}

run_falkor() {
	local extra_docker_args=()
	local auth="none"
	if [[ "${FALKOR_AUTH}" != "none" ]]; then
		auth="${FALKOR_USER}:${FALKOR_PASS}"
		# FalkorDB authenticates RESP clients through Redis ACLs; require a
		# password so the configured credentials actually gate the server.
		extra_docker_args=(-e "REDIS_ARGS=--requirepass ${FALKOR_PASS}")
	fi
	# Native RESP: the official falkordb-go client speaks GRAPH.QUERY over
	# port 6379 (mapped to FALKOR_PORT). The empty-array expansion is guarded
	# for `set -u` (bash 3.2 on macOS trips on a bare empty array).
	run_docker_engine "falkor" "${FALKOR_IMAGE}" "${FALKOR_PORT}" "6379" \
		"${FALKOR_DATA_DIR}" "${FALKOR_CONTAINER_DATA_DIR}" "${FALKOR_DATABASE}" "${auth}" "falkor" \
		${extra_docker_args[@]+"${extra_docker_args[@]}"}
}

run_memgraph() {
	# Memgraph maps the host dir into its data directory. No auth by default;
	# the report classifies the store from the data dir itself.
	# --user root: memgraph/memgraph 3.13+ exits at startup ("process is running as user memgraph, but
	# '/var/lib/memgraph' is owned by user root") when a root-owned host directory is bind-mounted.
	run_docker_engine "memgraph" "${MEMGRAPH_IMAGE}" "${MEMGRAPH_BOLT_PORT}" "7687" \
		"${MEMGRAPH_DATA_DIR}" "/var/lib/memgraph" "${MEMGRAPH_DATABASE}" "none" "bolt" --user root
}

run_ladybug() {
	local label="ladybug"
	log "=== LadybugDB run (embedded, Kuzu fork) ==="

	if [[ -d "${LADYBUG_DATA_DIR}" ]]; then
		if ! rm -rf "${LADYBUG_DATA_DIR}" 2>/dev/null; then
			sudo rm -rf "${LADYBUG_DATA_DIR}"
		fi
	fi
	mkdir -p "$(dirname "${LADYBUG_DATA_DIR}")"

	log "building ladybug-enabled benchmark runner"
	CGO_ENABLED=1 \
		CGO_CFLAGS="-I${LADYBUG_LIB_DIR}" \
		CGO_LDFLAGS="-L${LADYBUG_LIB_DIR} -llbug -Wl,-rpath,${LADYBUG_LIB_DIR}" \
		go build -tags "ladybug,system_ladybug" -o "${LADYBUG_BENCH_BIN}" ./testing/benchmarks/northwind_power \
		|| die "ladybug runner build failed (check ${LADYBUG_LIB_DIR} and the go-ladybug dependency)"

	if [[ "${SKIP_POWERMETRICS}" != "1" ]]; then
		log "starting powermetrics sampler (covers startup + benchmark + shutdown)"
		POWER_PID=$(start_powermetrics "${REPORT_DIR}/${label}.powermetrics.plist")
	fi
	VMSTAT_PID=$(start_vmstat "${REPORT_DIR}/${label}.vmstat.log")
	local t0=$(date +%s.%N)

	"${LADYBUG_BENCH_BIN}" \
		-driver ladybug \
		-ladybug-dir "${LADYBUG_DATA_DIR}" \
		-categories "${CATEGORIES}" \
		-suppliers "${SUPPLIERS}" \
		-customers "${CUSTOMERS}" \
		-products "${PRODUCTS}" \
		-orders "${ORDERS}" \
		-order-lines-min "${ORDER_LINES_MIN}" \
		-order-lines-max "${ORDER_LINES_MAX}" \
		-batch-size "${BATCH_SIZE}" \
		-parallel "${SEED_PARALLEL}" \
		-seed "${SEED}" \
		-iterations "${ITERATIONS}" \
		-warmup "${WARMUP}" \
		-label "${label}" \
		-out "${REPORT_DIR}/${label}.results.json" \
		2>"${REPORT_DIR}/${label}.bench.log" || die "LadybugDB benchmark failed — see ${REPORT_DIR}/${label}.bench.log"

	local t1=$(date +%s.%N)
	if [[ -n "${POWER_PID:-}" ]]; then
		log "stopping powermetrics sampler"
		stop_powermetrics "${POWER_PID}"
		POWER_PID=""
	fi
	stop_vmstat "${VMSTAT_PID}"
	VMSTAT_PID=""

	python3 -c "print(f'{float(${t1}) - float(${t0}):.3f}')" > "${REPORT_DIR}/${label}.wall_seconds.txt"

	sync
	log "measuring ${label} on-disk size"
	du -sk "${LADYBUG_DATA_DIR}" | awk '{print $1 * 1024}' > "${REPORT_DIR}/${label}.disk_bytes.txt"
	du -sh "${LADYBUG_DATA_DIR}" > "${REPORT_DIR}/${label}.disk_human.txt" || true
	echo "${LADYBUG_DATA_DIR}" > "${REPORT_DIR}/${label}.data_dir.txt"
	log "LadybugDB run complete"
}

# ------------------------------------------------------------------------
# Reports
# ------------------------------------------------------------------------

generate_reports() {
  log "generating reports"
  python3 "${SCRIPT_DIR}/northwind_report.py" \
    --dir "${REPORT_DIR}" \
    --iterations "${ITERATIONS}" \
    --warmup "${WARMUP}" \
    --batch-size "${BATCH_SIZE}" \
    --parallel "${SEED_PARALLEL}" \
    --products "${PRODUCTS}" \
    --orders "${ORDERS}"
  log "reports written to ${REPORT_DIR}"
  ls -la "${REPORT_DIR}"/*.md 2>/dev/null || true
}

# Install engines before any run so downloads/network issues surface up front.
install_engines

for mode in ${NORNIC_PARSER_MODES}; do
  run_nornic "${mode}"
done
if [[ "${SKIP_NEO4J}" != "1" ]]; then
  run_neo4j
fi
if [[ "${SKIP_FALKOR}" != "1" ]]; then
  run_falkor
fi
if [[ "${SKIP_MEMGRAPH}" != "1" ]]; then
  run_memgraph
fi
if [[ "${SKIP_LADYBUG}" != "1" ]]; then
  run_ladybug
fi
generate_reports

log "DONE — reports: ${REPORT_DIR}"
