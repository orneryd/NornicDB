#!/usr/bin/env bash
#
# benchmark_northwind_vs_neo4j.sh
#
# Serialized Northwind benchmark for NornicDB and Neo4j:
#
#   0. Wipe NornicDB's data directory and Neo4j's databases/ + transactions/
#      subtrees up front so both engines start from a fresh store. Any stale
#      Neo4j JVM is SIGKILL'd before the wipe.
#   1. Start NornicDB, sample powermetrics during seed+benchmark,
#      measure on-disk data size, stop NornicDB.
#   2. Start local Neo4j, sample powermetrics during seed+benchmark,
#      measure on-disk data size, stop Neo4j.
#   3. Generate three Markdown reports:
#        - reports/<timestamp>/nornicdb.md
#        - reports/<timestamp>/neo4j.md
#        - reports/<timestamp>/comparison.md
#
# Requires: sudo (for powermetrics), Neo4j installed locally (brew install neo4j),
# Go toolchain, Python 3. Invokes `sudo -v` up front so powermetrics can run
# non-interactively.
#
# Configuration via env (with defaults):
#   ITERATIONS=10           iterations per query (per-DB, per-query)
#   WARMUP=2                warmup iterations (not recorded)
#   PRODUCTS=2000           products in seed
#   ORDERS=2000             orders in seed
#   NORNIC_DATA_DIR         NornicDB data dir (default ./bench-data/nornic)
#   NEO4J_HOME              Neo4j install dir (default /opt/homebrew/opt/neo4j)
#   NEO4J_DATA_DIR          Neo4j data dir (default /opt/homebrew/var/neo4j/data)
#   NEO4J_PASSWORD          Neo4j password (default "testpass123")
#   REPORT_DIR              Parent dir for timestamped reports (default scripts/benchmark_reports)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ITERATIONS="${ITERATIONS:-10}"
WARMUP="${WARMUP:-2}"
CATEGORIES="${CATEGORIES:-96}"
SUPPLIERS="${SUPPLIERS:-144}"
CUSTOMERS="${CUSTOMERS:-1200}"
PRODUCTS="${PRODUCTS:-48000}"
ORDERS="${ORDERS:-48000}"
ORDER_LINES_MIN="${ORDER_LINES_MIN:-1}"
ORDER_LINES_MAX="${ORDER_LINES_MAX:-6}"
BATCH_SIZE="${BATCH_SIZE:-500}"
SEED="${SEED:-42}"
NORNIC_DATA_DIR="${NORNIC_DATA_DIR:-${REPO_ROOT}/bench-data/nornic}"
NEO4J_HOME="${NEO4J_HOME:-/opt/homebrew/opt/neo4j}"
NEO4J_DATA_DIR="${NEO4J_DATA_DIR:-/opt/homebrew/var/neo4j/data}"
NEO4J_PASSWORD="${NEO4J_PASSWORD:-testpass123}"
REPORT_PARENT="${REPORT_DIR:-${SCRIPT_DIR}/benchmark_reports}"

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
  fi
  if [[ -n "${POWER_PID:-}" ]]; then
    sudo kill -KILL "${POWER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${NEO4J_PID:-}" ]] && kill -0 "${NEO4J_PID}" 2>/dev/null; then
    log "cleanup: killing Neo4j (pid ${NEO4J_PID})"
    kill -KILL "${NEO4J_PID}" 2>/dev/null || true
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

require() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
require go
require sudo
require python3
require "${NEO4J_HOME}/bin/neo4j"
CYPHER_SHELL="${CYPHER_SHELL:-$(command -v cypher-shell || true)}"
[[ -x "${CYPHER_SHELL}" ]] || die "cypher-shell not found on PATH (set CYPHER_SHELL=/path/to/cypher-shell)"
[[ -x /usr/bin/powermetrics ]] || die "/usr/bin/powermetrics not found"

log "config: iterations=${ITERATIONS} warmup=${WARMUP}"
log "config: categories=${CATEGORIES} suppliers=${SUPPLIERS} customers=${CUSTOMERS}"
log "config: products=${PRODUCTS} orders=${ORDERS} order_lines=${ORDER_LINES_MIN}..${ORDER_LINES_MAX} seed=${SEED}"
log "config: report_dir=${REPORT_DIR}"

if [[ $EUID -ne 0 ]]; then
  log "priming sudo for powermetrics (single prompt up front)…"
  sudo -v
  # Keep the sudo timestamp alive while the script runs.
  ( while true; do sudo -n true 2>/dev/null; sleep 50; done ) &
  SUDO_KEEPALIVE_PID=$!
  trap 'kill ${SUDO_KEEPALIVE_PID} 2>/dev/null || true; cleanup' EXIT INT TERM
else
  log "running as root — skipping sudo prime"
fi

log "wiping data directories before run (NornicDB + Neo4j databases/transactions)…"
# NornicDB: remove the whole data dir. If a prior run left it root-owned
# (sudo invocation), fall through to a sudo rm so the wipe actually succeeds.
if [[ -d "${NORNIC_DATA_DIR}" ]]; then
  if ! rm -rf "${NORNIC_DATA_DIR}" 2>/dev/null; then
    sudo rm -rf "${NORNIC_DATA_DIR}"
  fi
fi
mkdir -p "${NORNIC_DATA_DIR}"

# Neo4j: stop any running instance first so the wipe isn't fighting a live JVM,
# then remove the ephemeral `databases/` + `transactions/` subtrees (leave the
# parent alone so the brew-managed config/logs directories persist).
if [[ -x "${NEO4J_HOME}/bin/neo4j" ]]; then
  # If a stale JVM is still alive, kill it hard — neo4j stop can be slow.
  if stale=$(pgrep -f "org\.neo4j\.server\." 2>/dev/null); then
    [[ -n "${stale}" ]] && echo "${stale}" | xargs -n1 kill -9 2>/dev/null || true
  fi
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

# ------------------------------------------------------------------------
# NornicDB run
# ------------------------------------------------------------------------

run_nornic() {
  log "=== NornicDB run ==="
  # Data dir already wiped at script start. Just make sure the directory
  # exists (some engines refuse to boot without it).
  mkdir -p "${NORNIC_DATA_DIR}"

  # Powermetrics wraps the entire DB lifecycle — startup, seed, benchmark,
  # shutdown — so the report captures the full energy envelope, not just the
  # query window.
  log "starting powermetrics sampler (covers startup + benchmark + shutdown)"
  POWER_PID=$(start_powermetrics "${REPORT_DIR}/nornicdb.powermetrics.plist")
  local t0=$(date +%s.%N)

  log "starting NornicDB (bolt=${NORNIC_BOLT_PORT} http=${NORNIC_HTTP_PORT})"
  NORNICDB_NO_AUTH=true NORNICDB_EMBEDDING_ENABLED=false \
    "${NORNIC_BIN}" serve \
      --bolt-port "${NORNIC_BOLT_PORT}" \
      --http-port "${NORNIC_HTTP_PORT}" \
      --data-dir "${NORNIC_DATA_DIR}" \
      --no-auth \
      >"${REPORT_DIR}/nornicdb.stdout.log" 2>"${REPORT_DIR}/nornicdb.stderr.log" &
  NORNIC_PID=$!

  for i in {1..30}; do
    if nc -z 127.0.0.1 "${NORNIC_BOLT_PORT}" 2>/dev/null; then break; fi
    sleep 1
    if ! kill -0 "${NORNIC_PID}" 2>/dev/null; then
      die "NornicDB crashed on startup; see ${REPORT_DIR}/nornicdb.stderr.log"
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
    -seed "${SEED}" \
    -iterations "${ITERATIONS}" \
    -warmup "${WARMUP}" \
    -label "nornicdb" \
    -out "${REPORT_DIR}/nornicdb.results.json" \
    2>"${REPORT_DIR}/nornicdb.bench.log" || die "NornicDB benchmark failed — see ${REPORT_DIR}/nornicdb.bench.log"

  log "killing NornicDB (SIGKILL)"
  kill_pid "${NORNIC_PID}"
  NORNIC_PID=""

  local t1=$(date +%s.%N)
  log "stopping powermetrics sampler"
  stop_powermetrics "${POWER_PID}"
  POWER_PID=""

  python3 -c "print(f'{float(${t1}) - float(${t0}):.3f}')" > "${REPORT_DIR}/nornicdb.wall_seconds.txt"

  log "measuring NornicDB on-disk size"
  du -sk "${NORNIC_DATA_DIR}" | awk '{print $1 * 1024}' > "${REPORT_DIR}/nornicdb.disk_bytes.txt"
  du -sh "${NORNIC_DATA_DIR}" > "${REPORT_DIR}/nornicdb.disk_human.txt" || true
  echo "${NORNIC_DATA_DIR}" > "${REPORT_DIR}/nornicdb.data_dir.txt"

  log "NornicDB run complete"
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
  "${prefix[@]}" "${NEO4J_HOME}/bin/neo4j-admin" dbms set-initial-password "${NEO4J_PASSWORD}" \
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

  # Kill any stale Neo4j JVM from a previous failed run. The main class name
  # varies across Neo4j versions; match on the classpath + `org.neo4j.server`
  # package to cover CommunityEntryPoint, Neo4jCommunity, and friends.
  local stale
  stale=$(pgrep -f "org\.neo4j\.server\." 2>/dev/null || true)
  if [[ -n "${stale}" ]]; then
    log "killing stale Neo4j JVM(s): ${stale}"
    echo "${stale}" | xargs -n1 kill -9 2>/dev/null || true
    sleep 2
  fi

  # Data dir already wiped at script start (including any prior Neo4j
  # `databases/` + `transactions/` subtrees). Just re-set the password on
  # the fresh store.
  configure_neo4j_password

  # Powermetrics wraps the entire DB lifecycle.
  log "starting powermetrics sampler (covers startup + benchmark + shutdown)"
  POWER_PID=$(start_powermetrics "${REPORT_DIR}/neo4j.powermetrics.plist")
  local t0=$(date +%s.%N)

  log "starting Neo4j (as user ${neo4j_owner})"
  # Don't abort the script on nonzero exit — we poll for the Bolt port and
  # surface a useful error ourselves.
  set +e
  "${NEO4J_RUN_PREFIX[@]}" "${NEO4J_HOME}/bin/neo4j" start >"${REPORT_DIR}/neo4j.start.log" 2>&1
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
    -seed "${SEED}" \
    -iterations "${ITERATIONS}" \
    -warmup "${WARMUP}" \
    -label "neo4j" \
    -out "${REPORT_DIR}/neo4j.results.json" \
    2>"${REPORT_DIR}/neo4j.bench.log" || die "Neo4j benchmark failed — see ${REPORT_DIR}/neo4j.bench.log"

  log "killing Neo4j (SIGKILL)"
  if [[ -n "${NEO4J_PID}" ]]; then
    kill_pid "${NEO4J_PID}"
  else
    pkill -KILL -f "org\.neo4j\.server\." 2>/dev/null || true
  fi
  NEO4J_PID=""

  local t1=$(date +%s.%N)
  log "stopping powermetrics sampler"
  stop_powermetrics "${POWER_PID}"
  POWER_PID=""

  python3 -c "print(f'{float(${t1}) - float(${t0}):.3f}')" > "${REPORT_DIR}/neo4j.wall_seconds.txt"

  log "measuring Neo4j on-disk size"
  du -sk "${NEO4J_DATA_DIR}" | awk '{print $1 * 1024}' > "${REPORT_DIR}/neo4j.disk_bytes.txt"
  du -sh "${NEO4J_DATA_DIR}" > "${REPORT_DIR}/neo4j.disk_human.txt" || true
  echo "${NEO4J_DATA_DIR}" > "${REPORT_DIR}/neo4j.data_dir.txt"
  log "Neo4j run complete"
}

# ------------------------------------------------------------------------
# Reports
# ------------------------------------------------------------------------

generate_reports() {
  log "generating reports"
  python3 "${SCRIPT_DIR}/northwind_report.py" \
    --dir "${REPORT_DIR}" \
    --iterations "${ITERATIONS}" \
    --products "${PRODUCTS}" \
    --orders "${ORDERS}"
  log "reports written to ${REPORT_DIR}"
  ls -la "${REPORT_DIR}"/*.md 2>/dev/null || true
}

run_nornic
run_neo4j
generate_reports

log "DONE — reports: ${REPORT_DIR}"
