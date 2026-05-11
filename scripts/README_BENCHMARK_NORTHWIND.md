# Northwind Power & Storage Benchmark: NornicDB vs. Neo4j

Serialized benchmark that starts each database in turn, runs a fixed Northwind
workload against it, samples power draw with `powermetrics`, measures on-disk
storage, and generates three Markdown reports:

- `nornicdb.md` — NornicDB results
- `neo4j.md` — Neo4j results
- `comparison.md` — side-by-side

The two databases are **never running at the same time**, so power and disk
numbers are isolated.

## What the workload does

For each database:

1. Wipe the data directory for a clean baseline.
2. **Start powermetrics** so the sampler captures startup cost too.
3. Start the database and wait for Bolt + a smoke `RETURN 1` to succeed.
4. Seed a randomised, Northwind-shaped graph over Bolt — `CATEGORIES`
   categories (default 16), `SUPPLIERS` suppliers (default 24), `CUSTOMERS`
   customers (default 200), `PRODUCTS` products (default 8 000), `ORDERS`
   orders (default 8 000) with between `ORDER_LINES_MIN` and
   `ORDER_LINES_MAX` line items each (default 1..6, picked uniformly at
   random). Property values vary in length (names, multi-block descriptions,
   tag arrays, contact names, addresses, phone numbers, timestamps) so each
   node carries meaningfully more data than the original fixed-size seeder,
   letting on-disk storage scaling be observed past Badger's preallocated
   scratch and Neo4j's empty-store baseline. `SEED` makes the random layout
   reproducible — identical graphs are written to NornicDB and Neo4j in the
   same run. For much larger runs, raise `PRODUCTS` / `ORDERS`; seed time
   scales linearly but query times are dominated by the larger fan-out.
5. Run four read queries `ITERATIONS` times each (default 10), with 2 warmup
   iterations that are discarded:
   - `products_per_category`
   - `customer_category_distinct_orders`
   - `optional_match_orders_count`
   - `revenue_by_product`
6. Record latencies (mean / p50 / p95 / p99 / min / max / stddev / ops/sec).
7. **SIGKILL the database process** (no graceful shutdown — observability
   signal handlers in some builds can hang on SIGTERM).
8. Stop powermetrics.
9. Measure on-disk data directory size (`du -sk`) after the DB has exited.

`powermetrics` samples CPU, GPU, and package power at 1-second intervals and
wraps the entire DB lifecycle (startup → seed → benchmark → shutdown), so the
reported energy covers the full run, not just the query window.

## Prerequisites

- macOS with Apple Silicon or Intel (powermetrics is macOS-only).
- `sudo` access — `powermetrics` requires root. The script runs `sudo -v`
  once at the start and keeps the timestamp alive.
- Go toolchain (any version that builds this repo).
- Python 3 (stdlib only; no pip installs needed).
- Neo4j Community Edition, installed locally (not Docker):

  ```bash
  brew install neo4j
  ```

  The script defaults to `NEO4J_HOME=/opt/homebrew/opt/neo4j` and the data
  directory at `/opt/homebrew/var/neo4j/data`. Override via env vars if your
  install lives elsewhere.

- `cypher-shell` on PATH (brew pulls this in as a dependency).

## Files produced by this benchmark

Under `scripts/benchmark_reports/<timestamp>/`:

```
nornicdb.results.json        raw benchmark output (latencies etc.)
nornicdb.powermetrics.plist  concatenated plist samples from powermetrics
nornicdb.disk_bytes.txt      du -sk result in bytes
nornicdb.disk_human.txt      du -sh result (human-readable)
nornicdb.wall_seconds.txt    wall-clock seconds of the sampled window
nornicdb.stdout.log          NornicDB server stdout
nornicdb.stderr.log          NornicDB server stderr
nornicdb.bench.log           bench runner stderr
neo4j.*                      same set for Neo4j
nornicdb.md                  single-DB report
neo4j.md                     single-DB report
comparison.md                side-by-side report
```

## Step-by-step instructions

1. **Install Neo4j** (once):

   ```bash
   brew install neo4j
   ```

2. **Clone / pull this repo**, then `cd` to its root:

   ```bash
   cd /path/to/NornicDB
   ```

3. **(Optional) Tune parameters** via env vars:

   ```bash
   export ITERATIONS=10          # default: 10
   export WARMUP=2               # default: 2
   export PRODUCTS=2000          # default: 2000
   export ORDERS=2000            # default: 2000
   ```

   All four of the Northwind benchmark queries run `ITERATIONS + WARMUP` times
   per database. With defaults that's 48 queries per DB after seed.

4. **Run the orchestrator**:

   ```bash
   ./scripts/benchmark_northwind_vs_neo4j.sh
   ```

   You will be prompted once for your sudo password (for `powermetrics`).
   After that the script runs unattended. Expect roughly 2–5 minutes per
   database at default scale on a modern Mac, plus Neo4j startup time (~15s).

   You may also run the whole script under sudo if your shell doesn't
   support a TTY sudo prompt:

   ```bash
   sudo ./scripts/benchmark_northwind_vs_neo4j.sh
   ```

   When invoked as root the script skips the sudo prime step.

   Progress is logged to stderr in the form:

   ```
   [11:05:24] === NornicDB run ===
   [11:05:24] starting NornicDB (bolt=17687 http=17474)
   [11:05:26] NornicDB ready (pid 12345)
   [11:05:26] starting powermetrics sampler
   [nornicdb] seeding Northwind (products=2000 orders=2000)
   [nornicdb] seeded in 812.3ms (16032 nodes, 12000 rels)
   [nornicdb] products_per_category            mean=   3.47ms p95=   4.88ms ops/s= 287.4
   ...
   [11:08:19] === Neo4j run ===
   ...
   [11:11:44] generating reports
   [11:11:44] DONE — reports: .../benchmark_reports/20260511_111144
   ```

5. **Read the reports**:

   ```bash
   ls scripts/benchmark_reports/$(ls -t scripts/benchmark_reports | head -1)
   open scripts/benchmark_reports/$(ls -t scripts/benchmark_reports | head -1)/comparison.md
   ```

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `ITERATIONS` | `10` | Iterations per query (excluding warmup). |
| `WARMUP` | `2` | Warmup iterations per query (not recorded). |
| `CATEGORIES` | `16` | Category nodes seeded. |
| `SUPPLIERS` | `24` | Supplier nodes seeded. |
| `CUSTOMERS` | `200` | Customer nodes seeded. |
| `PRODUCTS` | `8000` | Product nodes seeded. |
| `ORDERS` | `8000` | Order nodes seeded. |
| `ORDER_LINES_MIN` | `1` | Minimum ORDERS edges per Order (randomised per order). |
| `ORDER_LINES_MAX` | `6` | Maximum ORDERS edges per Order. |
| `BATCH_SIZE` | `200` | Rows per `UNWIND` seed batch. |
| `SEED` | `42` | PRNG seed — same seed produces an identical dataset on both DBs. |
| `NORNIC_DATA_DIR` | `./bench-data/nornic` | NornicDB data directory. Wiped each run. |
| `NEO4J_HOME` | `/opt/homebrew/opt/neo4j` | Neo4j install prefix. |
| `NEO4J_DATA_DIR` | `/opt/homebrew/var/neo4j/data` | Neo4j data dir. `databases/` and `transactions/` are wiped each run. |
| `NEO4J_PASSWORD` | `testpass123` | Neo4j password (set non-interactively via `neo4j-admin dbms set-initial-password`). |
| `NORNIC_DATABASE` | `nornic` | Default database name for NornicDB (it serves `nornic`, not `neo4j`). |
| `NEO4J_DATABASE` | `neo4j` | Default database name for Neo4j. |
| `CYPHER_SHELL` | `$(command -v cypher-shell)` | Override cypher-shell binary. |
| `REPORT_DIR` | `./scripts/benchmark_reports` | Parent directory for timestamped reports. |

NornicDB runs on non-default ports (`17687` bolt, `17474` HTTP) so it can
coexist with a developer's Neo4j on standard ports while still letting Neo4j
use its defaults during its own phase.

## Repeating a run

Repeat runs are designed to be deterministic in shape:

- NornicDB's data directory (`$NORNIC_DATA_DIR`) is fully wiped before its
  phase.
- Neo4j's `databases/` and `transactions/` subdirectories are wiped before its
  phase (the install itself is not touched).
- Seeded row counts, query set, and warmup counts are identical each run.

Latency and power figures will vary between runs — laptop thermals, OS
background load, and Bolt driver warmup all contribute noise. For a stable
comparison run the script 3+ times back-to-back and read the aggregate.

## Troubleshooting

**`sudo: a terminal is required to read the password`**
The script needs an interactive sudo prompt on start. Run it from a regular
terminal window; do not pipe the run through `nohup`/`ssh -n`/`&`.

**`Neo4j bolt port never came up`**
Check `scripts/benchmark_reports/<timestamp>/neo4j.start.log`. Common causes:
already-running Neo4j (`brew services stop neo4j`), port 7687 in use by
another service, or JVM not found. Confirm `java --version` works.

**`NornicDB bolt port never came up`**
Check `nornicdb.stderr.log`. Usually data-dir permission problems or a
clashing port. Set `NORNIC_DATA_DIR` to a writable location.

**Power samples are empty**
If `nornicdb.powermetrics.plist` is zero bytes, sudo probably timed out.
Re-run in a shell where your sudo timestamp is fresh (`sudo -v` first).

**Neo4j password is wrong**
The script forces the initial password via
`neo4j-admin dbms set-initial-password`. If you already set a different
password manually, export `NEO4J_PASSWORD` to match, or wipe
`$NEO4J_DATA_DIR/dbms/auth` before running.

## Files in this benchmark

- `scripts/benchmark_northwind_vs_neo4j.sh` — orchestrator (start/stop both
  DBs serially, sample power, measure disk, trigger report generation).
- `scripts/northwind_report.py` — parses results + powermetrics plists and
  emits the three Markdown reports.
- `testing/benchmarks/northwind_power/main.go` — Go benchmark runner
  (connects via Bolt, seeds Northwind, times queries, writes JSON).
- `scripts/README_BENCHMARK_NORTHWIND.md` — this file.
