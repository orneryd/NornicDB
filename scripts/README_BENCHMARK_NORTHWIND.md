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
   categories (default 96), `SUPPLIERS` suppliers (default 144), `CUSTOMERS`
   customers (default 1 200), `PRODUCTS` products (default 48 000), `ORDERS`
   orders (default 48 000) with between `ORDER_LINES_MIN` and
   `ORDER_LINES_MAX` line items each (default 1..6, picked uniformly at
   random). Property values vary in length (names, multi-block descriptions,
   tag arrays, contact names, addresses, phone numbers, timestamps) so each
   node carries meaningfully more data than the original fixed-size seeder,
   letting on-disk storage scaling be observed past Badger's preallocated
   scratch and Neo4j's empty-store baseline. `SEED` makes the random layout
   reproducible — identical graphs are written to NornicDB and Neo4j in the
   same run. For much larger runs, raise `PRODUCTS` / `ORDERS`; seed time
   scales linearly but query times are dominated by the larger fan-out.
5. Run the 14-workload read suite `ITERATIONS` times per query (default 30),
   with `WARMUP` iterations (default 5) that are discarded:
   - `products_per_category`
   - `customer_category_distinct_orders`
   - `optional_match_orders_count`
   - `revenue_by_product`
   - `products_by_supplier`
   - `orders_by_customer`
   - `revenue_by_category`
   - `revenue_by_supplier`
   - `revenue_by_customer`
   - `order_line_sales_by_country`
   - `low_stock_products`
   - `products_in_category`
   - `order_line_quantity_distribution`
   - `customer_order_details`
6. Record each query's sample count, mean / median / p95 / p99 / min / max /
   standard deviation, result rows, and per-query ops/sec. The comparison
   includes every query from either engine. End-to-end throughput is measured
   operations divided by total query-loop duration (including warmups and
   per-query setup); latency-only aggregate throughput is reported separately.
7. Seed writes use configurable `BATCH_SIZE` UNWIND chunks and
   `SEED_PARALLEL` independent Bolt sessions per phase (default 500 rows and 4
   sessions). Dependencies between seed phases remain ordered.
8. Stop the databases cleanly: NornicDB receives SIGTERM to flush storage, and
   Neo4j is stopped through its CLI.
9. Stop powermetrics.
10. Measure on-disk data directory size (`du -sk`) after the DB has exited.

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

> **Destructive:** the orchestrator deletes the configured NornicDB data
> directory and wipes Neo4j's `databases/` and `transactions/` directories.
> Use dedicated benchmark-only paths; do not point it at data you need.
> It refuses to reset an open NornicDB data directory or a running Neo4j
> instance. Stop those servers before starting the benchmark.

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
   export ITERATIONS=30          # default: 30 measured runs per query
   export WARMUP=5               # default: 5 discarded runs per query
   export BATCH_SIZE=500         # default: 500 rows per UNWIND batch
   export SEED_PARALLEL=4        # default: 4 concurrent Bolt sessions per phase
   export PRODUCTS=2000          # default: 48000
   export ORDERS=2000            # default: 48000
   ```

   All 14 Northwind workloads run `ITERATIONS + WARMUP` times per database.
   Defaults produce 420 measured operations and 70 warmup operations per DB.
   Seed settings are included in each report to make tuning reproducible.

4. **Run the orchestrator**:

   ```bash
   ./scripts/benchmark_northwind_vs_neo4j.sh
   ```

   You will be prompted once for your sudo password (for `powermetrics`).
   After that the script runs unattended. Runtime depends on dataset scale,
   query iterations, storage configuration, and Neo4j startup time.

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
| `ITERATIONS` | `30` | Measured iterations per query (excluding warmup). |
| `WARMUP` | `5` | Warmup iterations per query (not recorded). |
| `CATEGORIES` | `96` | Category nodes seeded. |
| `SUPPLIERS` | `144` | Supplier nodes seeded. |
| `CUSTOMERS` | `1200` | Customer nodes seeded. |
| `PRODUCTS` | `48000` | Product nodes seeded. |
| `ORDERS` | `48000` | Order nodes seeded. |
| `ORDER_LINES_MIN` | `1` | Minimum ORDERS edges per Order (randomised per order). |
| `ORDER_LINES_MAX` | `6` | Maximum ORDERS edges per Order. |
| `BATCH_SIZE` | `500` | Rows per `UNWIND` seed batch. |
| `SEED_PARALLEL` | `4` | Concurrent Bolt sessions per seed phase. |
| `SEED` | `42` | PRNG seed — same seed produces an identical dataset on both DBs. |
| `GRAPH_ONLY` | `1` | Disable NornicDB BM25/vector-index maintenance for this graph-only workload. Set to `0` to include search-index build cost. |
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
When tuning ingestion, change one of `BATCH_SIZE` or `SEED_PARALLEL` at a time
and compare ingestion duration on the same dataset and graph-only mode. The
report separates graph wipe, index setup, and ingestion (row generation plus
node and relationship writes). Total seed duration includes all three and
session overhead; ingestion nodes/sec and relationships/sec use only ingestion
time. Older reports without phase timings show rates based on total seed time
and label them as legacy. Index setup measures creating the declared indexes
before writing data, not the index maintenance performed during ingestion.
The defaults
are configurable baselines, not a claim that 500 rows / 4 sessions is optimal
on every machine.

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
