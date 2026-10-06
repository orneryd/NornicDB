#!/usr/bin/env python3
"""Generate Northwind-benchmark reports for NornicDB and Neo4j.

Inputs (produced by scripts/benchmark_northwind_vs_neo4j.sh in --dir):
  <label>.results.json         per-DB benchmark JSON (from northwind_power)
  <label>.powermetrics.plist   concatenated Apple `powermetrics -f plist` samples
  <label>.disk_bytes.txt       on-disk bytes for data directory
  <label>.wall_seconds.txt     wall-clock seconds for the sampled window

Outputs (written into --dir):
  nornicdb.md         NornicDB-only report (default parser)
  nornicdb-antlr.md   NornicDB-only report with NORNICDB_PARSER=antlr (when that run exists)
  neo4j.md            Neo4j-only report
  comparison.md       side-by-side comparison; when the ANTLR run exists it gains a parser-mode
                      section and an extra "NornicDB (ANTLR)" row per query. Every NornicDB-vs-Neo4j
                      figure is computed exactly as before.
  parser-modes.md     default-vs-ANTLR query latency and throughput (needs only the two NornicDB runs)
"""

import argparse
import fnmatch
import json
import os
import plistlib
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Storage classification
#
# Goal: strip preallocated scratch, WAL, indexes, and non-data bookkeeping so
# the headline "raw data" number compares the actual persisted graph store on
# each engine.
#
# NornicDB (BadgerDB on-disk layout):
#   *.sst          LSM sorted-string tables — RAW DATA (persisted records)
#   *.vlog         value log — RAW DATA (Badger stores large values here)
#   *.mem          preallocated memtable scratch — SKIP
#   DISCARD        value-log GC scratch — SKIP
#   MANIFEST       LSM manifest — META (small)
#   KEYREGISTRY    encryption key registry — META
#   LOCK           lock file — SKIP
#   wal/           write-ahead log — LOGS
#   snapshots/     point-in-time snapshots — SKIP
#
# Neo4j (record-store layout, version 5+):
#   neostore.*store.db          RAW DATA (nodes, relationships, properties,
#                                         labels, relationship types)
#   neostore.*.db.names         RAW DATA (string tables for tokens/names)
#   neostore                    small root store — META
#   neostore.counts.db          STATS (aggregate counters) — INDEX/STATS
#   neostore.indexstats.db      index statistics — INDEX/STATS
#   *.id                        id-allocation files — META
#   schema/                     native indexes — INDEX
#   transactions/               write-ahead log — LOGS
#   dbms/                       auth + users — META
#   server_id                   identity — META
# ---------------------------------------------------------------------------

# Each bucket: ordered list of (label, matcher). matcher is a callable
# (relpath: str, name: str) -> bool.
def _ext(pattern):
    return lambda rel, name: fnmatch.fnmatch(name, pattern)


def _any_in_path(*parts):
    return lambda rel, name: any(p in rel.split(os.sep) for p in parts)


def _exact(name_pattern):
    return lambda rel, name: fnmatch.fnmatch(name, name_pattern)


NORNIC_RULES = [
    # (bucket, matcher)
    ("skip",       _ext("*.mem")),           # preallocated memtable
    ("skip",       _exact("DISCARD")),        # VLog GC scratch
    ("skip",       _exact("LOCK")),
    ("skip",       _any_in_path("snapshots")),
    ("logs",       _any_in_path("wal")),
    ("raw_data",   _ext("*.sst")),
    ("raw_data",   _ext("*.vlog")),
    ("meta",       _exact("MANIFEST")),
    ("meta",       _exact("KEYREGISTRY")),
]

NEO4J_RULES = [
    ("skip",       _exact("database_lock")),
    ("skip",       _exact("store_lock")),
    ("skip",       _exact("*.tmp.*")),
    ("skip",       _exact("server_id")),
    ("logs",       _any_in_path("transactions")),
    ("meta",       _any_in_path("dbms")),
    ("index",      _any_in_path("schema")),
    ("index",      _exact("neostore.counts.db")),
    ("index",      _exact("neostore.indexstats.db")),
    ("meta",       _ext("*.id")),             # id allocation files
    # Record stores (node, rel, property, label, rel-type, rel-group, schema).
    # Anything else matching neostore* that isn't already captured above is raw data.
    ("raw_data",   _exact("neostore")),       # root store (small but data)
    ("raw_data",   _ext("neostore*.db")),
    ("raw_data",   _ext("neostore*.db.*")),   # .names, .labels, .arrays, .strings, .keys, .index
]


def classify_dir(root: Path, rules) -> dict:
    """Walk root, classify each file into buckets by the first matching rule.

    Uses on-disk allocated size (`st_blocks * 512`) rather than apparent size,
    so sparse files like Badger's preallocated `.vlog` aren't counted for
    space they don't actually occupy.

    Buckets: raw_data, index, logs, meta, other, skip.
    Returns dict of {bucket_name: total_bytes} plus 'files' mapping file→bucket.
    """
    totals = {"raw_data": 0, "index": 0, "logs": 0, "meta": 0, "other": 0, "skip": 0}
    files_by_bucket: dict[str, list[tuple[str, int]]] = {b: [] for b in totals}

    if not root.exists():
        return {"totals": totals, "files": files_by_bucket, "root": str(root)}

    for dirpath, _dirnames, filenames in os.walk(root):
        for fname in filenames:
            full = Path(dirpath) / fname
            try:
                st = full.stat()
            except FileNotFoundError:
                continue
            # On macOS/Linux, st_blocks is 512-byte units of actual disk allocation.
            # Fall back to apparent size if unavailable (e.g. Windows).
            blocks = getattr(st, "st_blocks", None)
            sz = blocks * 512 if blocks is not None else st.st_size
            rel = str(full.relative_to(root))
            bucket = None
            for b, matcher in rules:
                if matcher(rel, fname):
                    bucket = b
                    break
            if bucket is None:
                bucket = "other"
            totals[bucket] += sz
            files_by_bucket[bucket].append((rel, sz))

    return {"totals": totals, "files": files_by_bucket, "root": str(root)}


def parse_powermetrics_plist(path: Path) -> dict:
    """Extract power stats from a stream of plist samples.

    powermetrics -f plist emits one <plist>...</plist> document per sample,
    concatenated. Split on the XML header, parse each, average the per-sample
    power figures.

    Returned dict keys (all milliwatts unless noted):
      samples            number of samples parsed
      duration_seconds   summed hw_model elapsed_ns (fallback: samples * 1s)
      cpu_power_mw_avg   mean of combined_power (or cpu_power) across samples
      gpu_power_mw_avg   mean of gpu_power across samples
      package_power_mw_avg   best-guess total SoC package power
      energy_joules      cpu+gpu energy integrated over the sampling window
    """
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return {"samples": 0, "error": f"missing: {path}"}

    if not raw:
        return {"samples": 0, "error": "empty powermetrics file"}

    # powermetrics prints null bytes between plist documents. Split on \x00
    # first, then fall back to XML header split.
    chunks = [c for c in raw.split(b"\x00") if c.strip()]
    if len(chunks) <= 1:
        # Fall back to splitting on the plist prolog marker.
        marker = b"<?xml"
        parts = raw.split(marker)
        chunks = [marker + p for p in parts if p.strip()]

    samples = []
    for chunk in chunks:
        try:
            samples.append(plistlib.loads(chunk))
        except Exception:
            continue

    if not samples:
        return {"samples": 0, "error": "no parseable plist samples"}

    def pull(d, *keys, default=0.0):
        for k in keys:
            if isinstance(d, dict) and k in d:
                return d[k]
        return default

    cpu_mw = []
    gpu_mw = []
    pkg_mw = []
    elapsed_ns = 0
    for s in samples:
        # Apple Silicon: top-level "processor" dict with nested power readings.
        proc = s.get("processor", {}) if isinstance(s, dict) else {}
        # Fields seen on recent Apple Silicon:
        #   cpu_power, gpu_power, ane_power, combined_power (all mW)
        cpu = pull(proc, "cpu_power", "cpu_energy", default=None)
        gpu = pull(proc, "gpu_power", default=None)
        combined = pull(proc, "combined_power", default=None)
        package = pull(proc, "package_power", default=None)
        # Intel Macs: top-level "all_tasks" has different shape — fall back to
        # hw_model-level package_joules if present.
        if cpu is not None:
            cpu_mw.append(float(cpu))
        if gpu is not None:
            gpu_mw.append(float(gpu))
        if combined is not None:
            pkg_mw.append(float(combined))
        elif package is not None:
            pkg_mw.append(float(package))
        elapsed_ns += int(s.get("elapsed_ns", 0) or 0)

    def avg(xs):
        return sum(xs) / len(xs) if xs else 0.0

    duration_s = elapsed_ns / 1e9 if elapsed_ns else float(len(samples))

    cpu_avg_mw = avg(cpu_mw)
    gpu_avg_mw = avg(gpu_mw)
    pkg_avg_mw = avg(pkg_mw) if pkg_mw else cpu_avg_mw + gpu_avg_mw
    energy_j = (pkg_avg_mw / 1000.0) * duration_s

    return {
        "samples": len(samples),
        "duration_seconds": duration_s,
        "cpu_power_mw_avg": cpu_avg_mw,
        "gpu_power_mw_avg": gpu_avg_mw,
        "package_power_mw_avg": pkg_avg_mw,
        "energy_joules": energy_j,
    }


def parse_vmstat_log(path: Path) -> dict:
    """Parse a vm_stat interval log into memory pressure stats.

    vm_stat <interval> prints a header line with page size, a column-name
    row, then one row of absolute page counts per sample.  State columns
    (free, active, specul, inactive, throttle, wired, prgable, file-backed,
    anonymous, cmprssed, cmprssor) are absolute values on every row; counter
    columns (faults, copy, etc.) are deltas and may carry a K/M suffix on
    the first snapshot row.

    Returns dict with byte values:
      samples, page_size,
      mem_used_avg, mem_used_peak  (active + wired + compressor pages),
      mem_compressed_avg, mem_compressed_peak  (cmprssed pages — logical),
      mem_free_avg, mem_free_min,
      mem_active_avg, mem_wired_avg, mem_compressor_avg
    """
    try:
        text = path.read_text()
    except FileNotFoundError:
        return {"samples": 0, "error": f"missing: {path}"}

    if not text.strip():
        return {"samples": 0, "error": "empty vmstat file"}

    lines = text.strip().splitlines()

    # Extract page size from the header, e.g.
    # "Mach Virtual Memory Statistics: (page size of 16384 bytes)"
    page_size = 16384
    for line in lines[:2]:
        if "page size of" in line:
            import re as _re
            m = _re.search(r"page size of (\d+)", line)
            if m:
                page_size = int(m.group(1))
            break

    # Identify the column-name row and map column indices.
    col_names = None
    col_line_idx = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("free"):
            col_names = stripped.split()
            col_line_idx = i
            break

    if col_names is None:
        return {"samples": 0, "error": "could not find column header row"}

    # Column indices we care about (state columns — absolute page counts).
    want = {"free", "active", "inactive", "wired", "cmprssed", "cmprssor"}
    col_idx = {}
    for idx, name in enumerate(col_names):
        if name in want:
            col_idx[name] = idx

    if not col_idx:
        return {"samples": 0, "error": f"expected columns not found in: {col_names}"}

    # Parse data rows.
    free_pages = []
    active_pages = []
    wired_pages = []
    cmprssed_pages = []
    cmprssor_pages = []
    for line in lines[col_line_idx + 1:]:
        parts = line.split()
        if not parts or len(parts) < len(col_names):
            continue
        try:
            def parse_val(s):
                s = s.rstrip(".")
                if s.endswith("K"):
                    return int(s[:-1]) * 1024
                if s.endswith("M"):
                    return int(s[:-1]) * 1024 * 1024
                return int(s)

            if "free" in col_idx:
                free_pages.append(parse_val(parts[col_idx["free"]]))
            if "active" in col_idx:
                active_pages.append(parse_val(parts[col_idx["active"]]))
            if "wired" in col_idx:
                wired_pages.append(parse_val(parts[col_idx["wired"]]))
            if "cmprssed" in col_idx:
                cmprssed_pages.append(parse_val(parts[col_idx["cmprssed"]]))
            if "cmprssor" in col_idx:
                cmprssor_pages.append(parse_val(parts[col_idx["cmprssor"]]))
        except (ValueError, IndexError):
            continue

    n = len(free_pages)
    if n == 0:
        return {"samples": 0, "error": "no data rows parsed"}

    def avg(xs):
        return sum(xs) / len(xs) if xs else 0.0

    # "used" = active + wired + compressor (resident non-free, non-inactive)
    used = [a + w + c for a, w, c in zip(active_pages, wired_pages, cmprssor_pages)]

    return {
        "samples": n,
        "page_size": page_size,
        "mem_used_avg": avg(used) * page_size,
        "mem_used_peak": max(used) * page_size,
        "mem_compressed_avg": avg(cmprssed_pages) * page_size,
        "mem_compressed_peak": max(cmprssed_pages) * page_size,
        "mem_free_avg": avg(free_pages) * page_size,
        "mem_free_min": min(free_pages) * page_size,
        "mem_active_avg": avg(active_pages) * page_size,
        "mem_wired_avg": avg(wired_pages) * page_size,
        "mem_compressor_avg": avg(cmprssor_pages) * page_size,
    }


def read_int(path: Path, default=0) -> int:
    try:
        return int(path.read_text().strip())
    except Exception:
        return default


def read_float(path: Path, default=0.0) -> float:
    try:
        return float(path.read_text().strip())
    except Exception:
        return default


def load_run(dir_: Path, label: str) -> dict:
    results_path = dir_ / f"{label}.results.json"
    if not results_path.exists():
        raise FileNotFoundError(f"missing {results_path}")
    with results_path.open() as fh:
        results = json.load(fh)
    power = parse_powermetrics_plist(dir_ / f"{label}.powermetrics.plist")
    memory = parse_vmstat_log(dir_ / f"{label}.vmstat.log")
    disk_total = read_int(dir_ / f"{label}.disk_bytes.txt")
    wall_s = read_float(dir_ / f"{label}.wall_seconds.txt")

    # Resolve data directory and classify contents.
    data_dir_path = dir_ / f"{label}.data_dir.txt"
    storage = {"totals": {"raw_data": 0, "index": 0, "logs": 0, "meta": 0, "other": 0, "skip": 0},
               "files": {}, "root": ""}
    if data_dir_path.exists():
        root = Path(data_dir_path.read_text().strip())
        rules = NORNIC_RULES if label.startswith("nornicdb") else NEO4J_RULES
        storage = classify_dir(root, rules)

    return {
        "label": label,
        "results": results,
        "power": power,
        "memory": memory,
        "disk_total_bytes": disk_total,
        "storage": storage,
        "wall_seconds": wall_s,
    }


def human_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024


def fmt_ms(x: float) -> str:
    return f"{x:,.2f}"


def fmt_num(x: float, decimals: int = 2) -> str:
    return f"{x:,.{decimals}f}"


def query_sample_count(query: dict, fallback: int = 0) -> int:
    latencies = query.get("latencies_ms") or []
    return len(latencies) if latencies else int(query.get("iterations", fallback) or fallback)


def benchmark_operations(results: dict) -> int:
    declared = results.get("total_benchmark_operations")
    if declared is not None:
        return int(declared)
    fallback = int(results.get("iterations_per_query", 0) or 0)
    return sum(query_sample_count(query, fallback) for query in results.get("queries", []))


def benchmark_throughput(results: dict) -> dict:
    """Return end-to-end suite rate and query-latency-only rate, in ops/sec."""
    operations = benchmark_operations(results)
    duration_ms = float(results.get("total_benchmark_duration_ms", 0) or 0)
    end_to_end = operations * 1000.0 / duration_ms if operations and duration_ms > 0 else 0.0

    query_duration_ms = 0.0
    fallback_iterations = int(results.get("iterations_per_query", 0) or 0)
    for query in results.get("queries", []):
        latencies = query.get("latencies_ms") or []
        if latencies:
            query_duration_ms += sum(float(sample) for sample in latencies)
        else:
            samples = query_sample_count(query, fallback_iterations)
            query_duration_ms += float(query.get("mean_ms", 0) or 0) * samples
    latency_only = operations * 1000.0 / query_duration_ms if operations and query_duration_ms > 0 else 0.0
    return {
        "operations": operations,
        "duration_ms": duration_ms,
        "end_to_end_ops_per_second": end_to_end,
        "query_latency_ms": query_duration_ms,
        "query_latency_ops_per_second": latency_only,
    }


def seed_rate_per_second(results: dict, count_key: str, duration_key: str = "seed_duration_ms") -> float:
    duration_ms = float(results.get(duration_key, 0) or 0)
    count = float(results.get(count_key, 0) or 0)
    return count * 1000.0 / duration_ms if duration_ms > 0 else 0.0


def ordered_query_names(*query_maps: dict) -> list[str]:
    names = []
    seen = set()
    for query_map in query_maps:
        for name in query_map:
            if name not in seen:
                names.append(name)
                seen.add(name)
    return names


def comparison_configuration_mismatches(nornic: dict, neo4j: dict) -> list[str]:
    mismatches = []
    for field in (
        "categories", "suppliers", "customers", "products", "orders",
        "order_lines_min", "order_lines_max", "random_seed", "seed_batch_size",
        "seed_parallelism", "iterations_per_query", "warmup_iterations",
    ):
        if field in nornic and field in neo4j and nornic[field] != neo4j[field]:
            mismatches.append(f"{field}: NornicDB={nornic[field]} Neo4j={neo4j[field]}")
    n_operations = benchmark_operations(nornic)
    m_operations = benchmark_operations(neo4j)
    if n_operations != m_operations:
        mismatches.append(f"measured operations: NornicDB={n_operations} Neo4j={m_operations}")
    return mismatches


def render_single_report(run: dict, iterations: int, warmup: int, batch_size: int, parallel: int, products: int, orders: int) -> str:
    label = run["label"]
    r = run["results"]
    p = run["power"]
    storage = run["storage"]
    totals = storage["totals"]
    raw = totals["raw_data"]
    disk_total = run["disk_total_bytes"]
    wall = run["wall_seconds"]
    throughput = benchmark_throughput(r)

    display_name = {"nornicdb": "NornicDB", "nornicdb-antlr": "NornicDB (ANTLR parser)", "neo4j": "Neo4j"}.get(label, label)

    lines = []
    lines.append(f"# {display_name} — Northwind Benchmark Report")
    lines.append("")
    lines.append(f"**Run:** `{r.get('started_at', '')}` → `{r.get('finished_at', '')}`")
    lines.append(f"**Endpoint:** `{r.get('uri', '')}` (database `{r.get('database', '')}`)")
    lines.append("")
    lines.append("## Workload")
    lines.append("")
    lines.append(f"- Categories: **{r.get('categories', 0):,}**  |  Suppliers: **{r.get('suppliers', 0):,}**  |  Customers: **{r.get('customers', 0):,}**")
    lines.append(f"- Products seeded: **{r.get('products', products):,}**")
    lines.append(f"- Orders seeded: **{r.get('orders', orders):,}** ({r.get('order_lines_min', 0)}..{r.get('order_lines_max', 0)} lines each)")
    lines.append(f"- Random seed: `{r.get('random_seed', 0)}` (deterministic dataset)")
    lines.append(f"- Seed nodes: **{r.get('seed_nodes', 0):,}**")
    lines.append(f"- Seed relationships: **{r.get('seed_relationships', 0):,}**")
    if r.get("approx_seed_payload_bytes", 0):
        mib = r["approx_seed_payload_bytes"] / (1024 * 1024)
        lines.append(f"- Approx. seed payload (JSON-serialized): **{mib:.1f} MiB**")
    lines.append(f"- Seed duration: **{fmt_ms(r.get('seed_duration_ms', 0))} ms**")
    if "seed_ingestion_ms" in r:
        lines.append(f"- Wipe duration: **{fmt_ms(r['seed_wipe_ms'])} ms**")
        lines.append(f"- Index setup duration: **{fmt_ms(r['seed_index_ms'])} ms**")
        lines.append(f"- Ingestion duration (row generation and writes): **{fmt_ms(r['seed_ingestion_ms'])} ms**")
        rate_duration = "seed_ingestion_ms"
        rate_label = "Ingestion"
    else:
        rate_duration = "seed_duration_ms"
        rate_label = "Seed (total including setup; legacy)"
    lines.append(f"- {rate_label} nodes/sec: **{fmt_num(seed_rate_per_second(r, 'seed_nodes', rate_duration))}**")
    lines.append(f"- {rate_label} relationships/sec: **{fmt_num(seed_rate_per_second(r, 'seed_relationships', rate_duration))}**")
    lines.append(f"- Seed batch size: **{r.get('seed_batch_size', batch_size):,} rows**")
    lines.append(f"- Seed parallelism: **{r.get('seed_parallelism', parallel)} sessions per phase**")
    lines.append(f"- Query workloads: **{len(r.get('queries', []))}**")
    lines.append(f"- Iterations per query: **{r.get('iterations_per_query', iterations)}**")
    lines.append(f"- Warmup iterations per query: **{r.get('warmup_iterations', warmup)}**")
    lines.append("")
    lines.append("## Query Latency")
    lines.append("")
    lines.append("| Query | Description | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for q in r.get("queries", []):
        lines.append(
            f"| `{q['name']}` | {q.get('description', '')} | {query_sample_count(q, iterations)} | "
            f"{fmt_ms(q['mean_ms'])} | {fmt_ms(q['median_ms'])} | "
            f"{fmt_ms(q['p95_ms'])} | {fmt_ms(q['p99_ms'])} | {fmt_ms(q['min_ms'])} | "
            f"{fmt_ms(q['max_ms'])} | {fmt_ms(q['stddev_ms'])} | {fmt_num(q['ops_per_second'])} |"
        )
    lines.append("")
    lines.append(f"- **Overall mean latency:** {fmt_ms(r.get('overall_mean_ms', 0))} ms")
    lines.append(f"- **Measured query operations:** {throughput['operations']:,}")
    lines.append(f"- **End-to-end query-loop throughput:** {fmt_num(throughput['end_to_end_ops_per_second'])} ops/sec")
    lines.append(f"- **Query-latency-only aggregate throughput:** {fmt_num(throughput['query_latency_ops_per_second'])} ops/sec")
    lines.append(f"- **Query-loop duration:** {fmt_num(throughput['duration_ms'] / 1000.0, 3)} s")
    lines.append("- Query-loop duration includes warmups and per-query setup; only measured iterations count toward the end-to-end rate.")
    lines.append(f"- **Full lifecycle wall-clock (sampled):** {fmt_num(wall, 3)} s")
    lines.append("")

    # ---- Correctness (per-engine) ----
    lines.append("## Correctness")
    lines.append("")
    sc = r.get("seed_counts", {})
    if sc:
        lines.append("Seed counts (from the database's own `count(...)` queries):")
        lines.append("")
        lines.append("| Entity | Count |")
        lines.append("|---|---:|")
        for label_txt, key in [
            ("Category", "categories"),
            ("Supplier", "suppliers"),
            ("Customer", "customers"),
            ("Product", "products"),
            ("Order", "orders"),
            ("PART_OF edges", "part_of_edges"),
            ("SUPPLIES edges", "supplies_edges"),
            ("PURCHASED edges", "purchased_edges"),
            ("ORDERS edges", "orders_edges"),
        ]:
            lines.append(f"| {label_txt} | {sc.get(key, 0):,} |")
        lines.append("")
    lines.append("Per-query result fingerprints (SHA-256 over canonicalised rows):")
    lines.append("")
    lines.append("| Query | Rows | Hash | Stable across iterations |")
    lines.append("|---|---:|---|:---:|")
    for q in r.get("queries", []):
        ok = q.get("correctness_ok", True)
        lines.append(
            f"| `{q['name']}` | {q.get('row_count', 0):,} | "
            f"`{q.get('result_hash', '')[:16]}…` | "
            f"{'✅' if ok else '❌ UNSTABLE'} |"
        )
    lines.append("")
    errs = r.get("correctness_errors") or []
    if errs:
        lines.append("> ⚠️ **Correctness errors in this run:**")
        for e in errs:
            lines.append(f"> - {e}")
        lines.append("")
    else:
        lines.append("✅ No intra-run correctness errors.")
        lines.append("")

    lines.append("## Power Consumption")
    lines.append("")
    if p.get("samples", 0) > 0:
        lines.append(f"- Samples collected: **{p['samples']}** (~1s each)")
        lines.append(f"- Sampled duration: **{fmt_num(p['duration_seconds'], 2)} s**")
        lines.append(f"- Avg CPU power: **{fmt_num(p['cpu_power_mw_avg'], 1)} mW**")
        lines.append(f"- Avg GPU power: **{fmt_num(p['gpu_power_mw_avg'], 1)} mW**")
        lines.append(f"- Avg package power: **{fmt_num(p['package_power_mw_avg'], 1)} mW**")
        lines.append(f"- Estimated energy (benchmark window): **{fmt_num(p['energy_joules'], 2)} J**")
    else:
        lines.append(f"- _No powermetrics samples available:_ {p.get('error', 'unknown')}")
    lines.append("")

    mem = run["memory"]
    lines.append("## Memory Pressure")
    lines.append("")
    if mem.get("samples", 0) > 0:
        lines.append(f"- Samples collected: **{mem['samples']}** (~1s each)")
        lines.append(f"- Avg used (active + wired + compressor): **{human_bytes(mem['mem_used_avg'])}**")
        lines.append(f"- Peak used: **{human_bytes(mem['mem_used_peak'])}**")
        lines.append(f"- Avg free: **{human_bytes(mem['mem_free_avg'])}**")
        lines.append(f"- Min free: **{human_bytes(mem['mem_free_min'])}**")
        lines.append(f"- Avg compressed (logical): **{human_bytes(mem['mem_compressed_avg'])}**")
        lines.append(f"- Peak compressed: **{human_bytes(mem['mem_compressed_peak'])}**")
    else:
        lines.append(f"- _No vm_stat samples available:_ {mem.get('error', 'unknown')}")
    lines.append("")
    lines.append("## Storage")
    lines.append("")
    lines.append(f"- **Raw data files:** {human_bytes(raw)} ({raw:,} bytes)")
    lines.append(f"- Indexes/stats: {human_bytes(totals['index'])} ({totals['index']:,} bytes)")
    lines.append(f"- Write-ahead logs: {human_bytes(totals['logs'])} ({totals['logs']:,} bytes)")
    lines.append(f"- Metadata/bookkeeping: {human_bytes(totals['meta'])} ({totals['meta']:,} bytes)")
    lines.append(f"- Preallocated scratch (excluded): {human_bytes(totals['skip'])} ({totals['skip']:,} bytes)")
    lines.append(f"- Unclassified (other): {human_bytes(totals['other'])} ({totals['other']:,} bytes)")
    lines.append(f"- Full data directory `du`: {human_bytes(disk_total)} ({disk_total:,} bytes)")

    # Integrity check: every byte that du sees must land in exactly one
    # bucket. If the classified sum diverges from disk_total by more than
    # 8 KiB (one filesystem block tolerance for race between du and the
    # classifier walk), surface a WARNING in the report so operators know
    # to investigate — most likely a new Badger/Neo4j file type the rules
    # don't cover.
    classified = sum(totals.values())
    delta = classified - disk_total
    lines.append(f"- Classified sum: {human_bytes(classified)} ({classified:,} bytes, Δ vs du = {delta:+,} bytes)")
    if abs(delta) > 8 * 1024:
        lines.append("")
        lines.append(f"> ⚠️ **Classifier/du mismatch:** {delta:+,} bytes. "
                     f"A file type may be uncategorised — inspect the data "
                     f"directory manually and extend NORNIC_RULES / NEO4J_RULES.")
    lines.append("")
    lines.append("_Raw-data size is the comparison headline. Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size._")
    lines.append("")
    # Top contributors in raw_data bucket, for transparency.
    raw_files = sorted(storage["files"].get("raw_data", []), key=lambda x: x[1], reverse=True)[:10]
    if raw_files:
        lines.append("<details><summary>Top raw-data files</summary>")
        lines.append("")
        lines.append("| File | Size |")
        lines.append("|---|---:|")
        for rel, sz in raw_files:
            lines.append(f"| `{rel}` | {human_bytes(sz)} |")
        lines.append("")
        lines.append("</details>")
        lines.append("")
    lines.append("## Queries")
    lines.append("")
    for q in r.get("queries", []):
        lines.append(f"### `{q['name']}`")
        lines.append("")
        lines.append("```cypher")
        lines.append(q["cypher"].strip())
        lines.append("```")
        lines.append("")
    return "\n".join(lines)


def parser_mode_lines(default_run: dict, antlr_run: dict, heading_level: int = 2) -> list[str]:
    """Query latency and throughput of NornicDB's default parser vs NORNICDB_PARSER=antlr.

    Only query latency and throughput are compared. Both runs seed the same graph and run the
    same workload from a freshly wiped data directory, serialized, so the only difference is
    the parser mode. The mode changes the syntax-validation gate in front of the shared
    execution pipeline: the default mode validates with its scannerless scanner and caches
    texts it has accepted, while ANTLR mode revalidates every execution with the ANTLR grammar
    and does not use that cache.
    """
    d_r = default_run["results"]
    a_r = antlr_run["results"]
    d_throughput = benchmark_throughput(d_r)
    a_throughput = benchmark_throughput(a_r)
    h = "#" * heading_level
    lines = [f"{h} NornicDB Parser Modes: default vs ANTLR", ""]
    lines.append(
        "Query latency and throughput of the two `NORNICDB_PARSER` modes over the same seeded Northwind graph "
        "and the same workload. Each mode ran serialized from a freshly wiped data directory. "
        "Seeding, storage, power and memory are not compared here."
    )
    lines.append("")
    mismatches = comparison_configuration_mismatches(d_r, a_r)
    if mismatches:
        lines.append("> **Invalid parser-mode comparison:** benchmark settings or operation counts differ between the runs.")
        for mismatch in mismatches:
            lines.append(f"> - {mismatch}")
        lines.append("")

    def slower(lower_is_better, default_value, antlr_value):
        # "How many times slower is ANTLR mode": latency ratio antlr/default, throughput ratio default/antlr.
        if lower_is_better:
            return f"{antlr_value / default_value:.2f}×" if default_value else "n/a"
        return f"{default_value / antlr_value:.2f}×" if antlr_value else "n/a"

    def pct(new, old):
        return f"{((new - old) / old) * 100:+.1f}%" if old else "n/a"

    lines.append("| Metric | NornicDB (default) | NornicDB (ANTLR) | Delta | ANTLR slower by |")
    lines.append("|---|---:|---:|---:|---:|")
    for name, dv, av, fmt, lower in (
        ("Overall mean latency (ms)", d_r.get("overall_mean_ms", 0), a_r.get("overall_mean_ms", 0), fmt_ms, True),
        ("End-to-end query-loop throughput (ops/sec)", d_throughput["end_to_end_ops_per_second"],
         a_throughput["end_to_end_ops_per_second"], fmt_num, False),
        ("Query-latency-only aggregate throughput (ops/sec)", d_throughput["query_latency_ops_per_second"],
         a_throughput["query_latency_ops_per_second"], fmt_num, False),
        ("Query-loop duration (s)", d_throughput["duration_ms"] / 1000.0, a_throughput["duration_ms"] / 1000.0,
         lambda x: fmt_num(x, 3), True),
    ):
        lines.append(f"| {name} | {fmt(dv)} | {fmt(av)} | {pct(av, dv)} | {slower(lower, dv, av)} |")
    lines.append("")
    lines.append("_Delta = (ANTLR − default) / default. \"ANTLR slower by\" is ANTLR ÷ default for latency and "
                 "default ÷ ANTLR for throughput, so values above 1.00× mean ANTLR mode is slower._")
    lines.append("")

    d_by_name = {q["name"]: q for q in d_r.get("queries", [])}
    a_by_name = {q["name"]: q for q in a_r.get("queries", [])}
    names = ordered_query_names(d_by_name, a_by_name)
    lines.append(f"{h}# Per-query latency")
    lines.append("")
    lines.append("| Query | Mean default (ms) | Mean ANTLR (ms) | Mean × | Median default (ms) | Median ANTLR (ms) | "
                 "P95 default (ms) | P95 ANTLR (ms) | P99 default (ms) | P99 ANTLR (ms) | "
                 "Ops/sec default | Ops/sec ANTLR | Same result |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|")
    parity_failures = []
    for name in names:
        dq = d_by_name.get(name)
        aq = a_by_name.get(name)
        if dq is None or aq is None:
            lines.append(f"| `{name}` | {'not run' if dq is None else fmt_ms(dq.get('mean_ms', 0))} | "
                         f"{'not run' if aq is None else fmt_ms(aq.get('mean_ms', 0))} | — | — | — | — | — | — | — | — | — | ❌ |")
            parity_failures.append(f"{name}: missing in {'default' if dq is None else 'ANTLR'} run")
            continue
        same = dq.get("row_count") == aq.get("row_count") and dq.get("result_hash") == aq.get("result_hash")
        if not same:
            parity_failures.append(f"{name}: default rows={dq.get('row_count')} hash={dq.get('result_hash')} / "
                                   f"ANTLR rows={aq.get('row_count')} hash={aq.get('result_hash')}")
        dm = float(dq.get("mean_ms", 0) or 0)
        am = float(aq.get("mean_ms", 0) or 0)
        lines.append(
            f"| `{name}` | {fmt_ms(dm)} | {fmt_ms(am)} | {slower(True, dm, am)} | "
            f"{fmt_ms(dq.get('median_ms', 0))} | {fmt_ms(aq.get('median_ms', 0))} | "
            f"{fmt_ms(dq.get('p95_ms', 0))} | {fmt_ms(aq.get('p95_ms', 0))} | "
            f"{fmt_ms(dq.get('p99_ms', 0))} | {fmt_ms(aq.get('p99_ms', 0))} | "
            f"{fmt_num(dq.get('ops_per_second', 0))} | {fmt_num(aq.get('ops_per_second', 0))} | {'✅' if same else '❌'} |"
        )
    lines.append("")
    lines.append("_Mean × is ANTLR mean ÷ default mean. \"Same result\" compares each query's row count and SHA-256 result "
                 "fingerprint across the two modes._")
    lines.append("")
    if parity_failures:
        lines.append("> **Result mismatch between parser modes:**")
        for failure in parity_failures:
            lines.append(f"> - {failure}")
        lines.append("")
    else:
        lines.append(f"Both modes returned identical results for all {len(names)} queries.")
        lines.append("")
    return lines


def render_comparison(runs: dict[str, dict], iterations: int, warmup: int, batch_size: int, parallel: int, products: int, orders: int) -> str:
    n = runs.get("nornicdb")
    m = runs.get("neo4j")
    a = runs.get("nornicdb-antlr")  # optional: NORNICDB_PARSER=antlr run, shown in extra rows/sections only

    def pct_delta(new, old):
        if old == 0:
            return "n/a"
        return f"{((new - old) / old) * 100:+.1f}%"

    def ratio(a, b):
        if b == 0:
            return "n/a"
        return f"{a / b:.2f}×"

    lines = []
    lines.append("# NornicDB vs Neo4j — Northwind Benchmark Comparison")
    lines.append("")
    lines.append(f"- Products seeded: **{products:,}**, Orders seeded: **{orders:,}**")
    lines.append(f"- Query workloads: **{len(n['results'].get('queries', []))}** NornicDB / **{len(m['results'].get('queries', []))}** Neo4j")
    lines.append(
        f"- Iterations/query: **{n['results'].get('iterations_per_query', iterations)}** NornicDB "
        f"(**{n['results'].get('warmup_iterations', warmup)} warmup**) / "
        f"**{m['results'].get('iterations_per_query', iterations)}** Neo4j "
        f"(**{m['results'].get('warmup_iterations', warmup)} warmup**)"
    )
    lines.append(
        f"- Seed batches / parallel sessions: **{n['results'].get('seed_batch_size', batch_size)} / "
        f"{n['results'].get('seed_parallelism', parallel)}** NornicDB; **"
        f"{m['results'].get('seed_batch_size', batch_size)} / {m['results'].get('seed_parallelism', parallel)}** Neo4j"
    )
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | NornicDB | Neo4j | Delta | Ratio |")
    lines.append("|---|---:|---:|---:|---:|")

    def row(name, n_val, m_val, fmt=fmt_num, lower_is_better=True):
        delta = pct_delta(n_val, m_val)
        # For throughput (higher is better), flip the ratio perspective in the cell.
        if lower_is_better:
            r = ratio(m_val, n_val)  # how many times slower/larger Neo4j is
        else:
            r = ratio(n_val, m_val)
        lines.append(f"| {name} | {fmt(n_val)} | {fmt(m_val)} | {delta} | {r} |")

    n_r = n["results"]
    m_r = m["results"]
    n_p = n["power"]
    m_p = m["power"]
    n_throughput = benchmark_throughput(n_r)
    m_throughput = benchmark_throughput(m_r)

    configuration_mismatches = comparison_configuration_mismatches(n_r, m_r)
    if configuration_mismatches:
        lines.append("> **Invalid workload comparison:** benchmark settings or operation counts differ between engines.")
        for mismatch in configuration_mismatches:
            lines.append(f"> - {mismatch}")
        lines.append("")

    row("Overall mean latency (ms)", n_r.get("overall_mean_ms", 0), m_r.get("overall_mean_ms", 0), fmt_ms)
    row("End-to-end query-loop throughput (ops/sec)",
        n_throughput["end_to_end_ops_per_second"],
        m_throughput["end_to_end_ops_per_second"],
        fmt_num, lower_is_better=False)
    row("Query-latency-only aggregate throughput (ops/sec)",
        n_throughput["query_latency_ops_per_second"],
        m_throughput["query_latency_ops_per_second"],
        fmt_num, lower_is_better=False)
    row("Query-loop duration (s)", n_throughput["duration_ms"] / 1000.0, m_throughput["duration_ms"] / 1000.0, lambda x: fmt_num(x, 3))
    row("Seed duration (ms)", n_r.get("seed_duration_ms", 0), m_r.get("seed_duration_ms", 0), fmt_ms)
    if "seed_ingestion_ms" in n_r and "seed_ingestion_ms" in m_r:
        for key, label in (("seed_wipe_ms", "Wipe duration (ms)"),
                           ("seed_index_ms", "Index setup duration (ms)"),
                           ("seed_ingestion_ms", "Ingestion duration (ms)")):
            row(label, n_r[key], m_r[key], fmt_ms)
        for count_key, label in (("seed_nodes", "Ingestion nodes/sec"),
                                 ("seed_relationships", "Ingestion relationships/sec")):
            row(label, seed_rate_per_second(n_r, count_key, "seed_ingestion_ms"),
                seed_rate_per_second(m_r, count_key, "seed_ingestion_ms"), fmt_num, lower_is_better=False)
    else:
        row("Seed nodes/sec (total incl. setup, legacy)", seed_rate_per_second(n_r, "seed_nodes"), seed_rate_per_second(m_r, "seed_nodes"), fmt_num, lower_is_better=False)
        row("Seed relationships/sec (total incl. setup, legacy)", seed_rate_per_second(n_r, "seed_relationships"), seed_rate_per_second(m_r, "seed_relationships"), fmt_num, lower_is_better=False)
    row("Avg CPU power (mW)", n_p.get("cpu_power_mw_avg", 0), m_p.get("cpu_power_mw_avg", 0))
    row("Avg GPU power (mW)", n_p.get("gpu_power_mw_avg", 0), m_p.get("gpu_power_mw_avg", 0))
    row("Avg package power (mW)", n_p.get("package_power_mw_avg", 0), m_p.get("package_power_mw_avg", 0))
    row("Energy during benchmark (J)", n_p.get("energy_joules", 0), m_p.get("energy_joules", 0))
    row("Benchmark wall-clock (s)", n["wall_seconds"], m["wall_seconds"])
    row("Peak memory used (bytes)",
        float(n["memory"].get("mem_used_peak", 0)),
        float(m["memory"].get("mem_used_peak", 0)),
        lambda x: human_bytes(int(x)))
    n_raw = n["storage"]["totals"]["raw_data"]
    m_raw = m["storage"]["totals"]["raw_data"]
    row("Raw data files (bytes)", float(n_raw), float(m_raw), lambda x: f"{int(x):,}")

    lines.append("")
    lines.append("_Delta = (NornicDB − Neo4j) / Neo4j. Ratio compares Neo4j to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to Neo4j for throughput (higher is better)._")
    lines.append("_End-to-end query-loop throughput divides measured operations by the full suite window, including warmups and per-query setup; the query-latency-only rate excludes both._")
    lines.append("")
    nornic_by_name = {q["name"]: q for q in n_r.get("queries", [])}
    neo4j_by_name = {q["name"]: q for q in m_r.get("queries", [])}
    antlr_by_name = {q["name"]: q for q in a["results"].get("queries", [])} if a else {}
    query_names = ordered_query_names(nornic_by_name, neo4j_by_name)
    if a:
        lines.extend(parser_mode_lines(n, a))
    lines.append("## Full Query Suite")
    lines.append("")
    lines.append("Each workload is reported independently with all recorded latency percentiles, range, sample count, and per-query rate.")
    lines.append("")
    for name in query_names:
        nq = nornic_by_name.get(name)
        mq = neo4j_by_name.get(name)
        aq = antlr_by_name.get(name)
        query = nq or mq or {}
        lines.append(f"### `{name}`")
        if query.get("description"):
            lines.append("")
            lines.append(query["description"])
        lines.append("")
        lines.append("| Engine | Samples | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec | Rows |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        engine_rows = [
            ("NornicDB", nq, n_r),
            ("Neo4j", mq, m_r),
        ]
        if a:
            engine_rows.append(("NornicDB (ANTLR)", aq, a["results"]))
        for engine, engine_query, run in engine_rows:
            if engine_query is None:
                lines.append(f"| {engine} | not run | — | — | — | — | — | — | — | — | — |")
                continue
            sample_count = query_sample_count(engine_query, int(run.get("iterations_per_query", iterations)))
            lines.append(
                f"| {engine} | {sample_count} | {fmt_ms(engine_query.get('mean_ms', 0))} | "
                f"{fmt_ms(engine_query.get('median_ms', 0))} | {fmt_ms(engine_query.get('p95_ms', 0))} | "
                f"{fmt_ms(engine_query.get('p99_ms', 0))} | {fmt_ms(engine_query.get('min_ms', 0))} | "
                f"{fmt_ms(engine_query.get('max_ms', 0))} | {fmt_ms(engine_query.get('stddev_ms', 0))} | "
                f"{fmt_num(engine_query.get('ops_per_second', 0))} | {engine_query.get('row_count', 0):,} |"
            )
        if nq and mq and float(nq.get("mean_ms", 0) or 0) > 0:
            speed_ratio = float(mq.get("mean_ms", 0) or 0) / float(nq["mean_ms"])
            lines.append(f"\nMean-latency ratio (Neo4j / NornicDB): **{speed_ratio:.2f}×**.")
        if nq and aq and float(nq.get("mean_ms", 0) or 0) > 0:
            lines.append(f"\nMean-latency ratio (NornicDB ANTLR / default): **{float(aq.get('mean_ms', 0) or 0) / float(nq['mean_ms']):.2f}×**.")
        cypher = query.get("cypher", "").strip()
        if cypher:
            lines.extend(["", "<details><summary>Cypher</summary>", "", "```cypher", cypher, "```", "", "</details>"])
        lines.append("")

    # ------------------------------------------------------------------
    # Correctness section — cross-engine diff of seed counts + per-query
    # result fingerprints. This is the defence against "hey look how fast
    # Engine X is" numbers that are actually running against an empty or
    # partially-seeded graph.
    # ------------------------------------------------------------------
    lines.append("## Correctness")
    lines.append("")

    n_seed = n_r.get("seed_counts", {})
    m_seed = m_r.get("seed_counts", {})
    seed_mismatches = []
    lines.append("**Seed verification.** Post-seed counts reported by each database (via `MATCH (n:Label) RETURN count(n)` and equivalent edge queries).")
    lines.append("")
    lines.append("| Entity | NornicDB | Neo4j | Match |")
    lines.append("|---|---:|---:|:---:|")
    seed_rows = [
        ("Category", "categories"),
        ("Supplier", "suppliers"),
        ("Customer", "customers"),
        ("Product", "products"),
        ("Order", "orders"),
        ("PART_OF", "part_of_edges"),
        ("SUPPLIES", "supplies_edges"),
        ("PURCHASED", "purchased_edges"),
        ("ORDERS", "orders_edges"),
    ]
    for label_txt, key in seed_rows:
        nv = n_seed.get(key, 0)
        mv = m_seed.get(key, 0)
        mark = "✅" if nv == mv else "❌"
        if nv != mv:
            seed_mismatches.append(f"{label_txt}: NornicDB={nv} Neo4j={mv}")
        lines.append(f"| {label_txt} | {nv:,} | {mv:,} | {mark} |")
    lines.append("")

    lines.append("**Per-query result fingerprints.** Each engine runs the query on the first (warmup) iteration, canonicalises the full result set, and hashes it with SHA-256. A matching row_count + matching hash means the engines returned the same data.")
    lines.append("")
    lines.append("| Query | NornicDB rows | Neo4j rows | NornicDB hash | Neo4j hash | Match |")
    lines.append("|---|---:|---:|---|---|:---:|")
    fingerprint_mismatches = []
    for name in query_names:
        nq = nornic_by_name.get(name)
        mq = neo4j_by_name.get(name)
        n_rows = nq.get("row_count", -1) if nq else -1
        m_rows = mq.get("row_count", -1) if mq else -1
        n_hash = nq.get("result_hash", "") if nq else ""
        m_hash = mq.get("result_hash", "") if mq else ""
        match = nq is not None and mq is not None and n_rows == m_rows and n_hash == m_hash
        if not match:
            reason = []
            if nq is None:
                reason.append("missing in NornicDB")
            if mq is None:
                reason.append("missing in Neo4j")
            if nq is not None and mq is not None and n_rows != m_rows:
                reason.append(f"rows {n_rows}≠{m_rows}")
            if nq is not None and mq is not None and n_hash != m_hash:
                reason.append("hash differs")
            fingerprint_mismatches.append(f"{name}: {', '.join(reason)}")
        lines.append(
            f"| `{name}` | {f'{n_rows:,}' if nq else 'not run'} | {f'{m_rows:,}' if mq else 'not run'} | "
            f"{f'`{n_hash[:12]}…`' if nq else '—'} | {f'`{m_hash[:12]}…`' if mq else '—'} | "
            f"{'✅' if match else '❌'} |"
        )
    lines.append("")

    # Per-run intra-iteration stability and collected errors.
    n_errs = n_r.get("correctness_errors") or []
    m_errs = m_r.get("correctness_errors") or []
    lines.append("**Intra-run stability.** Every iteration of each query re-fingerprints its result set; a mismatch within a single engine's run is flagged below.")
    lines.append("")
    if not n_errs and not m_errs:
        lines.append("- No intra-run mismatches on either engine.")
    else:
        if n_errs:
            lines.append("- NornicDB:")
            for e in n_errs:
                lines.append(f"  - {e}")
        if m_errs:
            lines.append("- Neo4j:")
            for e in m_errs:
                lines.append(f"  - {e}")
    lines.append("")

    if seed_mismatches or fingerprint_mismatches:
        lines.append("> ⚠️ **Correctness mismatches detected.** Latency numbers below should not be compared until these are investigated.")
        if seed_mismatches:
            lines.append(">")
            lines.append("> Seed count mismatches:")
            for msg in seed_mismatches:
                lines.append(f"> - {msg}")
        if fingerprint_mismatches:
            lines.append(">")
            lines.append("> Result-set mismatches:")
            for msg in fingerprint_mismatches:
                lines.append(f"> - {msg}")
        lines.append("")
    else:
        lines.append("✅ **All correctness checks passed** — both engines seeded identically and returned identical result sets (by row count and canonical SHA-256 fingerprint) for every benchmark query.")
        lines.append("")

    lines.append("## Storage")
    lines.append("")
    n_t = n["storage"]["totals"]
    m_t = m["storage"]["totals"]
    lines.append("Raw data files only (preallocated scratch, WAL, and indexes excluded from the headline):")
    lines.append("")
    lines.append("| Bucket | NornicDB | Neo4j |")
    lines.append("|---|---:|---:|")
    for bucket, label_txt in [
        ("raw_data", "**Raw data**"),
        ("index", "Indexes / stats"),
        ("logs", "Write-ahead logs"),
        ("meta", "Metadata"),
        ("skip", "_Scratch (excluded)_"),
        ("other", "_Unclassified_"),
    ]:
        lines.append(f"| {label_txt} | {human_bytes(n_t[bucket])} ({n_t[bucket]:,} B) | {human_bytes(m_t[bucket])} ({m_t[bucket]:,} B) |")
    lines.append(f"| Total `du` | {human_bytes(n['disk_total_bytes'])} | {human_bytes(m['disk_total_bytes'])} |")
    lines.append("")
    if m_t["raw_data"] > 0:
        factor = n_t["raw_data"] / m_t["raw_data"]
        lines.append(f"- **Raw data ratio:** {factor:.2f}× Neo4j ({'smaller' if factor < 1 else 'larger'})")
    if m["disk_total_bytes"] > 0:
        factor = n["disk_total_bytes"] / m["disk_total_bytes"]
        lines.append(f"- Full-dir ratio (includes scratch/WAL): {factor:.2f}× Neo4j")
    lines.append("")
    lines.append("## Power")
    lines.append("")
    lines.append("| | NornicDB | Neo4j |")
    lines.append("|---|---:|---:|")
    lines.append(f"| Samples | {n_p.get('samples', 0)} | {m_p.get('samples', 0)} |")
    lines.append(f"| Duration (s) | {fmt_num(n_p.get('duration_seconds', 0), 2)} | {fmt_num(m_p.get('duration_seconds', 0), 2)} |")
    lines.append(f"| CPU avg (mW) | {fmt_num(n_p.get('cpu_power_mw_avg', 0), 1)} | {fmt_num(m_p.get('cpu_power_mw_avg', 0), 1)} |")
    lines.append(f"| GPU avg (mW) | {fmt_num(n_p.get('gpu_power_mw_avg', 0), 1)} | {fmt_num(m_p.get('gpu_power_mw_avg', 0), 1)} |")
    lines.append(f"| Package avg (mW) | {fmt_num(n_p.get('package_power_mw_avg', 0), 1)} | {fmt_num(m_p.get('package_power_mw_avg', 0), 1)} |")
    lines.append(f"| Energy (J) | {fmt_num(n_p.get('energy_joules', 0), 2)} | {fmt_num(m_p.get('energy_joules', 0), 2)} |")
    lines.append("")

    n_mem = n["memory"]
    m_mem = m["memory"]
    lines.append("## Memory Pressure")
    lines.append("")
    if n_mem.get("samples", 0) > 0 or m_mem.get("samples", 0) > 0:
        lines.append("System-wide memory during each engine's full lifecycle (startup → benchmark → shutdown).")
        lines.append("")
        lines.append("| | NornicDB | Neo4j |")
        lines.append("|---|---:|---:|")
        lines.append(f"| Samples | {n_mem.get('samples', 0)} | {m_mem.get('samples', 0)} |")
        lines.append(f"| Avg used (active+wired+compressor) | {human_bytes(n_mem.get('mem_used_avg', 0))} | {human_bytes(m_mem.get('mem_used_avg', 0))} |")
        lines.append(f"| Peak used | {human_bytes(n_mem.get('mem_used_peak', 0))} | {human_bytes(m_mem.get('mem_used_peak', 0))} |")
        lines.append(f"| Avg free | {human_bytes(n_mem.get('mem_free_avg', 0))} | {human_bytes(m_mem.get('mem_free_avg', 0))} |")
        lines.append(f"| Min free | {human_bytes(n_mem.get('mem_free_min', 0))} | {human_bytes(m_mem.get('mem_free_min', 0))} |")
        lines.append(f"| Avg compressed (logical) | {human_bytes(n_mem.get('mem_compressed_avg', 0))} | {human_bytes(m_mem.get('mem_compressed_avg', 0))} |")
        lines.append(f"| Peak compressed | {human_bytes(n_mem.get('mem_compressed_peak', 0))} | {human_bytes(m_mem.get('mem_compressed_peak', 0))} |")
    else:
        lines.append("_No vm_stat samples available for either engine._")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Power figures are Apple `powermetrics` estimates; treat as directional, not absolute. Apple's own docs note that reported averages are approximate.")
    lines.append("- Both databases were freshly initialized before each run; Neo4j was stopped during the NornicDB run, and vice versa, to isolate measurements.")
    lines.append("- Benchmarks ran over the Bolt protocol using the neo4j-go-driver.")
    lines.append("- **Storage classification:** NornicDB raw data = `*.sst` + `*.vlog` (LSM records + value log). Neo4j raw data = `neostore*store.db*` (record stores). Preallocated scratch files — Badger's 8 MiB memtable (`*.mem`) and 1 MiB discard log (`DISCARD`), and Neo4j empty `*.id` allocation files — are excluded because their size is fixed/preallocated and does not scale with the dataset.")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, type=Path)
    ap.add_argument("--iterations", type=int, default=30)
    ap.add_argument("--warmup", type=int, default=5)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--parallel", type=int, default=4)
    ap.add_argument("--products", type=int, default=48000)
    ap.add_argument("--orders", type=int, default=48000)
    args = ap.parse_args()

    out_dir: Path = args.dir
    if not out_dir.exists():
        print(f"error: --dir {out_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    runs = {}
    for label in ("nornicdb", "nornicdb-antlr", "neo4j"):
        try:
            runs[label] = load_run(out_dir, label)
        except FileNotFoundError as e:
            print(f"warning: skipping {label}: {e}", file=sys.stderr)

    for label, run in runs.items():
        report = render_single_report(run, args.iterations, args.warmup, args.batch_size, args.parallel, args.products, args.orders)
        (out_dir / f"{label}.md").write_text(report)
        print(f"wrote {out_dir / (label + '.md')}")

    if "nornicdb" in runs and "neo4j" in runs:
        comp = render_comparison(runs, args.iterations, args.warmup, args.batch_size, args.parallel, args.products, args.orders)
        (out_dir / "comparison.md").write_text(comp)
        print(f"wrote {out_dir / 'comparison.md'}")
    else:
        print("note: comparison report skipped (missing one of the runs)", file=sys.stderr)

    if "nornicdb" in runs and "nornicdb-antlr" in runs:
        d_cfg = runs["nornicdb"]["results"]
        header = ["# NornicDB Parser Modes — Northwind Query Latency (default vs ANTLR)", "",
                  f"- Products seeded: **{args.products:,}**, Orders seeded: **{args.orders:,}**",
                  f"- Iterations/query: **{d_cfg.get('iterations_per_query', args.iterations)}** "
                  f"(**{d_cfg.get('warmup_iterations', args.warmup)} warmup**)", ""]
        body = parser_mode_lines(runs["nornicdb"], runs["nornicdb-antlr"], heading_level=2)
        (out_dir / "parser-modes.md").write_text("\n".join(header + body) + "\n")
        print(f"wrote {out_dir / 'parser-modes.md'}")
    elif "nornicdb-antlr" in runs:
        print("note: parser-modes report skipped (missing the default-parser NornicDB run)", file=sys.stderr)


if __name__ == "__main__":
    main()
