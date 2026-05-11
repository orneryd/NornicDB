#!/usr/bin/env python3
"""Generate Northwind-benchmark reports for NornicDB and Neo4j.

Inputs (produced by scripts/benchmark_northwind_vs_neo4j.sh in --dir):
  <label>.results.json         per-DB benchmark JSON (from northwind_power)
  <label>.powermetrics.plist   concatenated Apple `powermetrics -f plist` samples
  <label>.disk_bytes.txt       on-disk bytes for data directory
  <label>.wall_seconds.txt     wall-clock seconds for the sampled window

Outputs (written into --dir):
  nornicdb.md    NornicDB-only report
  neo4j.md       Neo4j-only report
  comparison.md  side-by-side comparison
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
    disk_total = read_int(dir_ / f"{label}.disk_bytes.txt")
    wall_s = read_float(dir_ / f"{label}.wall_seconds.txt")

    # Resolve data directory and classify contents.
    data_dir_path = dir_ / f"{label}.data_dir.txt"
    storage = {"totals": {"raw_data": 0, "index": 0, "logs": 0, "meta": 0, "other": 0, "skip": 0},
               "files": {}, "root": ""}
    if data_dir_path.exists():
        root = Path(data_dir_path.read_text().strip())
        rules = NORNIC_RULES if label == "nornicdb" else NEO4J_RULES
        storage = classify_dir(root, rules)

    return {
        "label": label,
        "results": results,
        "power": power,
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


def render_single_report(run: dict, iterations: int, products: int, orders: int) -> str:
    label = run["label"]
    r = run["results"]
    p = run["power"]
    storage = run["storage"]
    totals = storage["totals"]
    raw = totals["raw_data"]
    disk_total = run["disk_total_bytes"]
    wall = run["wall_seconds"]

    display_name = {"nornicdb": "NornicDB", "neo4j": "Neo4j"}.get(label, label)

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
    lines.append(f"- Iterations per query: **{r.get('iterations_per_query', iterations)}**")
    lines.append("")
    lines.append("## Query Latency")
    lines.append("")
    lines.append("| Query | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for q in r.get("queries", []):
        lines.append(
            f"| `{q['name']}` | {fmt_ms(q['mean_ms'])} | {fmt_ms(q['median_ms'])} | "
            f"{fmt_ms(q['p95_ms'])} | {fmt_ms(q['p99_ms'])} | {fmt_ms(q['min_ms'])} | "
            f"{fmt_ms(q['max_ms'])} | {fmt_ms(q['stddev_ms'])} | {fmt_num(q['ops_per_second'])} |"
        )
    lines.append("")
    lines.append(f"- **Overall mean latency:** {fmt_ms(r.get('overall_mean_ms', 0))} ms")
    lines.append(f"- **Overall throughput:** {fmt_num(r.get('overall_ops_per_second', 0))} ops/sec")
    lines.append(f"- **Total benchmark wall-clock (sampled):** {fmt_num(wall, 3)} s")
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
    lines.append("## Storage")
    lines.append("")
    lines.append(f"- **Raw data files:** {human_bytes(raw)} ({raw:,} bytes)")
    lines.append(f"- Indexes/stats: {human_bytes(totals['index'])} ({totals['index']:,} bytes)")
    lines.append(f"- Write-ahead logs: {human_bytes(totals['logs'])} ({totals['logs']:,} bytes)")
    lines.append(f"- Metadata/bookkeeping: {human_bytes(totals['meta'])} ({totals['meta']:,} bytes)")
    lines.append(f"- Preallocated scratch (excluded): {human_bytes(totals['skip'])} ({totals['skip']:,} bytes)")
    lines.append(f"- Unclassified (other): {human_bytes(totals['other'])} ({totals['other']:,} bytes)")
    lines.append(f"- Full data directory `du`: {human_bytes(disk_total)} ({disk_total:,} bytes)")
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


def render_comparison(runs: dict[str, dict], iterations: int, products: int, orders: int) -> str:
    n = runs.get("nornicdb")
    m = runs.get("neo4j")

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
    lines.append(f"- Iterations per query: **{iterations}** (after 2 warmup iterations)")
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

    row("Overall mean latency (ms)", n_r.get("overall_mean_ms", 0), m_r.get("overall_mean_ms", 0), fmt_ms)
    row("Overall throughput (ops/sec)",
        n_r.get("overall_ops_per_second", 0),
        m_r.get("overall_ops_per_second", 0),
        fmt_num, lower_is_better=False)
    row("Seed duration (ms)", n_r.get("seed_duration_ms", 0), m_r.get("seed_duration_ms", 0), fmt_ms)
    row("Avg CPU power (mW)", n_p.get("cpu_power_mw_avg", 0), m_p.get("cpu_power_mw_avg", 0))
    row("Avg GPU power (mW)", n_p.get("gpu_power_mw_avg", 0), m_p.get("gpu_power_mw_avg", 0))
    row("Avg package power (mW)", n_p.get("package_power_mw_avg", 0), m_p.get("package_power_mw_avg", 0))
    row("Energy during benchmark (J)", n_p.get("energy_joules", 0), m_p.get("energy_joules", 0))
    row("Benchmark wall-clock (s)", n["wall_seconds"], m["wall_seconds"])
    n_raw = n["storage"]["totals"]["raw_data"]
    m_raw = m["storage"]["totals"]["raw_data"]
    row("Raw data files (bytes)", float(n_raw), float(m_raw), lambda x: f"{int(x):,}")

    lines.append("")
    lines.append("_Delta = (NornicDB − Neo4j) / Neo4j. Ratio compares Neo4j to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to Neo4j for throughput (higher is better)._")
    lines.append("")
    lines.append("## Per-Query Latency")
    lines.append("")
    lines.append("| Query | NornicDB mean (ms) | Neo4j mean (ms) | Delta | NornicDB P95 | Neo4j P95 | NornicDB ops/s | Neo4j ops/s |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")

    nornic_by_name = {q["name"]: q for q in n_r.get("queries", [])}
    neo4j_by_name = {q["name"]: q for q in m_r.get("queries", [])}
    for name in nornic_by_name:
        nq = nornic_by_name[name]
        mq = neo4j_by_name.get(name, {"mean_ms": 0, "p95_ms": 0, "ops_per_second": 0})
        lines.append(
            f"| `{name}` | {fmt_ms(nq['mean_ms'])} | {fmt_ms(mq['mean_ms'])} | "
            f"{pct_delta(nq['mean_ms'], mq['mean_ms'])} | "
            f"{fmt_ms(nq['p95_ms'])} | {fmt_ms(mq['p95_ms'])} | "
            f"{fmt_num(nq['ops_per_second'])} | {fmt_num(mq['ops_per_second'])} |"
        )
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
    ap.add_argument("--iterations", type=int, default=10)
    ap.add_argument("--products", type=int, default=2000)
    ap.add_argument("--orders", type=int, default=2000)
    args = ap.parse_args()

    out_dir: Path = args.dir
    if not out_dir.exists():
        print(f"error: --dir {out_dir} does not exist", file=sys.stderr)
        sys.exit(1)

    runs = {}
    for label in ("nornicdb", "neo4j"):
        try:
            runs[label] = load_run(out_dir, label)
        except FileNotFoundError as e:
            print(f"warning: skipping {label}: {e}", file=sys.stderr)

    for label, run in runs.items():
        report = render_single_report(run, args.iterations, args.products, args.orders)
        (out_dir / f"{label}.md").write_text(report)
        print(f"wrote {out_dir / (label + '.md')}")

    if "nornicdb" in runs and "neo4j" in runs:
        comp = render_comparison(runs, args.iterations, args.products, args.orders)
        (out_dir / "comparison.md").write_text(comp)
        print(f"wrote {out_dir / 'comparison.md'}")
    else:
        print("note: comparison report skipped (missing one of the runs)", file=sys.stderr)


if __name__ == "__main__":
    main()
