#!/usr/bin/env python3
"""Render the ANN benchmark results (run.py) as Markdown, in the shape of the Northwind reports.

Writes, next to the results:
  comparison.md  NornicDB vs Qdrant per mode (N/A where an engine has no equivalent), the
                 hnsw_ef sweeps side by side, and build/footprint figures
  nornicdb.md    every NornicDB run in detail
  qdrant.md      every Qdrant run in detail
"""
import argparse
import json
import platform
import subprocess
from pathlib import Path

ENGINES = ("nornicdb", "qdrant")
LABEL = {"nornicdb": "NornicDB", "qdrant": "Qdrant"}


def load(results_dir: Path):
    runs = {}
    for path in sorted(results_dir.glob("*.json")):
        data = json.loads(path.read_text())
        if "engine" not in data or "mode" not in data:  # environment.json, corpus.json
            continue
        runs[(data["engine"], data["mode"])] = data
    return runs


def default_run(result):
    return next(r for r in result["runs"] if r.get("hnsw_ef") is None)


def fmt(value, spec, na="N/A"):
    return na if value is None else format(value, spec)


def mb(n):
    return f"{n / 1e6:,.0f} MB"


def machine():
    try:
        chip = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()
        mem = int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout.strip() or 0)
        cores = subprocess.run(["sysctl", "-n", "hw.ncpu"], capture_output=True, text=True).stdout.strip()
        return f"{chip}, {cores} cores, {mem / 2**30:.0f} GiB, {platform.system()} {platform.release()}"
    except Exception:  # noqa: BLE001
        return platform.platform()


def header(env, corpus, k):
    lines = [
        f"- **Corpus:** {corpus['vectors']:,} vectors × {corpus['dimensions']} dimensions ({corpus['model']}, {corpus['metric']}), "
        f"chunks of {', '.join(corpus['roots'])} ({corpus['kinds'].get('doc', 0):,} documentation, "
        f"{corpus['kinds'].get('code', 0):,} code); {env['queries']:,} held-out queries; exact top-{corpus['truth_k']} ground truth.",
        f"- **Measured:** recall@{k} against exact ground truth; single-client latency over {env['queries']:,} queries "
        f"after {env['warmup']} warm-up; throughput with {env['concurrency']} concurrent clients.",
        f"- **Engines:** NornicDB {env['nornic_version']} (`{env['nornic_commit']}`), Qdrant {env['qdrant_version']} "
        f"(native binary); one server at a time, each on a fresh data directory, both driven over the Qdrant gRPC API "
        f"(Upsert batches of {env['batch']}, Search).",
        f"- **Machine:** {env['machine']}.",
        f"- **NornicDB GPU paths:** {'on' if env['nornic_gpu'] else 'off (CPU only, as Qdrant)'}.",
    ]
    return lines


def comparison(runs, modes, env, corpus):
    k = env["k"]
    out = ["# Vector ANN benchmark: NornicDB vs Qdrant", ""] + header(env, corpus, k) + [""]
    out += ["## Summary", "",
            f"Each mode at its server default (no `hnsw_ef`). N/A: the engine has no equivalent mode.", "",
            f"| Mode | NornicDB recall@{k} | NornicDB p50 | NornicDB p95 | NornicDB QPS | Qdrant recall@{k} | Qdrant p50 | Qdrant p95 | Qdrant QPS |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for mode in modes:
        cells = [mode["title"]]
        for engine in ENGINES:
            result = runs.get((engine, mode["name"]))
            if result is None:
                cells += ["N/A"] * 4
                continue
            r = default_run(result)
            cells += [fmt(r[f"recall@{k}"], ".4f"), fmt(r["p50_ms"], ".3f") + " ms", fmt(r["p95_ms"], ".3f") + " ms", fmt(r["qps"], ",.0f")]
        out.append("| " + " | ".join(cells) + " |")

    sweep_modes = [m for m in modes if any(len(runs.get((e, m["name"]), {}).get("runs", [])) > 1 for e in ENGINES)]
    if sweep_modes:
        out += ["", "## `hnsw_ef` sweeps", "",
                "The same Search parameter on both engines. Without `hnsw_ef` (server default), NornicDB searches with "
                "its preset's efSearch, raised to the result count; Qdrant with its own default ef.", ""]
        for mode in sweep_modes:
            out += [f"### {mode['title']}", "",
                    f"| hnsw_ef | NornicDB recall@{k} | NornicDB p50 | NornicDB QPS | Qdrant recall@{k} | Qdrant p50 | Qdrant QPS |",
                    "|---|---:|---:|---:|---:|---:|---:|"]
            efs = []
            for engine in ENGINES:
                for r in runs.get((engine, mode["name"]), {}).get("runs", []):
                    if r["hnsw_ef"] not in efs:
                        efs.append(r["hnsw_ef"])
            for ef in efs:
                cells = ["server default" if ef is None else str(ef)]
                for engine in ENGINES:
                    result = runs.get((engine, mode["name"]))
                    r = next((x for x in result["runs"] if x["hnsw_ef"] == ef), None) if result else None
                    if r is None:
                        cells += ["N/A"] * 3
                    else:
                        cells += [fmt(r[f"recall@{k}"], ".4f"), fmt(r["p50_ms"], ".3f") + " ms", fmt(r["qps"], ",.0f")]
                out.append("| " + " | ".join(cells) + " |")
            out.append("")

    out += ["## Build and footprint", "",
            "| Mode | Engine | Strategy | Load | Load rate | Index ready | Disk | RSS after queries |",
            "|---|---|---|---:|---:|---:|---:|---:|"]
    for mode in modes:
        for engine in ENGINES:
            result = runs.get((engine, mode["name"]))
            if result is None:
                continue
            out.append(f"| {mode['title']} | {LABEL[engine]} | {result['strategy']} | {result['load_s']:.1f} s | "
                       f"{result['load_vectors_per_s']:,.0f} vec/s | {result['index_ready_s']:.1f} s | {mb(result['disk_bytes'])} | "
                       f"{mb(result['rss_bytes_after_queries'])} |")
    out += ["", "## Notes", "",
            "- **Strategy** is what the engine reports: NornicDB's from its `Vector search strategy:` log line, Qdrant's from the collection configuration.",
            "- **Index ready**: NornicDB's search index built and its first search answered; Qdrant's collection green with every vector indexed "
            "(`indexing_threshold` is 1 KB so every segment gets its HNSW index).",
            "- **Disk** counts allocated blocks under the data directory after the load; **RSS** is the server process after the queries.",
            "- NornicDB's quantization is IVF-PQ (`compressed`); Qdrant quantizes on top of HNSW. The modes are listed separately, not as equivalents.",
            ""]
    return "\n".join(out)


def engine_report(engine, runs, modes, env, corpus):
    k = env["k"]
    out = [f"# {LABEL[engine]}: vector ANN benchmark", ""] + header(env, corpus, k) + [""]
    for mode in modes:
        result = runs.get((engine, mode["name"]))
        if result is None:
            continue
        out += [f"## {mode['title']}", "",
                f"- **Strategy:** {result['strategy']}",
                f"- **Load:** {result['load_s']:.1f} s ({result['load_vectors_per_s']:,.0f} vectors/s); "
                f"**index ready:** {result['index_ready_s']:.1f} s; **disk:** {mb(result['disk_bytes'])}; "
                f"**RSS:** {mb(result['rss_bytes_after_load'])} after load, {mb(result['rss_bytes_after_queries'])} after queries",
                "",
                "| hnsw_ef | recall@1 | recall@10 | mean | p50 | p95 | p99 | QPS |",
                "|---|---:|---:|---:|---:|---:|---:|---:|"]
        for r in result["runs"]:
            out.append(f"| {'server default' if r['hnsw_ef'] is None else r['hnsw_ef']} | {fmt(r.get('recall@1'), '.4f')} | "
                       f"{fmt(r.get(f'recall@{k}'), '.4f')} | {r['mean_ms']:.3f} ms | {r['p50_ms']:.3f} ms | {r['p95_ms']:.3f} ms | "
                       f"{r['p99_ms']:.3f} ms | {r['qps']:,.0f} |")
        out.append("")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", type=Path, required=True)
    ap.add_argument("--corpus", type=Path, required=True)
    args = ap.parse_args()
    import run  # the mode list is the runner's
    runs = load(args.results)
    env = json.loads((args.results / "environment.json").read_text())
    env.setdefault("machine", machine())
    corpus = json.loads((args.corpus / "meta.json").read_text())
    modes = [m for m in run.MODES if any((e, m["name"]) in runs for e in ENGINES)]
    (args.results / "comparison.md").write_text(comparison(runs, modes, env, corpus))
    for engine in ENGINES:
        if any(e == engine for e, _ in runs):
            (args.results / f"{engine}.md").write_text(engine_report(engine, runs, modes, env, corpus))
    print(f"wrote {args.results / 'comparison.md'}")


if __name__ == "__main__":
    main()
