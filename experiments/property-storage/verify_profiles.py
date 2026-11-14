"""Validate the saved workload population and regenerate the profiling report."""
import json
import re
from pathlib import Path
from statistics import median

root = Path(__file__).resolve().parent
receipts = root / "profiles"
digest = "2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64"

def load(path):
    data = json.loads((receipts / path).read_text())
    assert data["correctness_ok"]
    assert data["corpus_sha256"] == digest
    assert (data["nodes"], data["edges"], data["mixed_writes"]) == (4352, 8704, 871)
    assert data["gomaxprocs"] == 4 and not data["sync_writes"] and not data["low_memory"]
    assert data["edge_workers"] == data["phases"]["create_edge"]["workers"] == 1
    for name, phase in data["phases"].items():
        assert phase["operations"] > 0 and phase["conflict_retries"] >= 0
        assert 0 <= phase["p50_ns"] <= phase["p95_ns"] <= phase["p99_ns"]
        if name != "create_edge":
            assert phase["workers"] == data["workers"]
    return data

old = {}
selected = {}
for workers in (1, 4, 8, 16):
    for flag in (False, True):
        label = str(flag).lower()
        selected[workers, flag] = [load(f"selected/{label}-{workers}-{r}.json") for r in (1, 2, 3)]
        for data in selected[workers, flag]:
            assert data["enabled"] == flag and data["workers"] == workers
        for build in ("before", "final"):
            series = [load(f"concurrency/{build}-{label}-{workers}-{r}.json") for r in (1, 2, 3)]
            assert all(d["enabled"] == flag and d["workers"] == workers for d in series)
            old[build, workers, flag] = series

def value(series, key):
    return median(d[key] for d in series)

def phase(series, name, key):
    return median(d["phases"][name][key] for d in series)

lines = ["# Storage latency and concurrency profile", "",
"Priority: latency and concurrency, then storage size, then memory. All new storage behavior remains behind the single default-off `NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED` flag.", "",
"## Measured result", "",
"Three fresh processes per configuration, 1/4/8/16 closed-loop clients. The previous build is Nornic `7a2055be4` with Badger `adf08d82495b`. The selected build uses Badger `4af841ebc969`, bounded property-compression scratch/window, direct block restoration and the native-integer fast path. The original adaptive Zstd SST policy is retained.", "",
"| Clients | Flag off writes/s | Previous on writes/s | Selected on writes/s | Previous p95, us | Selected p95, us | Selected p99, us |",
"|---:|---:|---:|---:|---:|---:|---:|"]
for w in (1, 4, 8, 16):
    a, b, c = selected[w, False], old["before", w, True], selected[w, True]
    lines.append(f"| {w} | {phase(a,'create_node','ops_second'):,.0f} | {phase(b,'create_node','ops_second'):,.0f} | {phase(c,'create_node','ops_second'):,.0f} | {phase(b,'create_node','p95_ns')/1000:,.1f} | {phase(c,'create_node','p95_ns')/1000:,.1f} | {phase(c,'create_node','p99_ns')/1000:,.1f} |")

lines += ["", "These are embedded storage API timings, not HTTP/Bolt/Cypher end-to-end latency. Fsync-per-write is disabled in both compared configurations, matching `NewBadgerEngine` defaults; this is not a durable-per-write latency claim. The earlier forced-fsync calibration lives in `before-[1-3]-*.json` and is a different test-binary/low-memory workload.", "",
"## Longer warm-read comparison at eight clients", "",
"Each timed read phase executes 128 passes (557,056 calls), after correctness checks have warmed engine caches. Three independent processes per build. The shorter three-pass matrix is retained as exploratory evidence, not a cold-I/O result.", "",
"| Phase | Previous on ops/s | Selected on ops/s | Previous p95, us | Selected p95, us | Selected p99, us |",
"|---|---:|---:|---:|---:|---:|"]
sustained_before = [load(f"sustained/before-{r}.json") for r in (1, 2, 3)]
sustained_selected = [load(f"selected/sustained-{r}.json") for r in (1, 2, 3)]
for build in ("before","final","zstd","off"):
    series=[load(f"sustained/{build}-{r}.json") for r in (1,2,3)]
    assert all(d["workers"]==8 and d["read_rounds"]==128 and d["enabled"]==(build!="off") for d in series)
for data in sustained_before + sustained_selected:
    assert data["workers"] == 8 and data["read_rounds"] == 128
for name in ("projected_read", "full_read", "adjacency_read", "mixed_80_read_20_write"):
    lines.append(f"| {name} | {phase(sustained_before,name,'ops_second'):,.0f} | {phase(sustained_selected,name,'ops_second'):,.0f} | {phase(sustained_before,name,'p95_ns')/1000:,.1f} | {phase(sustained_selected,name,'p95_ns')/1000:,.1f} | {phase(sustained_selected,name,'p99_ns')/1000:,.1f} |")
lines += ["", "The selected build improves node-write and mixed-workload results, but it is not an across-the-board latency win: the longer projected-read throughput is lower than the prior build, and single-client writes still trail flag-off substantially. Three repeats are not a statistical non-regression bound. Keep the feature experimental and off by default."]
lines += ["", "Mixed traffic is 80% projected reads and 20% creates in the already initialized namespace; it has 4,352 operations, not 128 read passes. Returned errors fail the process. Retryable uncommitted conflicts have a 32-attempt budget with bounded exponential backoff/jitter; accepted/uncertain commits are never retried. Retry counts are in every phase receipt, and waits are inside reported latency.", "",
"## Storage and memory at eight clients", "",
"Medians from the three-pass matrix. File sizes include every file after clean close, including graph, indexes and MVCC metadata. Peak RSS includes fixture construction, checks and all phases. Go allocations exclude initial fixture creation but include correctness checks; they are not allocations per API call.", "",
"| Metric | Flag off | Previous on | Selected on |", "|---|---:|---:|---:|"]
for key, label, divisor in (("file_bytes","Closed-store MiB",2**20),("peak_rss_kib","Peak RSS MiB",1024),("live_heap_bytes","Post-GC live heap MiB",2**20),("allocated_bytes","Cumulative allocated MiB",2**20)):
    vals = [value(series,key)/divisor for series in (selected[8,False],old["before",8,True],selected[8,True])]
    lines.append("| %s | %.2f | %.2f | %.2f |" % (label,*vals))
lines += ["", "## Northwind server check", "",
"The repository script seeds 97,440 nodes and 312,050 relationships, using batches of 2,000 and four seed clients, then runs 14 queries with 30 iterations/five warmups. Both parser modes use fresh stores, graph-only indexes, `NORNIC_ASYNC_WRITES=0` and identical settings. This is one pair per parser, not a statistical latency bound. Query means include the existing warm result-cache behavior; benchmark fingerprints normalize floating values to 12 significant digits.", "",
"| Parser | Seed off/on, s | Mean query off/on, ms | Allocated disk off/on, MiB |", "|---|---:|---:|---:|"]
for parser in ("nornicdb","nornicdb-antlr"):
    a=json.loads((receipts/"northwind"/"off"/(parser+".results.json")).read_text())
    b=json.loads((receipts/"northwind"/"on"/(parser+".results.json")).read_text())
    assert a["seed_nodes"]==b["seed_nodes"]==97440 and a["seed_relationships"]==b["seed_relationships"]==312050
    assert len(a["queries"])==len(b["queries"])==14
    for x,y in zip(a["queries"],b["queries"]):
        assert x["correctness_ok"] and y["correctness_ok"]
        assert (x["name"],x["row_count"],x["result_hash"])==(y["name"],y["row_count"],y["result_hash"])
    off=int((receipts/"northwind"/"off"/(parser+".disk_bytes.txt")).read_text())
    on=int((receipts/"northwind"/"on"/(parser+".disk_bytes.txt")).read_text())
    lines.append(f"| {parser} | {a['seed_duration_ms']/1000:.3f} / {b['seed_duration_ms']/1000:.3f} | {a['overall_mean_ms']:.4f} / {b['overall_mean_ms']:.4f} | {off/2**20:.2f} / {on/2**20:.2f} |")
lines += ["", "All 14 row counts and fingerprints match in each pair. Disk reduction is about 19%, but seeding is 2–4% slower and warm query means are higher in this single pair. This is a storage win, not evidence of a Northwind latency win. The profile includes both stronger table compression and transposition; an ordinary-Zstd-only control is still required to attribute savings specifically to the column transform.","",
"## What changed", "",
"- Reuse at most 128 KiB of output scratch per property encoder. Accepted payload bytes remain caller-owned; rejected high-entropy attempts avoid an input-sized allocation on every call.",
"- Limit independent property Zstd history to 128 KiB, instead of retaining multi-MiB histories per pooled encoder.",
"- Stop the second compression contest when a native integer column already saves at least 75%, preserving exact int64 values and the existing 12.5% acceptance floor.",
"- Restore SST blocks directly into Badger's destination allocation, removing the extra restored-block buffer/copy. Byte layout, checksums and logical version visibility remain unchanged.", "",
"The Snappy table candidate approximately halved the block-encode microbenchmark, but the longer public-API comparison favored the optimized Zstd variant on mixed-workload throughput and storage. Snappy was rejected; its raw results remain in `concurrency`, `sustained/final-*`, `table-fast.txt` and `rejected-fast-policy.txt`. Microbenchmark improvements alone did not decide the implementation.", "",
"## Isolated codec CPU and allocations", "",
"Three 500 ms repetitions, one CPU; medians. Property results use Nornic's dependency selection (compress v1.19.1). Table results use the fork's own module (compress v1.18.0). These are separate workloads; do not combine their timings into server latency.", "",
"| Property encode | Previous ns/op | Selected ns/op | Previous B/op | Selected B/op |", "|---|---:|---:|---:|---:|"]
def bench(file,name):
    text=(receipts/file).read_text()
    found=re.findall(re.escape(name)+r"\s+\d+\s+(\d+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op",text)
    assert len(found)==3,(file,name,found)
    return [median(int(row[i]) for row in found) for i in range(3)]
for name in ("text","random-bytes","integer-array"):
    before=bench("property-before.txt","BenchmarkExperimentalPropertyEncode/"+name)
    after=bench("property-final.txt","BenchmarkExperimentalPropertyEncode/"+name)
    lines.append(f"| {name} | {before[0]:,.0f} | {after[0]:,.0f} | {before[1]:,.0f} | {after[1]:,.0f} |")
before=bench("table-before.txt","BenchmarkColumnarPhysicalCodec/decode")
after=bench("table-selected.txt","BenchmarkColumnarPhysicalCodec/decode")
lines += ["",f"The real-SST decode microbenchmark changes from {before[0]:,.0f} to {after[0]:,.0f} ns/op and {before[1]:,.0f} to {after[1]:,.0f} B/op. It checks byte equality and includes the reader's destination allocation.","",
"## Limits and discovered failures", "",
"- Concurrent shared-endpoint edge creates exhaust the bounded retry budget in the unchanged flag-off baseline. `concurrency-initial-failure.txt` records a four-client failure after 32 attempts. This is not qualified as working. `-edge-workers 4` reproduces that workload; comparison runs explicitly seed edges with one worker. Concurrent adjacency reads are included.",
"- Standalone measurements avoid the test fixture's goroutine/lock diagnostics. Earlier test-binary CPU/allocation profiles are retained for diagnosis, separately from the primary public-API results.",
"- Full suites remain deferred at the maintainer's request. Focused race checks cover fork codecs/tables, Nornic configuration/storage/WAL and the Cypher regression in both parser modes. Check the saved logs for actual completion.",
"- Unfiltered fork-root `go vet` reports upstream protobuf lock-copy warnings in publisher code/tests. The old baseline produces the same warnings; affected leaf-package vet is checked separately. These warnings are not a passing root-vet result.",
"- Whole SST blocks are still reconstructed. Independently addressable compressed columns, physical property-range reads and a vectorized CSR executor are not implemented.", "",
"## Workload and reproduction", "",
"Intel i9-9900KF, Linux Docker laboratory limited to four CPUs and 10 GiB, Go 1.27.3, `noui,nolocalllm`, default Badger engine memory profile. Runs execute serially without other benchmark or test jobs. No new runtime feature flag was introduced.", "",
"The corpus is the 272 Markdown files from release commit `ddc59dc20`, archived with Git (LF bytes). Corpus digest: `"+digest+"`. Sixteen replicas produce 4,352 nodes, each carrying document text, stable keys, 256 low-range int64 metrics and independent 4 KiB hash-derived bytes. The graph adds 8,704 relationships and 871 mixed-phase nodes. Text repeats across replicas intentionally; arbitrary high-entropy data does not become compressible by assertion.", "",
"```sh", "mkdir -p /tmp/frozen-nornic-corpus", "git archive ddc59dc20 docs | tar -x -C /tmp/frozen-nornic-corpus",
"GOWORK=off go build -tags noui,nolocalllm -o /tmp/storage-profile ./experiments/property-storage/profile",
"NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED=true /tmp/storage-profile -docs /tmp/frozen-nornic-corpus/docs -workers 8 -out /tmp/on.json",
"# Use a separate process and fresh temporary store for every case; repeat with false.",
"# Add -read-rounds 128 for the sustained read check; -sync-writes for forced fsync.",
"# Add -cpu /tmp/on.cpu -heap /tmp/on.heap only for separate diagnostic runs.",
"python experiments/property-storage/verify_profiles.py", "```", "",
"## Architectural reference", "",
"[Ladybug's primary repository](https://github.com/LadybugDB/ladybug) describes columnar storage, CSR adjacency and vectorized/factorized execution. The [Kuzu CIDR paper](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf) provides the original read-optimized design context. These suggest future contiguous adjacency chunks, bounded vector batches and short publication sections; they are not evidence that this SST transform implements those features or outperforms Ladybug. Its [current transaction manager](https://github.com/LadybugDB/ladybug/blob/main/src/transaction/transaction_manager.cpp) explicitly coordinates writes and publication, so analytical-layout results alone do not establish multi-writer performance.", ""]
(root/"PROFILE.md").write_text("\n".join(lines),encoding="utf-8")
print("Verified 48 exploratory cases, 24 selected cases and longer read comparisons.")
