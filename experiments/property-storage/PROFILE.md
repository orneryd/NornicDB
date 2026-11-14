# Storage latency and concurrency profile

Priority: latency and concurrency, then storage size, then memory. All new storage behavior remains behind the single default-off `NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED` flag.

## Measured result

Three fresh processes per configuration, 1/4/8/16 closed-loop clients. The previous build is Nornic `7a2055be4` with Badger `adf08d82495b`. The selected build uses Badger `4af841ebc969`, bounded property-compression scratch/window, direct block restoration and the native-integer fast path. The original adaptive Zstd SST policy is retained.

| Clients | Flag off writes/s | Previous on writes/s | Selected on writes/s | Previous p95, us | Selected p95, us | Selected p99, us |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 6,874 | 4,122 | 4,485 | 454.5 | 432.2 | 701.7 |
| 4 | 12,643 | 11,733 | 12,689 | 603.9 | 585.0 | 909.0 |
| 8 | 15,875 | 15,693 | 16,894 | 1,000.9 | 893.9 | 1,388.0 |
| 16 | 18,056 | 15,841 | 17,873 | 1,850.6 | 1,594.5 | 2,166.9 |

These are embedded storage API timings, not HTTP/Bolt/Cypher end-to-end latency. Fsync-per-write is disabled in both compared configurations, matching `NewBadgerEngine` defaults; this is not a durable-per-write latency claim. The earlier forced-fsync calibration lives in `before-[1-3]-*.json` and is a different test-binary/low-memory workload.

## Longer warm-read comparison at eight clients

Each timed read phase executes 128 passes (557,056 calls), after correctness checks have warmed engine caches. Three independent processes per build. The shorter three-pass matrix is retained as exploratory evidence, not a cold-I/O result.

| Phase | Previous on ops/s | Selected on ops/s | Previous p95, us | Selected p95, us | Selected p99, us |
|---|---:|---:|---:|---:|---:|
| projected_read | 4,198,888 | 3,705,460 | 1.3 | 1.4 | 5.1 |
| full_read | 723,921 | 697,219 | 8.6 | 8.7 | 18.8 |
| adjacency_read | 882,926 | 899,008 | 6.0 | 6.1 | 11.3 |
| mixed_80_read_20_write | 17,217 | 19,081 | 601.2 | 550.4 | 870.4 |

The selected build improves node-write and mixed-workload results, but it is not an across-the-board latency win: the longer projected-read throughput is lower than the prior build, and single-client writes still trail flag-off substantially. Three repeats are not a statistical non-regression bound. Keep the feature experimental and off by default.

Mixed traffic is 80% projected reads and 20% creates in the already initialized namespace; it has 4,352 operations, not 128 read passes. Returned errors fail the process. Retryable uncommitted conflicts have a 32-attempt budget with bounded exponential backoff/jitter; accepted/uncertain commits are never retried. Retry counts are in every phase receipt, and waits are inside reported latency.

## Storage and memory at eight clients

Medians from the three-pass matrix. File sizes include every file after clean close, including graph, indexes and MVCC metadata. Peak RSS includes fixture construction, checks and all phases. Go allocations exclude initial fixture creation but include correctness checks; they are not allocations per API call.

| Metric | Flag off | Previous on | Selected on |
|---|---:|---:|---:|
| Closed-store MiB | 75.85 | 50.46 | 50.41 |
| Peak RSS MiB | 972.07 | 621.11 | 604.32 |
| Post-GC live heap MiB | 254.43 | 277.08 | 240.57 |
| Cumulative allocated MiB | 4957.53 | 3669.67 | 3387.45 |

## Northwind server check

The repository script seeds 97,440 nodes and 312,050 relationships, using batches of 2,000 and four seed clients, then runs 14 queries with 30 iterations/five warmups. Both parser modes use fresh stores, graph-only indexes, `NORNIC_ASYNC_WRITES=0` and identical settings. This is one pair per parser, not a statistical latency bound. Query means include the existing warm result-cache behavior; benchmark fingerprints normalize floating values to 12 significant digits.

| Parser | Seed off/on, s | Mean query off/on, ms | Allocated disk off/on, MiB |
|---|---:|---:|---:|
| nornicdb | 16.444 / 16.802 | 0.3875 / 0.3923 | 300.18 / 241.79 |
| nornicdb-antlr | 16.257 / 16.964 | 0.3814 / 0.4111 | 300.05 / 241.71 |

All 14 row counts and fingerprints match in each pair. Disk reduction is about 19%, but seeding is 2–4% slower and warm query means are higher in this single pair. This is a storage win, not evidence of a Northwind latency win. The profile includes both stronger table compression and transposition; an ordinary-Zstd-only control is still required to attribute savings specifically to the column transform.

## What changed

- Reuse at most 128 KiB of output scratch per property encoder. Accepted payload bytes remain caller-owned; rejected high-entropy attempts avoid an input-sized allocation on every call.
- Limit independent property Zstd history to 128 KiB, instead of retaining multi-MiB histories per pooled encoder.
- Stop the second compression contest when a native integer column already saves at least 75%, preserving exact int64 values and the existing 12.5% acceptance floor.
- Restore SST blocks directly into Badger's destination allocation, removing the extra restored-block buffer/copy. Byte layout, checksums and logical version visibility remain unchanged.

The Snappy table candidate approximately halved the block-encode microbenchmark, but the longer public-API comparison favored the optimized Zstd variant on mixed-workload throughput and storage. Snappy was rejected; its raw results remain in `concurrency`, `sustained/final-*`, `table-fast.txt` and `rejected-fast-policy.txt`. Microbenchmark improvements alone did not decide the implementation.

## Isolated codec CPU and allocations

Three 500 ms repetitions, one CPU; medians. Property results use Nornic's dependency selection (compress v1.19.1). Table results use the fork's own module (compress v1.18.0). These are separate workloads; do not combine their timings into server latency.

| Property encode | Previous ns/op | Selected ns/op | Previous B/op | Selected B/op |
|---|---:|---:|---:|---:|
| text | 16,814 | 7,506 | 27,504 | 336 |
| random-bytes | 4,934 | 1,136 | 10,240 | 0 |
| integer-array | 18,124 | 2,260 | 3,472 | 784 |

The real-SST decode microbenchmark changes from 24,390 to 19,109 ns/op and 57,355 to 28,677 B/op. It checks byte equality and includes the reader's destination allocation.

## Limits and discovered failures

- Concurrent shared-endpoint edge creates exhaust the bounded retry budget in the unchanged flag-off baseline. `concurrency-initial-failure.txt` records a four-client failure after 32 attempts. This is not qualified as working. `-edge-workers 4` reproduces that workload; comparison runs explicitly seed edges with one worker. Concurrent adjacency reads are included.
- Standalone measurements avoid the test fixture's goroutine/lock diagnostics. Earlier test-binary CPU/allocation profiles are retained for diagnosis, separately from the primary public-API results.
- Full suites remain deferred at the maintainer's request. Focused race checks cover fork codecs/tables, Nornic configuration/storage/WAL and the Cypher regression in both parser modes. Check the saved logs for actual completion.
- Unfiltered fork-root `go vet` reports upstream protobuf lock-copy warnings in publisher code/tests. The old baseline produces the same warnings; affected leaf-package vet is checked separately. These warnings are not a passing root-vet result.
- Whole SST blocks are still reconstructed. Independently addressable compressed columns, physical property-range reads and a vectorized CSR executor are not implemented.

## Workload and reproduction

Intel i9-9900KF, Linux Docker laboratory limited to four CPUs and 10 GiB, Go 1.27.3, `noui,nolocalllm`, default Badger engine memory profile. Runs execute serially without other benchmark or test jobs. No new runtime feature flag was introduced.

The corpus is the 272 Markdown files from release commit `ddc59dc20`, archived with Git (LF bytes). Corpus digest: `2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`. Sixteen replicas produce 4,352 nodes, each carrying document text, stable keys, 256 low-range int64 metrics and independent 4 KiB hash-derived bytes. The graph adds 8,704 relationships and 871 mixed-phase nodes. Text repeats across replicas intentionally; arbitrary high-entropy data does not become compressible by assertion.

```sh
mkdir -p /tmp/frozen-nornic-corpus
git archive ddc59dc20 docs | tar -x -C /tmp/frozen-nornic-corpus
GOWORK=off go build -tags noui,nolocalllm -o /tmp/storage-profile ./experiments/property-storage/profile
NORNICDB_EXPERIMENTAL_PROPERTY_STORAGE_ENABLED=true /tmp/storage-profile -docs /tmp/frozen-nornic-corpus/docs -workers 8 -out /tmp/on.json
# Use a separate process and fresh temporary store for every case; repeat with false.
# Add -read-rounds 128 for the sustained read check; -sync-writes for forced fsync.
# Add -cpu /tmp/on.cpu -heap /tmp/on.heap only for separate diagnostic runs.
python experiments/property-storage/verify_profiles.py
```

## Architectural reference

[Ladybug's primary repository](https://github.com/LadybugDB/ladybug) describes columnar storage, CSR adjacency and vectorized/factorized execution. The [Kuzu CIDR paper](https://www.cidrdb.org/cidr2023/papers/p48-jin.pdf) provides the original read-optimized design context. These suggest future contiguous adjacency chunks, bounded vector batches and short publication sections; they are not evidence that this SST transform implements those features or outperforms Ladybug. Its [current transaction manager](https://github.com/LadybugDB/ladybug/blob/main/src/transaction/transaction_manager.cpp) explicitly coordinates writes and publication, so analytical-layout results alone do not establish multi-writer performance.
