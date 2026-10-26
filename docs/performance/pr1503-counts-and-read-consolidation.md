# PR #1503: count isolation and native read qualification

Baseline: release/v1.4.4 `fc48d85d1cda694689e67e0d1ccc7b2b02ca1cd7`.
Candidate: the code in this PR, measured on Apple M2 Max / macOS with the same
Go toolchain and default process count. Eight balanced A/B pairs per workload;
rows show medians, not a statistical significance claim. No concurrent local Go
tests, worker inference or ingestion ran; checker network analysis may overlap.

The first integrated experiment exposed +11.1%/+7.5% cached-adjacency time.
CPU profiling attributed the extra work to inactive entity-policy dispatch.
The final correction checks inactive policy before entity dispatch. All original
samples and profiles are preserved; the table below is the corrected source.

| Workload | Base ns/op | Current ns/op | Time change | Base/current B/op | Base/current allocations/op |
|---|---:|---:|---:|---:|---:|
| `BadgerEngine_CreateEdge_WithCounters` | 36,189.50 | 36,167.50 | -0.06% | 23761.5 / 23761.5 | 423 / 423 |
| `BadgerEngine_DeleteByPrefix` | 109,351,520.50 | 109,096,527.50 | -0.23% | 1.49495e+08 / 1.49485e+08 | 886670 / 886668 |
| `BadgerEngine_EdgeCountByPrefix_Warm` | 42.39 | 42.33 | -0.15% | 1 / 1 | 0 / 0 |
| `BadgerEngine_NodeCountByPrefix_Warm` | 42.41 | 42.41 | +0.00% | 1 / 1 | 0 / 0 |
| `SnapshotOverlayReads/pending=false/adjacency` | 932.25 | 847.15 | -9.13% | 512 / 512 | 1 / 1 |
| `SnapshotOverlayReads/pending=false/batch` | 13,172.00 | 13,136.00 | -0.27% | 44504 / 44504 | 260 / 260 |
| `SnapshotOverlayReads/pending=false/counter` | 8,008.00 | 7,994.00 | -0.17% | 1872 / 1872 | 43 / 43 |
| `SnapshotOverlayReads/pending=false/label` | 18,878.50 | 18,859.00 | -0.10% | 44120 / 44120 | 347 / 347 |
| `SnapshotOverlayReads/pending=false/point` | 274.25 | 276.60 | +0.86% | 648.5 / 648 | 4 / 4 |
| `SnapshotOverlayReads/pending=false/projection` | 365.70 | 364.15 | -0.42% | 976 / 976 | 6 / 6 |
| `SnapshotOverlayReads/pending=false/stream` | 98,688.50 | 96,949.00 | -1.76% | 83112 / 83112 | 1238 / 1238 |
| `SnapshotOverlayReads/pending=true/adjacency` | 1,957.00 | 1,899.00 | -2.96% | 984 / 984 | 6 / 6 |
| `SnapshotOverlayReads/pending=true/batch` | 14,315.00 | 14,191.00 | -0.87% | 44504 / 44504 | 260 / 260 |
| `SnapshotOverlayReads/pending=true/counter` | 8,069.00 | 8,002.00 | -0.83% | 1872 / 1872 | 43 / 43 |
| `SnapshotOverlayReads/pending=true/label` | 20,315.50 | 20,250.50 | -0.32% | 44776 / 44776 | 355 / 355 |
| `SnapshotOverlayReads/pending=true/point` | 322.10 | 316.40 | -1.77% | 650 / 644.5 | 4 / 4 |
| `SnapshotOverlayReads/pending=true/projection` | 412.05 | 411.00 | -0.25% | 976 / 976 | 6 / 6 |
| `SnapshotOverlayReads/pending=true/stream` | 99,947.50 | 98,254.50 | -1.69% | 84384 / 84384 | 1250 / 1250 |
| `Transaction_SnapshotAdjacency/edges=100` | 4,597.50 | 4,532.50 | -1.41% | 28665 / 28665 | 81 / 81 |
| `Transaction_SnapshotAdjacency/edges=1000` | 4,846.50 | 4,833.00 | -0.28% | 28665 / 28665 | 81 / 81 |
| `Transaction_SnapshotAdjacency/edges=5000` | 5,026.50 | 4,898.50 | -2.55% | 28665 / 28665 | 81 / 81 |

## Reproduction and limits

The same compiled storage benchmark binaries ran these selectors with
`-test.run=^$ -test.benchmem`:

- `^BenchmarkTransaction_SnapshotAdjacency$`: `-test.benchtime=200000x`.
- `^BenchmarkSnapshotOverlayReads$`: `-test.benchtime=200ms`.
- `^BenchmarkBadgerEngine_(Node|Edge)CountByPrefix_Warm$`: `-test.benchtime=5000000x`.
- `^BenchmarkBadgerEngine_CreateEdge_WithCounters$`: `-test.benchtime=2000x`.
- `^BenchmarkBadgerEngine_DeleteByPrefix$`: `-test.benchtime=3x`.

Prefix deletion uses the native in-memory Badger implementation behind
`MemoryEngine`: 5,000 nodes, 4,999 relationships, warmed caches, setup excluded
from timing. It does not qualify disk fsync latency or the manager's added
catalog intent/completion writes. Those writes enforce the corrected durability
contract; no manager DROP throughput claim is made. Prefix counts are warmed.
Relationship creation and snapshot/overlay measurements have the fixture sizes
encoded by their named repository benchmarks. Byte differences below one
percent retain normal background-allocation variation; allocations are unchanged
apart from sub-unit median rounding on the large deletion workload.

CPU profiles were collected during the affected cached-adjacency reproduction
and during the final 1,000-edge snapshot workload. The matched binaries and all
raw samples are retained under
`/Users/timothysweet/.local/share/nornicdb-pr-readiness/post1406/pr1503/`:
`overlay-regression-*.cpu`, `publish-perf-*.log`, `publish-perf-*.cpu`,
`publish-performance-{identity,receipt,summary}.json`.

These scoped results do not qualify NOR-37's concurrent UNIQUE workload,
other workloads/hardware, a full race sweep, or Neo4j/Docker compatibility.
`make test` is the sole full local verification gate; its result and checked
head/base are recorded in the PR verification comment.

Measured candidate staged tree (before documentation updates): `cd1add73b2574f682fd28e5666c881df7176419f`.
Baseline binary SHA256: `b312bfffa17820d9bae9f32bc1588bf842495e1449a88ed3b78781349857b18c`.
Candidate binary SHA256: `00333a868b7f2a4c5b79c095ad4165f30188e409dc1498754a25cc639c945697`.
