# Scalar default and IVF-HNSW follow-up — October 17, 2026

Single-graph application HNSW now defaults to scalar int8 traversal with exact
original-vector rescoring. `NORNICDB_VECTOR_HNSW_QUANTIZATION=none` explicitly
retains float traversal; explicit collection compression policies take precedence.
The low-level index configuration remains explicit. The [original full-mode
report](../README.md) supplies the scalar selection evidence: 99.59% recall@10,
0.551 ms p50, 0.669 ms p95, 7,998 queries/s. Those scalar measurements use
file-backed originals; scalar encoding alone does not establish lower process
RSS when originals remain in RAM. No new scalar benchmark is claimed here.

IVF-HNSW now builds a graph for every nonempty routed cluster. Its previous
minimum-size and maximum-count build filters are retired (accepted legacy config
keys are ignored). Missing required graphs fail the query; there is no exact
population-scan fallback. Selected graph searches use at most four workers,
bounded by GOMAXPROCS, retain the full per-cluster candidate budget and merge
in route order. Exact scoring of the final candidate pool remains supported.
Lexical cluster selection is preserved. Float benchmark controls explicitly
select `none` so the application default cannot change their representation.

## Full-corpus measurement

Apple M2 Max / 32 GiB / native ARM64 CGO; all 88,155 cached 1024-D vectors,
all 1,000 queries, exact truth, K=10, 100 warmups and eight-client throughput.
Engines ran one at a time, after tests and builds stopped. No AWS/Titan requests
and no competitor execution. Both cases use float traversal, the same corpus,
and the same benchmark settings. Each case constructs independent clusters.

| NornicDB IVF-HNSW | Recall@10 | p50 ms | p95 ms | Queries/s | RSS MB | Cluster graphs |
|---|---:|---:|---:|---:|---:|---:|
| Fresh serial control, with fallback | 0.9734 | 7.159 | 20.637 | 557 | 3,850 | 193 / 209 |
| Concurrent, every cluster indexed, no fallback | 0.9737 | 2.363 | 2.691 | 738 | 3,767 | 209 / 209 |

Against the fresh control: 3.03× lower p50, 7.67× lower p95 and 32.5% higher
throughput. Recall remains approximately 97.4%; the tiny difference between
independent cluster constructions is not an established quality improvement.
These results combine concurrency and fallback removal, not an isolated causal
measurement of either change. RSS snapshots do not establish a repeatable memory
gain. Coarse routing still bounds recall.

The original full-mode report's serial run measured 18.177 ms p50 and 21.607 ms
p95, with 190 / 209 graphs. Its result remains preserved. Fresh control p50
varied substantially, so the 3.03× claim uses the new control, not that older p50.

- [Fresh serial raw result](serial/nornicdb-ivf-hnsw+serial-control.json)
- [New raw result](current/nornicdb-ivf-hnsw+no-fallback.json)
- [Fresh serial report](serial/nornicdb.md), [new report](current/nornicdb.md)
- [Repeatable artifact audit](audit.py), [audit result](audit.json)

Control binary: clean `7c201376d1203a651fc625869fb98df7be746ac4`.
Current binary and frozen verification source: clean
`c5ef90e3825d2871f7df772a293b09237c00c2a8`. Both were built with
`go build -buildvcs=true -tags 'noui nolocalllm'`.
The harness records revision, binary/corpus hashes and settings in each result.
Checked release base remains `f2ddd27cfae3788076778a0a0d48c827df046a3b`.

Commands (cached virtualenv Python; paths relative to repository root):

```sh
python testing/benchmarks/vector_ann/run.py \
  --corpus bench-data/vector_ann/corpus \
  --out bench-data/vector_ann/profiling/ivf-no-fallback/serial \
  --data-root bench-data/vector_ann/profiling/ivf-no-fallback/data \
  --nornic-bin bench-data/vector_ann/tools/nornicdb-quantization-final \
  --qdrant-bin bench-data/vector_ann/tools/nornic-qdrant-1.19.2 \
  --engines nornicdb --modes ivf-hnsw --tag serial-control
# Repeat with --out .../current --tag no-fallback and
# --nornic-bin bench-data/vector_ann/tools/nornicdb-ivf-no-fallback-final
```

The competitor binary argument is a harness requirement and provenance input;
`--engines nornicdb` prevents its execution. Selected unmodified server records
are archived alongside each result; full logs remain in local benchmark data.

## Verification, failures and limits

- `GOFLAGS=-p=1 make test`: **passed, 94 packages (26 fresh, 68 cached)** on
  the frozen final source. Fresh search 173.428s, Qdrant 2.665s, server 160.271s,
  storage 71.599s. Package serialization changes scheduling, not assertions.
- `go test -race ./pkg/search -run
  'TestIVFHNSW|TestHNSWDefaultScalar|TestQuantizedHNSWPublic' -count=1
  -coverprofile=/tmp/nornic-ivf-final-focused.cover`: passed **47.711s**.
  IVF-HNSW `SearchCandidates` statement coverage **94.6%**. The focused run's
  whole-search-package coverage was 26.3%; it is not a full-package coverage run.
- Public regressions cover unset/default scalar and explicit `none`, quality
  presets, explicit collection overrides, exact rescoring and lexical hints;
  small lexical-selected graphs despite retired size/count filters; full
  candidate budgets, independent merge equivalence, missing graphs, graph errors
  and cancellation. Their fail-first observations are archived.
- Python harness: **17 tests passed**, including the new float-control assertion;
  its prior missing-explicit-mode failure is retained.
- Initial scalar-default fresh package checks passed (search 211.538s, Qdrant
  3.553s). Earlier default-parallel `make test` attempts failed storage timing
  checks: first `TestScopedLabelReadCostIgnoresOtherDatabases`, then
  `TestTemporalCurrentPointerRefresh_ScalesLinearly` and
  `TestWriteBehind_NodeKeyBulkLoadScalesLinearly`. The scoped-label check passed
  ten isolated repetitions unchanged. No storage source, dependencies, assertions
  or thresholds were modified. Full failure logs are preserved.
- An intermediate serialized run was canceled when final source changed; it is
  superseded and incomplete. The completed serialized run above checks the frozen
  final source. All transcripts are in [checks/](checks/).
- The final eight-cluster microbenchmark (64-D / 4,096 points / K=20,
  five 2-second trials) measured median 142,508 ns/op, 31,349 B/op and
  60 allocations/op. The earlier serial microbenchmark is retained but excluded
  from timing conclusions: its final trials may have overlapped subsequent race
  compilation, so complete isolation cannot be established. The earlier 2.4×
  microbenchmark comparison is withdrawn; the full-corpus 3.03× comparison above
  uses freshly isolated complete runs. No extra full-corpus work overlapped them.
- `make verify` remains skipped per maintainer instruction. No CI was dispatched.
  CUDA/Vulkan/native AMD64 execution and BEIR SciFact were not rerun; previous
  [hardware and corpus limitations](../verification.md) remain applicable.
