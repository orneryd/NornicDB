# NOR-120 / N152-47 worker-stage evidence

This preserves the worker candidate receipts. Parent changes supersede its
performance numbers and final-source claims below; see `parent-acceptance.md`
for the integrated candidate and final checks.

Base HEAD: `163cfa2e0e497b464eda5d9473625ad8c125bf54` (worktree
`/private/tmp/n152-47-topology`, initially clean). Host: Apple M2 Max,
darwin/arm64, go1.27.4. All runs used
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm`, sequentially
(no concurrent benchmark jobs).

## 1. Red: unchanged graph changes its top-1 candidate (production unmodified)

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher \
  -run '^TestNOR120UnchangedGraphTieSelection$' -count=1 -v -timeout=10m
```

exit 1 (115.5 s, first package compile). `/private/tmp/n152-47-red-witness-first.log`

```text
/nornic: unchanged 12-node graph; 64 uncached repeats:
         selected=map[x:52 y:12] scoreBits=map[3ffe69a6019be459:46 3ffe69a6019be45a:18]
/antlr:  selected=map[x:58 y:6]  scoreBits=map[3ffe69a6019be459:51 3ffe69a6019be45a:13]
FAIL: the unchanged graph selected x and y; expected only x (smaller target ID)
```

The five contract owners around it passed. The same six owners in one run:

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher \
  -run '^(TestLinkPrediction|TestNOR120)' -count=1 -v -timeout=15m
```

exit 1 (9.4 s). `/private/tmp/n152-47-red-matrix.log`

```text
PASS TestLinkPredictionEqualScoresKeepOneOrderAndOneTopK (nornic, antlr)
PASS TestLinkPredictionSameBusinessDataAcrossLoads (6 loads x 2 lanes, 30 repeats each)
PASS TestLinkPredictionDistinctScoresRankScoreAndCount (nornic, antlr)
PASS TestLinkPredictionTopKBoundaries (nornic, antlr)
PASS TestLinkPredictionGraphAndDatabaseIdentityAndFilters (nornic, antlr)
FAIL TestNOR120UnchangedGraphTieSelection
     nornic selected=map[x:60 y:4] scoreBits=map[3ffe69a6019be459:49 3ffe69a6019be45a:15]
     antlr  selected=map[x:58 y:6] scoreBits=map[3ffe69a6019be459:43 3ffe69a6019be45a:21]
```

## 2. Red: deterministic owner coverage

Added `pkg/linkpredict/topology_scores_stability_test.go` before the production
edit (`degreeOrderedSum` = terms added from the smallest degree up; exact bit
comparison, 64 calls per layout; public owner case 32 repeats; count-only
allocation assertion).

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/linkpredict \
  -run '^(TestPairScoreAddsCommonNeighbourTermsInDegreeOrder|TestTopologyScoresEqualContributionMultisetsTie|TestPairScoreCountOnlyDoesNotAllocate)$' \
  -count=1 -v -timeout=5m
```

exit 1. `/private/tmp/n152-47-unit-red.log`

```text
FAIL adamic_adar/three-ascending   run 13: got 3ffe69a6019be45a, want 3ffe69a6019be459
FAIL adamic_adar/three-descending  run  9: got 3ffe69a6019be45a, want 3ffe69a6019be459
FAIL adamic_adar/eight-mixed       run  0: got 40101e6205886cb7, want 40101e6205886cb6
FAIL resource_allocation/eight-mixed run 0: got 3ff18e809b5ff4f8, want 3ff18e809b5ff4f9
PASS resource_allocation/three-ascending, three-descending (their 3-term sums happen
     to be order-insensitive at these degrees; the 8-term case exposes it)
FAIL TestTopologyScoresEqualContributionMultisetsTie run 0: x and y got the two
     adjacent representations of the same degree multiset {4,5,6}
PASS TestPairScoreCountOnlyDoesNotAllocate
```

## 3. Green after the `PairScore` correction

```sh
# unit
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/linkpredict \
  -run '^(TestPairScoreAddsCommonNeighbourTermsInDegreeOrder|TestTopologyScoresEqualContributionMultisetsTie|TestPairScoreCountOnlyDoesNotAllocate)$' \
  -count=1 -v -timeout=5m
```
exit 0 (0.47 s). `/private/tmp/n152-47-unit-green.log`

```sh
# public matrix, both parsers
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher \
  -run '^(TestLinkPrediction|TestNOR120)' -count=1 -v -timeout=15m
```
exit 0 (3.48 s). `/private/tmp/n152-47-matrix-green.log`

```text
PASS TestNOR120UnchangedGraphTieSelection
     nornic: selected=map[x:64] scoreBits=map[3ffe69a6019be459:64]
     antlr:  selected=map[x:64] scoreBits=map[3ffe69a6019be459:64]
PASS the other five owners (unchanged)
```

## 4. Focused verification on the final source

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go vet -tags=noui,nolocalllm ./pkg/linkpredict ./pkg/cypher
```
exit 0 (6.8 s). `/private/tmp/n152-47-vet.log`

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/linkpredict -count=1 -timeout=10m
```
PASS (0.72 s). `/private/tmp/n152-47-package-linkpredict.log`

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/inference \
  -run '^TestTopology' -count=1 -timeout=10m
```
PASS (1.00 s). `/private/tmp/n152-47-inference-topology.log`

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher \
  -run '^(TestLinkPrediction|TestNOR120|TestGdsLinkPrediction|TestGDSLinkPrediction|TestNOR357LinkPrediction|TestProfile.*LinkPrediction|TestProfileGDSLinkPrediction|TestGdsIntegrationWithLinkPrediction)' \
  -count=1 -timeout=15m
```
PASS (3.57 s). `/private/tmp/n152-47-consumer-cypher.log`

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher \
  -run '^(TestGDSFunctionsMatchGDS|TestGDSFunctionArgumentErrors|TestGDSLinkPredictionReportsFailedReads|TestCompositeIdentityConsumerMatrix|TestCompositeNativeGDSStreamConsumerMatrix|TestLinkPrediction|TestNOR120|TestGdsLinkPrediction|TestGDSLinkPrediction|TestNOR357LinkPrediction|TestProfile.*LinkPrediction|TestProfileGDSLinkPrediction|TestGdsIntegrationWithLinkPrediction)' \
  -count=1 -timeout=15m
```
PASS (6.86 s). `/private/tmp/n152-47-consumer-cypher-broad.log`

`gofmt -l` over the five changed/added Go files: no output. Linker reports the
existing duplicate `-lobjc` warning.

## 5. Benchmarks: baseline before the edit, repeated after

Workloads (`pkg/linkpredict/topology_scores_bench_test.go`): source `s` whose
common neighbours for candidates `x` and `y` number 3 (the witness), 32 and 256;
hub degrees distinct so the terms differ; the degree function is a map lookup as
in the named-projection public path. Each measurement is `-count=5`,
`-benchtime=200ms`; ranges below are min-max over the five samples (median in
parentheses).

```sh
# before
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/linkpredict \
  -run '^$' -bench '^BenchmarkPairScore$' -benchmem -benchtime=200ms -count=5 \
  -cpuprofile /private/tmp/n152-47-cpu-before-pairscore.prof -timeout=15m
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/linkpredict \
  -run '^$' -bench '^(BenchmarkTopologyScores|BenchmarkAdamicAdarWorkload)$' -benchmem -benchtime=200ms -count=5 \
  -cpuprofile /private/tmp/n152-47-cpu-before-topology.prof -timeout=15m
# after (same settings)
  ... -bench '^BenchmarkPairScore$' ... -cpuprofile /private/tmp/n152-47-cpu-after-pairscore.prof
  ... -bench '^(BenchmarkTopologyScores|BenchmarkAdamicAdarWorkload)$' ... -cpuprofile /private/tmp/n152-47-cpu-after-topology.prof
```

Logs: `/private/tmp/n152-47-bench-before-pairscore.log`,
`...-bench-before-topology.log`, `...-bench-after-pairscore.log`,
`...-bench-after-topology.log`.

### Shared owner `PairScore` (ns/op, B/op, allocs/op)

| workload | before | after | delta | allocs |
|---|---|---|---|---|
| small / adamic_adar | 164.5-165.6 | 173.4-175.8 | +5.6% | 0 -> 0 (slice on stack) |
| small / resource_allocation | 142.4-145.8 | 151.3-156.2 | +6.4% | 0 -> 0 |
| small / common_neighbors | 114.2-115.1 | 115.0-116.1 | +1.0% | 0 -> 0 |
| medium / adamic_adar | 1589-1593 | 1610-1611 | +1.3% | 0 -> 1 (256 B) |
| medium / resource_allocation | 1173-1181 | 1460-1462 | +24.2% | 0 -> 1 (256 B) |
| medium / common_neighbors | 919.0-921.5 | 920.2-921.0 | 0% | 0 -> 0 |
| high / adamic_adar | 12911-12995 | 16637-16717 | +28.7% | 0 -> 1 (2048 B) |
| high / resource_allocation | 8947-8956 | 15482-15616 | +73.2% | 0 -> 1 (2048 B) |
| high / common_neighbors | 6822-6855 | 7104-7120 | +3.9% (see A/B) | 0 -> 0 |

The count-only rows (and `jaccard`, measured with them) keep zero allocations.
The earlier +3.9% for `high/common_neighbors` was re-tested in a controlled A/B
(see below); the same-source difference is about 1%.

### Public topology owner (`TopologyScores`) and public `AdamicAdar`

| workload | before | after | delta | allocs |
|---|---|---|---|---|
| TopologyScores small / adamic_adar | 828.8-836.2 | 843.3-848.1 | +1.5% | 2 -> 2 |
| TopologyScores small / common_neighbors | 727.8-731.2 | 735.5-754.6 | +1.7% | 2 -> 2 |
| TopologyScores medium / adamic_adar | 6527-6552 | 6499-6640 | 0% (noise) | 2 (256 B) -> 4 (768 B) |
| TopologyScores medium / common_neighbors | 5043-5070 | 5221-5243 | +3.3% | 2 -> 2 |
| TopologyScores high / adamic_adar | 51122-51632 | 59267-59404 | +15.6% | 2 (256 B) -> 4 (4352 B) |
| TopologyScores high / common_neighbors | 38893-39000 | 39328-39545 | +1.2% | 2 -> 2 |
| `AdamicAdar` small (top-1) | 1011-1022 | 1023-1026 | +1.0% | 6 -> 6 (504 B) |
| `AdamicAdar` medium | 6364-6395 | 6313-6362 | -0.9% | 6 (504 B) -> 8 (1016 B) |
| `AdamicAdar` high | 48284-48478 | 46811-47070 | -2.8% | 6 (504 B) -> 8 (4600 B) |

The public `AdamicAdar` path passes `nil` degree, so its hubs all have degree 3
and the terms are identical; sorting identical ints is cheap and the numbers are
flat, while the labelled cost of the allocation growth is visible in B/op.

### Controlled count-only A/B (same source pair, 8 samples each)

Baseline file restored with `git checkout HEAD -- pkg/linkpredict/topology_scores.go`,
measured, then reinstated from a `/private/tmp` copy (sha256
`c6c01d9bbf990fea5e1c229068944ac85a665724ee00e6497aabb4474aed8ed2` verified).

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/linkpredict \
  -run '^$' -bench '^BenchmarkPairScore/(small|medium|high)/(common_neighbors|jaccard)$' \
  -benchmem -benchtime=200ms -count=8 -timeout=10m
```
Before: `/private/tmp/n152-47-count-before.log`; after:
`/private/tmp/n152-47-count-after.log`.

| workload | before (8 samples) | after (8 samples) | delta |
|---|---|---|---|
| small / common_neighbors | 113.4-114.6 | 114.4-115.7 | +1.0% |
| medium / common_neighbors | 895.1-903.9 | 896.7-899.3 | 0% |
| high / common_neighbors | 6831-6850 | 6901-6928 | +1.0% |
| small / jaccard | 112.4-113.9 | 114.4-115.2 | +1.4% |
| medium / jaccard | 896.9-900.3 | 896.8-900.0 | 0% |
| high / jaccard | 6837-6885 | 6899-6928 | +1.1% |

`0 B/op, 0 allocs/op` on both sides; the residual ~1% is the per-neighbour
`degreeBased` branch that keeps the accumulating scores from allocating on these
paths.

### CPU profiles

`/private/tmp/n152-47-cpu-before-pairscore.prof`,
`...-cpu-after-pairscore.prof`, `...-cpu-before-topology.prof`,
`...-cpu-after-topology.prof`; `go tool pprof -top` receipts in
`/private/tmp/n152-47-pprof-*.log`.

- Before (PairScore set, 18.29 s samples): map iteration, string map access,
  hashing and `math.log` dominate; `PairScore` flat 1.05 s.
- After (17.55 s samples): same map/iters dominate and the new
  `slices.pdqsortOrdered`/`partitionOrdered`/`insertionSortOrdered` frames appear.
  `go tool pprof -peek slices.Sort` reports **0.95 s cumulative, 5.41%** of the
  whole mixed workload (`/private/tmp/n152-47-pprof-after-sort-peek.log`).
- Public-owner profiles are dominated by harness syscalls (`kevent`,
  `madvise`); `PairScore` cumulative 1.65 s -> 2.24 s in the same wall-time
  window, consistent with the per-call sort/allocation delta above.

## 6. Duplicate candidates and owner inventory

Source inventory (`rg`) at this revision: `PairScore` is the only owner that
accumulates Adamic-Adar / resource-allocation terms. Consumers:
`TopologyScores`/`RankedPredictions`, `AdamicAdar`/`ResourceAllocation`
(`pkg/linkpredict/topology.go`), the hybrid scorer (`hybrid.go`), inference
topology (`pkg/inference/topology_integration.go`, delegating),
`pkg/cypher/linkprediction.go` (GDS stream procedures) and
`pkg/cypher/gds_functions.go` (scalar `gds.alpha.linkprediction.*`). No parallel
scoring implementation was added; the fix extends the existing owner.

Graph/checker receipts (actual failures, not a clean verdict):

- `integration_tools(service="nornicdb")`: `MCP startup failed: timed out
  handshaking with MCP server after 30s`; `nornicdb_docs` 29.999 s and `plane`
  connection closed. No tools were returned, so indexed candidate analysis was
  not performed. The task's supplied retrieval receipt also records
  `/db/nornicdbcode/tx/commit` and `/nornic/search` timeouts with
  `code_baseline: []`.
- The staged duplicate checker (`scripts/code_intelligence.py check-staged`) was
  **not run**: this task must leave its edits unstaged (no `git add`), and the
  checker only inspects staged content. That required check is therefore
  outstanding, not passed.

## 7. Diff scope and artifacts

```text
 M CHANGELOG.md
 M pkg/linkpredict/topology_scores.go            (+32 / -7)
?? pkg/linkpredict/topology_scores_stability_test.go
?? pkg/linkpredict/topology_scores_bench_test.go
?? pkg/cypher/nor120_linkprediction_tie_contract_test.go      (supplied matrix, verbatim
   except one stale comment paragraph, see below)
?? pkg/cypher/nor120_linkprediction_unchanged_graph_test.go   (supplied witness, verbatim)
?? openspec/changes/nor120-score-accumulation-order/
```

The supplied files were copied in unchanged. One comment inside
`nor120_linkprediction_tie_contract_test.go` still said multi-term ties "cannot
be pinned"; after the fix that statement is false, so it now points at the
witness test and the owner. No test body, fixture or assertion was altered.

## 8. Limitations

- One host, darwin/arm64, `GOMAXPROCS=2`, no `-race`, no full suite, no Linux,
  no HTTP/Bolt, no release verification. These are parent-owned.
- The original NOR-120 reload workload and its 8-to-10-of-12 statistic remain
  unavailable and were not replayed; the identity-key contract (target node ID
  across loads) is unchanged and out of scope.
- The witness exposes the defect through executed iteration schedules; it is not
  a proof that every schedule fails before the fix. The unit coverage pins the
  deterministic order by construction after the fix.
- The 256-common-neighbour workloads are deliberately extreme. The measured cost
  is real: about +16% end-to-end on the public owner, +29% (Adamic-Adar) and
  +73% (resource allocation) on the owner microbenchmarks, with one temporary
  2 KB buffer per scored pair. Mitigations considered and not adopted to keep
  the owner minimal and the value inside the formula's previous result set:
  skipping the sort when all degrees are equal (helps only equal-degree
  neighbourhoods), and exact/correctly-rounded summation (order-independent and
  allocation-free, but yields a value no map order may have produced).
- The repository's existing generic `BenchmarkAdamicAdar`/`BenchmarkCommonNeighbors`
  do not exercise this path: `buildLargeGraph` keys nodes with
  `string(rune(i))`, so source `node-500` is absent and both return in ~8 ns with
  no allocation. They were left untouched (out of scope) and are not evidence.

## 9. Final combined run on the final source (after the comment-only edit)

```sh
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/linkpredict ./pkg/cypher \
  -run '^(TestPairScore|TestTopologyScores|TestLinkPrediction|TestNOR120|TestGdsLinkPrediction|TestGDSLinkPrediction|TestNOR357LinkPrediction|TestProfile.*LinkPrediction|TestProfileGDSLinkPrediction|TestGdsIntegrationWithLinkPrediction|TestGDSFunctionsMatchGDS|TestGDSFunctionArgumentErrors|TestCompositeIdentityConsumerMatrix|TestCompositeNativeGDSStreamConsumerMatrix|TestPrediction|TestEnsemble)' \
  -count=1 -timeout=15m
```

exit 0: `ok pkg/linkpredict 0.460s`, `ok pkg/cypher 6.994s`.
`/private/tmp/n152-47-final-combined.log`. `git diff --check` exit 0.
