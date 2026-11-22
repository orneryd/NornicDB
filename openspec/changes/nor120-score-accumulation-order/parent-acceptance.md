# N152-47 integrated candidate acceptance

Base: `163cfa2e0e497b464eda5d9473625ad8c125bf54`, release/v1.5.2.
DeepSeek job `15800cc428954f47bf60d886011bb290` reproduced the supplied
unchanged-graph failure before editing and corrected the existing PairScore
owner. The parent reviewed current consumers, integrated additional public
coverage and revised temporary storage; a separate read-only reviewer checked
contracts, fixtures, formulas, callbacks and allocation behavior.

## Final implementation and review

- Sort common-neighbor degrees before accumulating AA/RA; keep formulas and
  exact target-ID ordering. Equal contribution multisets receive identical bits.
- Keep up to 32 degrees on the stack; ordinary slice growth is proportional to
  actual common neighbors. Review rejected preallocating the full smaller set:
  large sets can have a small intersection. No shared scratch state or pool.
- Skip the unused union scan for weighted scores. Count-only arithmetic stays
  in its existing owner; no new scoring or selection implementation.
- Retain the supplied six-owner contract matrix. Extract the repeated twelve-node
  fixture, assert one score-bit representation in the original witness, and add
  scalar, raw AA/RA, predict AA/RA and ensemble top1/top2 bit-stability checks
  through both parsers. Keep numeric checks alongside exact-bit assertions.
- Owner tests cover different hub identities and 3/8/32/33/256 shared-neighbor
  degree sets. Small weighted and count-only calls assert zero allocations.

`PairScore` remains the sole summation owner. Current-source review traced
TopologyScores → public AdamicAdar/ResourceAllocation, hybrid/inference,
Cypher raw/predict streams and scalar GDS functions to it. Existing tests cover
relationship-count versus distinct-neighbor degree semantics, parallel edges,
self-loops, zero degrees, graph identity, filters and cardinality. No epsilon
comparator or business-key identity rule was added.

The staged checker was actually run using the configured code-intel interpreter
and checkout-local hook with `--advisory`. It returned an incomplete analysis:
`/db/nornicdbcode/tx/commit: <urlopen error timed out>`.
Wrapper exit zero is not a clean duplicate verdict. Indexed candidate analysis
remains pending; source inventory is narrower evidence.

## Final benchmark qualification

Apple M2 Max, darwin/arm64, go1.27.4, GOMAXPROCS=2, GOMEMLIMIT=1500MiB,
`-p=1 -tags=noui,nolocalllm -run '^$' -benchmem -benchtime=200ms -count=5`.
Before production edits: worker baseline logs `n152-47-bench-before-pairscore.log`
and `n152-47-bench-before-topology.log`, each with its own CPU profile.
Final source: `n152-47-bench-final-conditional.log` and
`n152-47-cpu-final-conditional.prof`, all three benchmark owners in one run.
No competing Go test/build/benchmark job ran during final measurements.
The earlier worker and unconditional-buffer results are superseded.

Sizes are 3/32/256 shared neighbors, two candidates. PairScore/TopologyScores
use synthetic distinct degrees 3..(size+2), not the exact 4/5/6 witness.
AdamicAdarWorkload uses nil callback and actual Graph.Degree: each hub has three
neighbors, so that separate public workload has homogeneous contributions.

| Workload | Baseline median ns/op (min–max) | Final median ns/op (min–max) | Change | B/op before → after | allocs/op before → after |
| --- | --- | --- | --- | --- | --- |
| BenchmarkPairScore/small/adamic_adar | 164.6 (164.1–165.6) | 127.5 (127.2–127.6) | -22.5% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/small/resource_allocation | 142.4 (142.4–145.8) | 97.54 (97.1–98.35) | -31.5% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/small/common_neighbors | 114.8 (114.2–115.1) | 115 (114.8–115.6) | +0.2% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/small/jaccard | 113.8 (112.9–114.1) | 115 (114.2–115.3) | +1.1% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/medium/adamic_adar | 1590 (1589–1593) | 1037 (1035–1043) | -34.8% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/medium/resource_allocation | 1175 (1173–1181) | 883 (882.8–884.6) | -24.9% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/medium/common_neighbors | 919.6 (919–921.5) | 900.2 (899.7–900.6) | -2.1% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/medium/jaccard | 919.3 (918.6–920) | 901.1 (900.8–903.4) | -2.0% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/high/adamic_adar | 12915 (12911–12995) | 13459 (13433–13518) | +4.2% | 0 → 3584 | 0 → 3 |
| BenchmarkPairScore/high/resource_allocation | 8948 (8947–8956) | 12312 (12277–12318) | +37.6% | 0 → 3584 | 0 → 3 |
| BenchmarkPairScore/high/common_neighbors | 6842 (6822–6855) | 6982 (6976–6994) | +2.0% | 0 → 0 | 0 → 0 |
| BenchmarkPairScore/high/jaccard | 6825 (6817–6881) | 6988 (6976–7015) | +2.4% | 0 → 0 | 0 → 0 |
| BenchmarkTopologyScores/small/adamic_adar | 833.5 (828.8–836.2) | 740.9 (738.1–744.3) | -11.1% | 256 → 256 | 2 → 2 |
| BenchmarkTopologyScores/small/common_neighbors | 728.4 (727.8–731.2) | 743.4 (742.2–744.3) | +2.1% | 256 → 256 | 2 → 2 |
| BenchmarkTopologyScores/medium/adamic_adar | 6529 (6527–6552) | 5418 (5412–5457) | -17.0% | 256 → 256 | 2 → 2 |
| BenchmarkTopologyScores/medium/common_neighbors | 5060 (5043–5070) | 5173 (5167–5180) | +2.2% | 256 → 256 | 2 → 2 |
| BenchmarkTopologyScores/high/adamic_adar | 51204 (51122–51632) | 53005 (52873–55395) | +3.5% | 256 → 7424 | 2 → 8 |
| BenchmarkTopologyScores/high/common_neighbors | 38902 (38893–39000) | 38945 (38789–38955) | +0.1% | 256 → 256 | 2 → 2 |
| BenchmarkAdamicAdarWorkload/small | 1016 (1011–1022) | 925.8 (924.2–929.7) | -8.9% | 504 → 504 | 6 → 6 |
| BenchmarkAdamicAdarWorkload/medium | 6367 (6364–6395) | 5365 (5352–5371) | -15.7% | 504 → 504 | 6 → 6 |
| BenchmarkAdamicAdarWorkload/high | 48360 (48284–48478) | 41537 (41292–41742) | -14.1% | 504 → 7672 | 6 → 12 |

The small/medium weighted workloads improve and remain allocation-free in
PairScore. High-degree resource allocation costs 37.6% more (12.3 microseconds
versus 8.95) and allocates 3584 bytes across three slice growths; the high-degree
AA owner costs 4.2% more, and its two-candidate TopologyScores workload 3.5%.
These are real remaining correctness costs, not a universal speedup or a
noise-equivalent claim. Count-only controls range -2.1% to +2.4% with unchanged
allocations; those small shifts are recorded without asserting significance.
This PR remains a review candidate, not fully qualified for merge.

CPU profiling preceded optimization. Worker profiles identified map traversal,
lookup, math.log and added sorting. Parent profile still shows map iteration
(23.04% flat), map lookup (10.60% flat), PairScore (4.51% flat), sorting partition
(1.15% flat) and insertion (0.79% flat). Removing the unused weighted union scan
and stack storage reduced the first candidate costs without changing results.
No complex accumulator, pooling or parallel scoring path was introduced.

## Final checks

All Go commands use `GOMAXPROCS=2 GOMEMLIMIT=1500MiB` and
`-p=1 -tags=noui,nolocalllm -count=1` unless noted.

- Full `go test ./pkg/linkpredict -coverprofile=...`: PASS, 0.806s,
  package 90.9% statements, PairScore 97.2%, TopologyScores 100%.
- Full `go test -race ./pkg/linkpredict`: PASS, 3.306s.
- `go test -race ./pkg/cypher -run
  '^(TestLinkPrediction|TestNOR120|TestN15247|TestGdsLinkPrediction|TestGDSLinkPrediction|TestNOR357LinkPrediction|TestProfile.*LinkPrediction|TestProfileGDSLinkPrediction|TestGdsIntegrationWithLinkPrediction|TestGDSFunctionsMatchGDS|TestGDSFunctionArgumentErrors|TestCompositeIdentityConsumerMatrix|TestCompositeNativeGDSStreamConsumerMatrix)'
  -timeout=15m`: PASS, 18.772s. Public two-parser witness, scalar/raw/predict/
  ensemble plus existing GDS and composite consumer contracts.
- `go test ./pkg/inference -run '^TestTopology'`: PASS, 1.009s.
- `go vet -tags=noui,nolocalllm ./pkg/linkpredict ./pkg/cypher`: PASS (exit 0).
- `make verify-list BASE=origin/release/v1.5.2`: PASS; all Go/runtime owners
  selected, docs path-filter skipped. Fixture self-tests printed missing ci.yml
  sed diagnostics but passed and the plan completed successfully.
- `make verify BASE=origin/release/v1.5.2 ONLY=localization`: PASS (exit 0).
  Full runtime groups/coverage, UI, llama libs, build, allocation ratchet,
  conformance and differential owners are intentionally unexecuted in this
  review-candidate pass; docs were also not selected (and the full plan path-
  filtered them). This is not full verify.
- `git diff --check`: PASS. Existing duplicate `-lobjc` linker warnings only.

Raw parent logs use `/tmp/n152-47-parent-*.log`; benchmark/profile receipts and
original red failures are archived under
`/Users/timothysweet/.codex/artifacts/n152-47-20261121/` before publication.

## Practical limits

The original historical reload dataset is unavailable; no replay or new
business-key stability guarantee is claimed. This is one Mac host and an
in-process public executor matrix, not Linux, HTTP/Bolt or a full release
qualification. Full make test, all make verify owners, and graph-backed duplicate
analysis remain pending. Large-neighborhood allocation and sorting costs are
explicit review considerations, not silently waived performance gates.
