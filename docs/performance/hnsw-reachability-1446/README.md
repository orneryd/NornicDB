# HNSW reachability profiling (#1446)

Measured on October 18, 2026, on an Apple M2 Max, darwin/arm64, Go 1.26.9,
with the local database and its embedding workers left running. Results describe
these workloads on this machine, not GPU or TrueNAS performance.

The reported 44% remove/reinsert regression belongs to a repair implementation
already removed from the PR. The subsequent spanning-tree experiment was also
rejected. These measurements compare develop `16923aef778025fab3490f6a57549763b536f3d5`,
the current PR `c0c97becce1ce1610f8a0b33923b628ddc68fb6b`, and the tuned source.
There is no fixed project percentage threshold: report remaining cost and let
the maintainer decide release inclusion.

## What changed

The allocation profile identified numeric conversions implemented by formatting
and scanning text. The canonical unsigned conversion helpers now use bounds
checks and casts. The uint16 input domain is exhaustively tested, with uint32
boundaries and zero-allocation assertions. Other signed/float conversion
implementations are unchanged.

HNSW's pooled generation-stamp array previously allocated an exactly sized
replacement whenever insertion added another node. Its existing state owner now
uses amortized slice growth, preserving old stamps, clearing newly exposed slots,
and clearing all active slots on generation wrap. A fail-first test measured
1,024 buffer allocations for 1,024 successive sizes before this correction.

These changes do not alter graph topology, lexical hints, vector scores,
persistence, candidate routing, or exhaustion semantics. Population-wide search
still visits historical slots; that separate cost remains tracked in #1471.

## Method

Five rounds rotate the order of three separately compiled test binaries. All
three use identical churn and continuation benchmark harnesses. The churn
fixture has 5,000 vectors of 64 dimensions; remove/reinsert uses exactly 10,000
operations per round. Search runs after 30% removals and reports exact recall@10.
The build matrix uses 2,000 and 5,300 vectors of 2,048 dimensions, including
identical vectors and lexical hints both enabled and disabled.

The continuation fixture drains ranked results from 2,000 two-dimensional
vectors, with and without the result cache. Its threshold case returns 11
documents. IVF-HNSW builds all eight populated cluster graphs (confirmed in the
run logs), with a minimum cluster size of one. Empty continuation mode defaults
to ranked results; there is no catalog tail. `docs/op` must accompany latency:
develop can stop early in the clustered fixture.

Report medians of five runs and preserve individual observations in the adjacent
data files. Short runs, randomized existing fixtures, background work and cache
state introduce variance; small differences do not establish a speedup.

The `heap-B/node` measurement is retained heap after GC, whereas `B/op` and
`allocs/op` measure allocation traffic. These quantities are not interchangeable.

Compile each source with `go test -c -o <binary> ./pkg/search`, then execute from
`pkg/search` with `-test.run=^$ -test.count=1 -test.benchmem` and these selectors:

| Group | `-test.bench` | `-test.benchtime` |
| --- | --- | --- |
| remove | `^BenchmarkHNSWChurn$/^remove_reinsert$` | `10000x` |
| churn | `^BenchmarkHNSWChurn$/(search_after_churn\|search_near_removed_region\|recall_near_removed_region\|load_after_churn)$` | `500ms` |
| normal | `^(BenchmarkHNSWIndex_(Add\|Search)\|BenchmarkHNSWSearch2048)$` | `300ms` |
| build | `^BenchmarkHNSWBuild2048$` | `1x` |
| continuation | `^BenchmarkSearchTextContinuationRanked(HNSW\|IVFHNSW)$` | `250ms` |
| heap | `^BenchmarkHNSWChurn$/^heap_per_node$` | `1x` |
| ivfpq | `^BenchmarkIVFPQCandidateGen(WithOverlay)?$` | `300ms` |

The escaped pipes above are Markdown table escapes, not backslashes to pass in
the regex. The IVF-PQ cases are unchanged controls using the benchmark's
synthetic corpus; they do not measure the full production corpus.

The three source orders are develop/original/tuned, original/tuned/develop,
and tuned/develop/original, repeated for five rounds. No other test or benchmark
process runs concurrently. Builds on this machine require
`CGO_LDFLAGS=-L/Users/timothysweet/src/NornicDB/lib/llama`.

## Verification

Fresh checks on the tuned source:

- `go test ./pkg/util -count=1 -coverprofile=/tmp/nornic-1446-final-util.cover`: passed, 77.8% package coverage;
  both changed conversion functions have 100% statement coverage.
- `go test ./pkg/search -count=1 -coverprofile=/tmp/nornic-1446-final-search.cover`: **failed**, 90.4% package
  coverage. `TestRuntimeStrategyTransition_CPUToGPUBruteNoRebuild` times out
  after 45 seconds with strategy 0 instead of 2. The original PR and tuned test
  binaries each reproduce this failure in isolation. It is not a passing package
  check. The new visited-generation method has 100% statement coverage.
- `go test ./pkg/gpu/... -count=1`: passed with default build tags. CUDA, OpenCL
  and Vulkan checks exercise their available stubs here; this is not hardware
  validation of those backends.
- `go test ./pkg/qdrantgrpc -count=1`: passed.
- `go test -race ./pkg/search ./pkg/util -count=1 -run
  'Test(HNSW(Removal|InsertPruning|EntryPointRemoval|Reachability|Descent|PopulationWide|BuildLinks|SearchReaches|InitialBuild|Visited)|Continuation|SearchTextContinuationRanked|UnsignedIntConversions|SafeIntToUint)'`:
  passed (search 24.407 seconds, util 1.275 seconds). Includes public mutation,
  reload and continuation regression paths, not only helper tests.
- `go vet ./pkg/util ./pkg/search ./pkg/gpu/...`: passed.
- `GOOS=linux GOARCH=386 CGO_ENABLED=0 go test -c -o /tmp/nornic-1446-final-util-386.test ./pkg/util`: passed;
  compilation only, not execution on a 32-bit host.

The conversion allocation test first failed against original source (four and
five objects for uint16/uint32). The visited-growth test first failed with 1,024
allocations. Both pass after the corresponding corrections. No assertion or
readiness rule was weakened to pass these checks.

`make verify-list BASE=origin/develop` and `make verify BASE=origin/develop`
both exit 2 in `verify-selftest`. The verifier creates and commits a temporary
`plan` repository; the mandatory global live-database hook rejects it because
that fixture repository has no ingested database record. The initial build-input
gate self-tests pass, but downstream verifier checks never execute. The hooks
remain enabled and unchanged. This is a verifier/hook integration blocker, not a
full-suite pass. Full PR/release readiness is **not established** by these runs.

Published runtime/report commit: `ce2234bfe3704323520dbb00faadbb996d6a557f`.
Its normal commit hook contacted `http://localhost:7474`, database
`nornicdbcode`, and found no duplicates among 13 added or rewritten functions
(151.22 seconds of ranked-match analysis). The hook explicitly warned that its
corpus was last ingested at `7e90401c6`, older than this source; this is not a
claim of comparison against a freshly ingested PR head. The committed-head
`make verify BASE=origin/develop` rerun again stopped at the same fixture gate.

## Measured behavior and remaining costs

See the [complete latency table](results.md) and the per-round JSON files for
[remove/reinsert](remove.json), [churn](churn.json), [ordinary search](normal.json),
[builds](build.json), [continuation](continuation.json), [heap](heap.json), and
[IVF-PQ controls](ivfpq.json). All 105 benchmark invocations passed.

Remove/reinsert's five-run median falls from 133.709 to 127.878 microseconds
against the original PR (-4.4%); develop is 126.336 microseconds (+1.2% for tuned).
Allocation traffic falls from 17,553 B and 63 objects to 2,348 B and 3 objects
per operation (-86.6% bytes, -95.2% objects). Retained heap is 801 B/node versus
805.6 for the original PR and 802.4 for develop: this is primarily an allocation
traffic correction, not compression of retained vectors.

Churn search remains 94 microseconds versus develop's 87 microseconds, while
recall@10 increases from 0.869 to 0.941. Queries near a removed region have recall
0.084 on develop and 0.831 on both original and tuned PR. Those low-quality
baseline searches do less useful work; their timing is not an equal-quality
performance target.

Uncached full HNSW continuation is 9.156 milliseconds for all 2,000 documents.
Uncached full IVF-HNSW continuation is 4.839 milliseconds for all 2,000; develop
returns only 150 in 0.532 milliseconds. Selective IVF-HNSW returns 11 documents
on all sources but takes 0.259 milliseconds tuned versus 0.049 on develop:
proving exhaustion across the routed population has a remaining cost even when
the selected results happen to agree. Cached results do not measure that work.

The initial allocation profile attributed most sampled objects to uint16 text
conversion. The final CPU profile uses `phase=measured-churn` labels to exclude
fixture construction; it attributes 2.23 CPU seconds cumulatively to the layer
search kernel and 1.38 cumulatively to SIMD dot products (including CGO) in a
6.76-second recording with 5.90 CPU seconds sampled overall. These inclusive
times overlap. The final allocation profile has no sampled conversion/scanner
hotspot. Allocation profiles still include fixture setup; the benchmark's
per-operation allocation counters are the evidence for measured-loop savings.

One build median (5,300 distinct vectors, no lexical hints) was 4.4% slower in
the first matrix. An additional original/tuned/tuned/original,
tuned/original/original/tuned sequence did not reproduce it: original median
10.819 seconds, tuned 10.654 seconds (-1.5%). Individual observations are in
[build-confirmation.json](build-confirmation.json). Do not interpret either
short-run percentage as a reproducible build speedup or slowdown. In the initial
matrix that case's allocation traffic fell from 40,905,008 B / 397,420 objects
to 4,546,896 B / 5,837 objects per build.
