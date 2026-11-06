# PR #1688 matched cost qualification

Baseline b098f7e708b4c701a743ffb0310a9b179a84c8f2; Apple M2 Max; GOMAXPROCS=4, GOGC=100; matched noui,nolocalllm binaries. Sequential ABBA, six samples per public cell and four per native cell. Setup and snapshot priming are untimed. Full row multiset is asserted on every public sample. Values below are medians; raw min/max and counts are in qualification-summary.json. No concurrent tests, builds or other task profiling.

| Parser / query / mode | Baseline ms | Candidate ms | Baseline / candidate MB | Baseline / candidate allocations |
| --- | ---: | ---: | ---: | ---: |
| nornic / original / explicit | 958.54 | 275.55 | 606.374 / 243.218 | 8,371,689 / 1,896,074 |
| nornic / original / autocommit | 249.54 | 249.69 | 185.423 / 185.423 | 1,527,236 / 1,527,233 |
| nornic / bound / explicit | 869.56 | 240.69 | 588.146 / 236.027 | 8,318,332 / 2,056,619 |
| nornic / bound / autocommit | 204.11 | 205.36 | 177.108 / 177.105 | 1,680,093 / 1,680,070 |
| antlr / original / explicit | 955.95 | 277.85 | 606.398 / 243.220 | 8,372,094 / 1,896,087 |
| antlr / original / autocommit | 251.80 | 252.93 | 185.424 / 185.423 | 1,527,244 / 1,527,239 |
| antlr / bound / explicit | 875.13 | 240.49 | 588.175 / 236.028 | 8,318,874 / 2,056,622 |
| antlr / bound / autocommit | 209.58 | 209.20 | 177.107 / 177.106 | 1,680,078 / 1,680,076 |

| Native path | Baseline ns/op | Candidate ns/op | Baseline / candidate B/op | Baseline / candidate allocs/op |
| --- | ---: | ---: | ---: | ---: |
| BenchmarkNOR160EdgeReads/list | 667,400.5 | 664,947.0 | 1,183,424 / 1,183,424 | 11,779 / 11,779 |
| BenchmarkNOR160EdgeReads/stream | 669,824.0 | 666,661.0 | 1,135,175 / 1,135,176 | 11,766 / 11,766 |
| BenchmarkNOR160EdgeReads/between-current | 9,470.0 | 9,618.0 | 11,504 / 11,504 | 195 / 195 |
| BenchmarkNOR160EdgeReads/between-snapshot | 1,852,398.5 | 16,714.5 | 1,965,424 / 15,136 | 29,771 / 271 |
| BenchmarkNativeTransactionPointRead/node | 1,596.5 | 225.3 | 1,720 / 496 | 32 / 4 |
| BenchmarkNativeTransactionPointRead/query-node | 1,627.5 | 226.2 | 1,720 / 496 | 32 / 4 |
| BenchmarkNativeTransactionPointRead/projected-node | 1,877.0 | 194.7 | 1,920 / 496 | 41 / 4 |
| BenchmarkNativeTransactionPointRead/edge | 1,195.0 | 1,202.5 | 1,120 / 1,120 | 25 / 25 |
| BenchmarkNativeTransactionPointRead/cold-node | 2,995.5 | 3,035.0 | 4,920 / 4,920 | 62 / 62 |

Historical de70 runs are separate context on another lineage; they do not isolate this PR. The generated fixture matches the original cardinalities, not a private saved database. Timed profiles run separately from cost samples. Normal make test skips the optional full performance profile; correctness coverage runs independently.


## Workload and interpretation

The generated original-PR fixture contains 70,802 nodes, 96,160 relationships,
5,101 input pairs and 1,390 expected complete result rows. Parameter SHA-256 is
`b1755d394fe831e85f4f7cdd16786f683f878def387bddc919f95202d2c2524a`.
Each original/bound and explicit/autocommit cell compares its complete sorted
record-value multiset, not just counts. An unused changing parameter invalidates
the result-cache key, so these are execution measurements. Both modes use durable
writes for untimed setup (`SYNC=true`, write-behind off).

Repeated small node images use the existing 128-node/256-KiB snapshot cache;
partial projections, sidecars, vectors, large images and write-behind overlays
retain their selection rules. Namespace edge projection stops cloning an already
caller-owned property map. Current one-off point reads and edge list/stream
paths retain the same allocation counts; sub-percent byte differences in the
large samples reflect runtime sampling, not an added per-record allocation.
The historical between read now selects MVCC adjacency instead of decoding the
whole graph. Its ten edge identities, directions and property values are
asserted in every native benchmark iteration.

CPU profiles were captured separately for original/bound explicit/autocommit
queries on each parser. An additional native point-edge profile exposed scoring
clock work even with visibility inactive. The final owner checks the existing
visibility policy before reading that clock; live suppression and decay still
filter pending and cached results through the same completion owner. All native
latency medians are faster or within 1.6% of baseline, with no extra allocations.
This workload does not qualify every schema, degree distribution, sidecar/vector
size, write-behind configuration or private production database.

## Historical parent context (not an isolated change comparison)

The original PR parent `de70c0e8375e9c74800d5ab6d7da5a6505650cb8` is on a
different release lineage from b098. Its preserved original harness ran against
the same generated shape and full-row oracle. Three samples per cell establish
context, not the primary A/B evidence or a claim about one historical merge.

| Parser / query / mode | Historical median ms | Historical MB | Historical allocations |
| --- | ---: | ---: | ---: |
| nornic / original / explicit | 870.04 | 620.328 | 8,634,363 |
| nornic / original / autocommit | 188.42 | 203.224 | 1,816,776 |
| nornic / bound / explicit | 822.56 | 602.664 | 8,560,341 |
| nornic / bound / autocommit | 153.36 | 195.246 | 1,947,804 |
| antlr / original / explicit | 912.24 | 620.331 | 8,634,381 |
| antlr / original / autocommit | 195.04 | 203.225 | 1,816,776 |
| antlr / bound / explicit | 834.99 | 602.659 | 8,560,343 |
| antlr / bound / autocommit | 152.58 | 195.247 | 1,947,807 |

## Reproduction

Run the opt-in full generated fixture once per parser, before `make test`:

```sh
GOMAXPROCS=4 GOGC=100 NORNICDB_PARSER=nornic NOR160_FULL=1 NOR160_ROUNDS=3 NOR160_PROFILE=0 go test -tags noui,nolocalllm ./pkg/bolt -run '^TestNOR160RelationshipReadProfile$' -count=1 -v
GOMAXPROCS=4 GOGC=100 NORNICDB_PARSER=antlr NOR160_FULL=1 NOR160_ROUNDS=3 NOR160_PROFILE=0 go test -tags noui,nolocalllm ./pkg/bolt -run '^TestNOR160RelationshipReadProfile$' -count=1 -v
GOMAXPROCS=4 GOGC=100 go test -tags noui,nolocalllm ./pkg/storage -run '^$' -bench '^(BenchmarkNativeTransactionPointRead|BenchmarkNOR160EdgeReads)$' -benchmem -benchtime=500ms -count=2
```

For the primary baseline, apply only this PR's `pkg/bolt` test diff and the two
storage benchmark files to an isolated b098 checkout. They use the same public
APIs on both revisions; production sources stay unchanged. Build both binaries
before measurements, alternate baseline/candidate/candidate/baseline, and run no
other tests, compilation, containers or local inference during that window.
For profiles, use `NOR160_ROUNDS=1 NOR160_PROFILE=1` in a separate run. Raw logs,
binary hashes, source fingerprints, min/max values and the 161.1-second receipt
with no changed inputs are retained in the task evidence. Default native focused
race tests passed independently; full `make test` is the final local gate.

## Review-candidate qualification status

The tables above qualify the earlier b098-based transaction-read checkpoint.
Later corrections restore shallow nested-copy costs from 1,400 B/9 allocations
to 872 B/7 allocations while scalar copies stay at 496 B/3 allocations. These
counts repeat across samples; their diagnostic timings were contended and do
not replace the isolated latency qualification above. A new exact-head paired
qualification after rebasing onto merged NOR-136 is pending. No new-head latency
or full-suite pass is inferred from the earlier checkpoint.

## Mixed write-behind retry correction

Parallel CI uses the workflow from `main` and checks out the requested immutable
PR head for tests. Its auxiliary `.ci-workflow-tools` checkout is a different
revision and is excluded from the scanner-family source audit, like `.verify`.
An injected duplicate declaration in the actual PR source still fails that audit.

On `b8a83d72`, the public NOR-136 six-mode increment workload failed 26 of 30
repetitions with `GOMAXPROCS=2`: mixed explicit/autocommit workers exhausted the
unchanged 2,000-attempt budget. Diagnostics found a native Badger conflict after
accepted preparation with an initially empty buffer (begin epoch 134, current
replay epoch 140). A post-journal discard retains recovery authority, so later
writers cannot make progress merely by retrying.

The retained correction settles direct-commit ordering, rebase and endpoint
validation before the WAL acceptance handoff. Snapshot and mutation-preimage
checks remain in place. An additional experiment that broadened the replay
detector passed the increment workload but failed relationship counters: seeds
68 and 73 reduce to a stale staged cascade, a peer relationship deletion, and
an unrelated buffered write. That experiment is withdrawn; the existing rebase
criteria remain unchanged. The two minimized programs now live in the canonical
write-order schedules and fail with the withdrawn experiment.

With only the ordering correction retained, the two original seeds passed five
repetitions (2.591 seconds). The complete generated model and named schedules
passed in both write-behind modes (72.695 seconds). The final ordering-only correction passed 30 repetitions of the six-mode
public progress workload (144.715 seconds) and public/conflict/counter race
controls (69.024 seconds). Complete exact-head CI remains required; the earlier
detector-only and combined-candidate passes do not qualify this combination.

CI also exposed a missing relational proof in the stored-write census: the same
immutable boolean both gates acquisition and returns buffered commits before the
direct-write tail. The proof now uses that exact SSA value and a dominating
branch, reusing the context census's stability owner. Changed/reloaded values,
loop definitions, missing/failed acquisitions and early release remain rejected.
The actual module's ordered-write audit passes (23.039 seconds), and regenerated
artifacts retain all 369 field-write records and eight decision records. Two
existing accepted-completion context occurrences moved within PublishCommit;
only their recorded offsets changed. The full context census passes (98.891
seconds), as do stale-scope rejection controls (2.548 seconds). No duplicate
checker rules, exemptions, thresholds or hooks changed.

The complete stored-write census also passes (184.397 seconds), including its
existing bypass and fail-closed controls. The separate full mutant package
remains part of the required parallel CI gate; no local full-suite pass is
claimed.

## Auxiliary verification checkout isolation

The numeric-wire audit exposed the same extra-checkout population as the scanner
audit. Both now use `internal/testfixture.WalkRepositoryTree`, which recognizes
nested Git directories and worktree marker files. Existing nested modules and
experiments remain audited; numeric path and exception inventories are unchanged.
The earlier scanner-only directory-name exclusion is removed.

Both audits pass with the foreign-checkout fixture and reject the same fixture
at the same path when its Git marker is removed. The traversal tests also retain
ordinary source under the CI-helper directory name, nested-module source,
caller directory policy, and traversal/visitor errors. Temporary fixture files
are removed. Fresh clean-tree checks pass: fixture traversal 0.606 seconds, numeric audit
0.642 seconds, scanner audit 1.297 seconds, and census freshness/ordering
11.317 seconds. Complete exact-head CI remains required.

## Cumulative mutant-package budget

Complete CI run 42137139063 at `1f84a988` passed command, Cypher, storage,
server, both censuses and lint. Its remaining package,
`testing/storagecensusmutants`, reached the original 30-minute cumulative
budget. The final `unresolved_write_callback` case had run for 44 seconds;
the log reports no assertion failure before the timeout. The same complete
matrix passed the preceding run in 1496.164 seconds. That pass does not waive
the later timeout or establish an individual case defect.

The matrix now lives once in `testing/storagecensus/internal/mutanttest`.
Four test entry packages under `testing/storagecensus/mutants` select original
indices modulo four, giving 10/9/9/9 cases. Each partition checks the clean
baseline and runs its cases sequentially with their original assertions.
The complete case literal is byte-identical, and the formatted assertion
callback has the same source hash. A partition control checks distinct names,
complete assignment and nonempty partitions. The existing CI helper routes
these packages to the storage-census shard without workflow changes.

Analyzer rules, fixture edits, expected violations, short-mode behavior and
the per-package timeout are unchanged. Integrated assignment controls pass
(0.322 seconds); all four entry packages compile. Fresh integrated census
artifact/ordering checks pass (12.908 seconds). These are not a full matrix
or full-suite pass; complete partition execution and replacement CI remain
required.

## Embedding-free mutation preimage validation

Write-behind full-property validation left its projection nil, causing the
projected reader to select full embedding hydration. The comparison uses only
properties and labels. Its physical read now uses the existing
`nodeItemBodyOnly` mode; constraint-only validation retains the existing
borrowed projected read. Buffer selection, record versions, conflict equality,
error classification, public full reads and storage formats are unchanged.

The clean pushed baseline fails a public commit after a test-owned embedding
chunk is made undecodable. The regression also proves ordinary full reads
reject the malformed chunk. It establishes warm/cold body-cache states after
the fault write, verifies the predecessor is physical and the result is an
acknowledged buffered update, restores the chunk, then checks every vector
value through full reads before and after flushing and clearing caches.
Stale property/label images and changed constraint keys remain conflicts.
The constraint control changes a key outside the embedding text source, so
it retains the vector-preservation oracle rather than testing invalidation.

Fixture construction reuses the existing write-behind/manual-drain owner,
embedding writeback builder, raw-record read/write helpers and large-vector
fixture. The constant-vector builder is shared with the existing chunk-update
test and new benchmark; no forwarding helper or production test hook is added.
Tests are named for their behavior, not a PR identifier.

### Scoped benchmark

Apple M2 Max, darwin/arm64, Go 1.27.3; three runs of 1,000 public write-behind
updates per payload, with CPU and allocation profiles for both variants.
Background replay is stopped and each iteration explicitly flushes, making
the next predecessor physical. Time includes staging, validation, publication
and flushing. The baseline uses the unchanged `3cdd2049` validation function;
both variants use the same benchmark and fixtures. Medians:

| Sidecar payload | ns/op before → after | B/op before → after | allocations/op before → after |
| --- | ---: | ---: | ---: |
| 1 × 8 float32 values | 41,265 → 39,756 | 27,628 → 26,548 | 450 → 428 |
| 2 × 16,000 float32 values | 2,119,438 → 1,287,829 | 3,564,119 → 2,548,002 | 842 → 682 |

The large workload reduces time by 39.2%, bytes by 28.5% and allocation count
by 19.0% in this bounded comparison. The focused allocation profile attributes
vector reads/decoding to validation before the change; those vector-loading
frames disappear from the validation profile afterward. Remaining staging
and persistence costs are included, so these results do not imply vector-free
work across the whole update or a release-wide throughput qualification.

Final focused race checks cover the new regressions, existing chunk persistence
and cancellation controls (three repetitions, 8.064 seconds). Fresh blocking
stored/context census checks pass (19.956/82.463 seconds), as does go vet. A
delegated full storage-package run passed before final test-only integration
refinements (499.887 seconds); its redundant repeat was canceled, not passed.
Complete make-test CI on the resulting pushed head remains required.
