# Ordered publication performance (#1406)

The tuned candidate preserves commit-order publication per entity and unique
value, publication before acknowledgment, disjoint parallelism, quota-admission
ordering and reentrant listeners. It reduces delete latency and bookkeeping
allocations; update overhead remains and is reported below.

Three measured changes:

- Collect publication dependency keys under one schema read lock without
  copying and sorting validation constraint lists.
- Keep the first pending timestamp inline for each key; allocate a timestamp
  slice only when additional publications share it.
- Remove a posting at the front of an index list without shifting the remaining
  entries. Clear the removed reference and preserve lookup order.

The maintainer clarified that 5% is **not** a project release threshold.
Performance is optimized as part of the fix; the maintainer decides whether
its measured remaining cost is acceptable for 1.4.4.

## Workload and controls

Apple M2 Max, darwin/arm64, 12 logical CPUs, Go 1.26.9; measured October 18,
2026. The local NornicDB server and embedding workers remained running.
Five rounds rotated these sources, with one-second benchmark targets:

| Control | Source |
|---|---|
| Current integration base | `128bd67aa724688d8fd9f56a08515875f697db8b` |
| Original PR | `0e4c36b9027c2511a625f2c98948c5a1f6243cfc` |
| Tuned candidate | Original PR plus this change; source hashes in the data |

[Individual samples and metadata](measurements.json) include all five successful
runs for every workload/source. Tables use medians of the five run-level
metrics, including medians of each run's p50/p99, rather than pooled percentiles.
The earlier recorded develop control, `436fd21d2`, predates quota-admission and
commit-ownership changes already present in the original PR; the current-base
control avoids attributing those intervening changes to this tuning pass.

Each engine has a property index and a uniqueness constraint. The workloads
cover direct and transactional create/update/delete, three reads immediately
after a write, a 10,000-node transaction, eight distinct or overlapping writers,
and small writes alongside repeated 10,000-node transactions.

## Single-writer results

Values are microseconds per operation, except the 10,000-node transaction,
which is milliseconds. Negative changes are faster.

| Workload | Current base | Original PR | Tuned | vs PR | vs base |
|---|---:|---:|---:|---:|---:|
| Direct create | 49.67 | 49.15 | 49.19 | +0.1% | -1.0% |
| Direct update | 24.26 | 25.64 | 25.46 | -0.7% | +5.0% |
| Direct delete | 31.27 | 40.24 | 32.15 | -20.1% | +2.8% |
| Transaction create | 50.09 | 51.16 | 50.79 | -0.7% | +1.4% |
| Transaction update | 30.16 | 32.96 | 32.65 | -0.9% | +8.3% |
| Transaction delete | 41.45 | 46.52 | 41.95 | -9.8% | +1.2% |
| Read after write | 25.28 | 26.69 | 26.32 | -1.4% | +4.1% |
| 10,000-node transaction (ms) | 390.66 | 397.01 | 396.81 | -0.1% | +1.6% |

## Concurrent latency and throughput

| Workload | Source | p50 µs | p99 µs | Writes/s |
|---|---|---:|---:|---:|
| Eight distinct writers | Current base | 78.25 | 231.92 | 89,467 |
| Eight distinct writers | Original PR | 79.75 | 224.96 | 89,007 |
| Eight distinct writers | Tuned | 79.67 | 223.79 | 88,988 |
| Eight overlapping writers | Current base | 75.46 | 205.79 | 94,354 |
| Eight overlapping writers | Original PR | 76.79 | 206.71 | 92,675 |
| Eight overlapping writers | Tuned | 76.46 | 203.50 | 93,244 |
| Small writes beside bulk transactions | Current base | 35.21 | 1140.50 | 8,331 |
| Small writes beside bulk transactions | Original PR | 36.50 | 1143.79 | 8,011 |
| Small writes beside bulk transactions | Tuned | 36.46 | 1146.50 | 8,012 |

Eight-writer throughput remains effectively unchanged versus the original PR.
Against the current base, distinct-writer throughput is 0.5% lower and overlapping
throughput 1.2% lower. Small-write throughput beside bulk transactions is 3.8%
lower; its median p99 is approximately 1.15 ms.

## Allocation and profiling evidence

The original and tuned helper controls are retained in the test benchmarks.

| Isolated operation | Original | Tuned | Allocation change |
|---|---:|---:|---|
| Node dependency collection | 478.0 ns | 259.6 ns | 688 → 224 B; 10 → 6 objects |
| Stage registration/application | 318.4 ns | 317.9 ns | 64 → 48 B; 3 → 1 objects |
| Remove first of 1,000 postings | 194.3 ns | 4.240 ns | Both zero |
| Remove first of 50,000 postings | 13,799 ns | 4.243 ns | Both zero |
| Remove middle of 50,000 postings | 42,052 ns | 42,026 ns | Both zero |

Helper results do not imply the same percentage gain for complete writes.
CPU and allocation profiles of fixed 100,000-operation update/delete workloads
identify constraint-list copies, scheduler registration and posting-list shifts.
The tuned allocation profile shows fewer objects in constraint lookup and stage
registration. Much CPU sampling on this Mac lands in native runtime scheduling;
the profile does not assign every remaining microsecond to a specific Go function.

Three further rotating runs fixed each update/delete/read-after-write workload
at 10,000 operations, avoiding adaptive iteration counts in the allocation comparison.
[All fixed-count samples](fixed-count-allocations.json) are recorded separately.

| Workload | Original B/op | Tuned B/op | Original objects/op | Tuned objects/op |
|---|---:|---:|---:|---:|
| update/direct | 13,105 | 12,616 | 253 | 247 |
| delete/direct | 10,466 | 10,218 | 242 | 238 |
| update/tx | 19,535 | 19,055 | 308 | 302 |
| delete/tx | 16,574 | 16,326 | 319 | 315 |
| read_after_write | 14,595 | 14,124 | 265 | 259 |

These are allocated bytes/objects per operation, not a live-heap or RSS measurement.

The stage still builds dependency identities, registers them before timestamp
publication, selects/help-runs eligible callbacks and retires them before returning.
Returning directly after Badger commits would leave derived state stale and violate
the fix's contract. The measurements do not prove that all remaining overhead is
irreducible: transaction updates remain 8.3% slower than the current integration
base, while direct updates remain 5.0% slower. No Badger patch, unsafe memory handling,
format change or weakening of publication guarantees was introduced.

## Correctness and verification

The current integration base and older develop control both fail the exact
`TestDeleteNodeRemovesPropertyIndexEntries` reproduction for direct and transactional
deletes: the deleted `db:a` remains in the property index. The original PR and tuned
source perform that missing cleanup. Their delete timings therefore include work
that those baseline controls omit; this does not erase the measured difference.

The combined tuned source passed the focused publication suite fresh in 33.708 s:
public node/edge publication matrices, disjoint execution, unique-value ordering,
quota-admission callback ordering, buffered acknowledgment, listener reentrancy and
the complete stage boundary. Additional tests compare node/relationship dependency
sets with the original sorted-schema implementation, exercise out-of-order registration
with duplicate keys, and verify first/middle/last/missing removals, reinsertion and
independence of returned lookup snapshots for single and composite indexes.

Fresh verification on the combined tuned source:

- `go test ./pkg/storage -count=1 -timeout=20m -coverprofile=...`: passed in
  150.859 s; overall storage statement coverage 87.7%. The stage's registration,
  dependency selection, application and retirement functions and posting removal
  have 100% coverage; node/edge dependency collectors have 94.7% each.
- `go test -race ./pkg/storage -run 'Test(PostCommit.*Publication|DisjointPublications|UniqueValueKey|PublicationStage|PublicationKeys|PublicationIndex|AfterPublication|BadgerTransaction_PublicationBeforeReentrantListener|DeleteNodeRemovesPropertyIndexEntries|CompositeIndex)' -count=1 -timeout=15m`:
  passed in 59.792 s. This is the focused race suite, not all storage tests under race.
- `make verify-list BASE=origin/integrate/1366-on-develop` and
  `make verify BASE=origin/integrate/1366-on-develop`: both failed in
  `verify-selftest`. Its fresh Git repository `plan` has no ingested graph, so the
  mandatory live duplicate hook rejects the fixture commit. The hook remained
  enabled, used the configured live endpoint and was not bypassed.
- Downstream make-verifier checks, including its full build/test, allocation,
  TCK, differential, UI and documentation checks, did not run. The independent
  storage/race/benchmark results above do not substitute for those checks.

The installed live pre-commit hook passed for the source commit: 17 added or
rewritten functions, no duplicates found, repository `orneryd/NornicDB`, database
`nornicdbcode`, RRF floor 0.01. Ranked analysis took 241.18 s. Its indexed corpus
was ingested at `7e90401c6`, older than the PR base; the hook reported that
staleness limitation. Graphify also warned about an unchanged NEON header;
the affected Go functions were extracted and analyzed.

The verifier fixture issue remains open; this report does not claim a full
verification pass or merge readiness.

A preview merge of the current integration base is clean and changes no storage
source relative to the original PR, but a preview is not an executed integration test.

## Reproduction and limits

```sh
go test ./pkg/storage -run '^$' \
  -bench 'BenchmarkPublication(WritePath|ConcurrentWriters|LargeCommitThenSmallWrites)$' \
  -benchtime=1s -count=1 -benchmem

go test ./pkg/storage -run '^$' \
  -bench 'BenchmarkPublication(NodeKeys|Stage|PostingRemoval)$' -benchmem
```

Run the first command in rotating source order for five rounds. Linked worktrees
on this machine also need `CGO_LDFLAGS=-L/path/to/NornicDB/lib/llama` to locate the
existing local static library. CPU profiles label measured write phases separately
from population/cleanup; use `-cpuprofile` and `-memprofile` with a fixed operation
count for diagnostics, and unprofiled runs for latency comparison.

Earlier paired runs canceled for hook work or superseded to add the current-base
control are incomplete; they are not counted in this report's final comparison.
An experiment combining publication/scheduler records measured 322.1 versus 323.8 ns
with identical 64 B/3-object allocation counts and was reverted for lack of gain.
