# PR #1548: durable-cache qualification

Release baseline: `343ca141f4e2516883e5f83cac12efe022a4a3b1`.

## Product proof

Returned/admitted mutable values are owned by their caller or retained owner;
cache revocation follows durable success and affects the declared scope. Rejected
schema changes preserve accepted live definitions, index state and binding tables.
Direct schema persistence and native physical/live publication share one gate so
an acknowledged intervening policy survives reopen. Context cancellation reaches
admission while accepted commands retain their own completion lifetime.

Public storage, Cypher and protocol tests, compiled population/ordering checks,
negative controls and focused races cover those contracts. Population discovery
alone is not semantic proof. No new Badger key, stored payload, WAL or storage
version is introduced.

## Paired local costs

Apple M2 Max (12 cores, 32 GiB), Go 1.26.9, GOMAXPROCS=1. Six samples per
revision/workload in release/current/current/release order; medians below.
Property/read cases use 200 ms, buffer cases 1,024 iterations and commit cases
500 ms. Both binaries execute identical benchmark inputs and result assertions.
The current binary is bound to runtime/test input SHA256
`1fe382f8a350b8032f8b73bd178fb60de2f52b25135812fe8a655de7e775907d` and binary SHA256
`cc44038e7f49977bd28feb4daa039666574cc7c0191264f4926da2befd1e10a9`.

No Qwen inference was submitted; its server was resident and idle. Docker's
socket was unavailable, so container state could not be independently listed.
Ordinary OS work was present; this is paired local evidence, not complete machine
isolation or a Northwind/whole-system performance claim.

| Workload | Release ns/op | Current ns/op | Change | Release B / allocs | Current B / allocs |
| --- | ---: | ---: | ---: | ---: | ---: |
| `EdgePropertyOwnership/integer` | 237.95 | 249.30 | +4.77% | 496 / 3 | 496 / 3 |
| `EdgePropertyOwnership/float` | 240.45 | 248.80 | +3.47% | 496 / 3 | 496 / 3 |
| `EdgePropertyOwnership/nested-list` | 237.85 | 493.65 | +107.55% | 496 / 3 | 872 / 7 |
| `ResidualCosts/relationship-body` | 1,080.50 | 1,091.50 | +1.02% | 1188 / 18 | 1188 / 18 |
| `ResidualCosts/relationship-type` | 12.82 | 13.13 | +2.46% | 0 / 0 | 0 / 0 |
| `ResidualCosts/numeric-list` | 17,700.50 | 18,032.50 | +1.88% | 8784 / 13 | 8784 / 13 |
| `ResidualCosts/schema-definition` | 713.25 | 941.25 | +31.97% | 1552 / 16 | 2096 / 17 |
| `ResidualCosts/transaction-metadata` | 1,709.00 | 2,072.50 | +21.27% | 416 / 36 | 1032 / 40 |
| `ResidualCosts/buffer-append-node/32` | 92.59 | 411.90 | +344.86% | 233 / 1 | 1345 / 9 |
| `ResidualCosts/buffer-append-edge/32` | 117.90 | 408.15 | +246.18% | 308 / 1 | 1420 / 8 |
| `ResidualCosts/buffer-append-node/1024` | 84.15 | 2,759.00 | +3178.67% | 233 / 1 | 9281 / 9 |
| `ResidualCosts/buffer-append-edge/1024` | 116.10 | 2,780.50 | +2294.92% | 308 / 1 | 9356 / 8 |
| `ResidualCosts/commit` | 57,494.50 | 60,504.50 | +5.24% | 30593.5 / 333 | 33144.5 / 361.5 |

The baseline nested property, schema and metadata owners retain caller containers;
copying them is the correction, so their baseline is not a correct zero-copy
alternative. Public buffer admission now owns the submitted image (including its
payload), whereas the old API retained it; the 32/1,024 sizes expose that cost.
Internal accepted-ACK replay does not route through this public append API.
Schema definition copies include the owned property slice and the manager's
publication-gate footprint. Commit cost includes the corrected scope/value and
publication work. These costs are disclosed rather than called performance neutral.

Read-body/type and scalar/list allocation counts remain unchanged. The earlier
quiet run and focused numeric-list ABBA gave +1.99% (same 8,784 B/13 allocations),
while one short initial series gave +5.79%; both are retained in the evidence.
A correctly owned convergence checkpoint versus final commit comparison gave
+4.86% median time with wide overlapping ranges (53,698–65,440 versus
53,439–71,272 ns/op), not a statistical-neutrality proof. Earlier contended
frozen measurements shifted both binaries' absolute times and are not substituted
for the local series above.

## Avoidable convergence cost corrected

Rich policy SHOW families share pointer sorting and their canonical deep cloners;
small scalar promotion profiles use direct value sorting. At 1,024 entries the
all-pointer scalar variant required 75,008 B/two allocations versus 65,536 B/one
for the integrated scalar path (+14.5% bytes). Repeated correct-baseline versus
integrated medians were decay bindings 732,949 → 407,385 ns, promotion policies
573,733 → 404,924 ns and scalar profiles 188,700 → 171,856 ns. GC-disabled
scalar profiles were 125,868 → 128,696 ns (+2.2%); that variation is retained.
Both representations were checked at 0/1/32/1,024 entries for ownership, ordering
and non-nil empty results, with CPU profiles. This is the concrete measured reason
for the remaining small scalar sort, not an assumed abstraction penalty.

## Test and analyzer limits

`make test` completed successfully in 677.4 seconds on staged tree
`95f60e14a9e24dcb6774edef4538e18c948ed1a9`, parent head
`2fdc8fdfc9a8008abbc8fb1ab441ff4783e2480c` and the release baseline above.
Both lint gates passed; 134 packages passed (9 freshly executed, 125 Go-cached),
including fresh storage, Cypher, search and storage-census runs. Working-tree
fingerprint `5ea02eb292f32976a09ca1d318f11a4e1ea8702b5fb46da6c1d2d42c4083c64c`
was unchanged throughout. Subsequent edits only record qualification in Markdown;
the committed runtime/test input hash remains the value above.

The completed unchanged duplicate scans analyzed 587 functions/87 total candidates
and later 980 functions/75 reuse candidates without cap/unsearched skips. Every
candidate has a concrete disposition in
[the review](../development/durable-cache-duplicate-review.md). The later frozen
remote scan timed out at `/nornicdb/search`; no final clean checker result is
claimed. The maintainer explicitly authorized the prior complete inventory as the
guide. The unchanged ARM SIMD header's symbols also have an analyzer extraction
limitation; the Mac build compiles the header. No checker/hook changes are included.

The final gate's first completed attempt failed only a wall-clock storage cost
assertion (ratio 4.86 > 4). The corrected test checks allocated work on the same
real Badger path and unchanged 1,000/16,000-degree fixtures: 28 allocations at
either degree across three repetitions and a focused race pass. A restored hub
scan fails the unchanged ratio bound with 15,038/240,055 allocations (15.96x).
No production change, threshold increase or skip was used; the old short-mode
skip was removed. This detects the original allocating scan, not every hypothetical
allocation-free CPU regression. The failing gate and mutant logs are preserved.

`make test` is the maintainer-selected gate. No full `make verify`, complete fresh
coverage report, full-suite race or new Neo4j differential is claimed. PR #1676 is
stacked on #1548: merge #1548 first, then its owner must restack and reverify #1676.
Its separate acceptance criteria are not claimed here.

External reproducibility evidence: `~/.local/share/nornicdb-pr-readiness/next-after1655`
contains source/binary receipts, ABBA raw samples, CPU profiles, initial and refreshed
candidate ledgers, fail-first release controls, final gate full output and hub-scan
mutation/restoration receipts. Logs from canceled/interrupted runs are not passes.


## Publication and evidence retention

Implementation commit `7189928a86e153e0d92aba72f2f9e317cc4f676b` is pushed to
PR #1548. Its normal unchanged advisory hook reached the comparison deadline;
that additional attempt is incomplete, not a clean result. NOR-34 is Test Complete
pending review/merge and release. Final documentation records only this evidence;
qualified runtime/test inputs are unchanged.

Expanded snapshot and retired experiment-binary artifacts are now compressed
under the external `cleanup/` directory, verified before originals were removed.
Raw results, profiles, repro sources, ledgers and final release/current benchmark
binaries remain directly accessible. Restore a referenced retired artifact from
its named cleanup archive if needed. Active worktrees/shared Go cache are
preserved after the app rejected baseline archival as still in use. Docker was
unavailable; no image/container was created for this PR. No remote branch or
merge policy was changed, and the PR is not merged.

## PR #1615 follow-up: explicit read-only COMMIT cost

The historical checks above missed a read-only COMMIT workload. The supplied
regression recipe measures BEGIN → an empty-label count → COMMIT, not a read
inside one retained transaction. It also measures a fresh query literal per
transaction. A local reconstruction reproduced the regression between
`343ca141f4e2516883e5f83cac12efe022a4a3b1` and
`67ac9fe1c753708f786a317f6e34ec5e03bcf3e7`; the unavailable original harness
and its reported absolute timings are not claimed reproduced exactly.

Allocation profiling identified global cache-map recreation at read-only
publication, rather than copying stored graph data on each read. The correction
keeps warm caches when the transaction's declared cache scope proves it made no
changes. Real writes, unknown/raw writes, publication ordering and intervening
invalidations retain their existing behavior. Tests cover those distinctions
and the public explicit-transaction path. No storage-format change is needed.

The retained `BenchmarkNOR180TerminalCost` uses a real WAL, identical fresh
fixture and statement sequence on all three revisions. Apple M2 Max, Go 1.26.9,
three alternating-order rounds; fixed 5,000 iterations for cached-query reads,
500 for new-query reads and 2,000 for each write/rollback control. These are
matched local measurements with concurrent ordinary/worker activity, not a
statistical or isolated whole-system qualification.

| Operation | Before #1548 µs/op | After #1548 µs/op | Corrected µs/op | Before B / allocations | After B / allocations | Corrected B / allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `read_in_tx` | 44.80 | 164.38 | 45.79 | 14,269 / 184 | 2,548,111 / 335 | 14,458 / 191 |
| `new_read_in_tx` | 569.98 | 720.60 | 551.23 | 603,823 / 7344 | 3,137,517 / 7495 | 613,421 / 7492 |
| `COMMIT` | 75.56 | 76.75 | 74.69 | 31,160 / 484 | 32,774 / 502 | 32,633 / 502 |
| `ROLLBACK` | 44.01 | 43.33 | 44.74 | 21,284 / 283 | 21,340 / 283 | 21,279 / 283 |
| `implicit` | 70.33 | 73.05 | 73.45 | 27,575 / 437 | 29,170 / 455 | 29,104 / 455 |

The corrected cached-query sequence uses about 14 KiB instead of 2.4 MiB; its
median is 2.2% above the pre-#1548 baseline in this series. Fresh-query latency
is 3.3% below baseline, with 1.6% more bytes and 2.0% more allocations. Write
controls retain #1548's ownership/publication costs (18 additional allocations
against the older, incompletely owned write baseline); they are not described
as allocation neutral. The earlier per-entity Grow experiment increased memory
and was rejected.

Reproduce the workload with:

```sh
go test ./pkg/cypher -run '^$' -bench '^BenchmarkNOR180TerminalCost$/^read_in_tx$' -benchmem -benchtime=5000x -count=3
go test ./pkg/cypher -run '^$' -bench '^BenchmarkNOR180TerminalCost$/^new_read_in_tx$' -benchmem -benchtime=500x -count=3
```

Raw samples, profiles, source recipes and the three binary receipts are retained
under `~/.local/share/nornicdb-pr-readiness/pr1615/tx-perf-1548/`.
The corrected measured binary SHA256 is `d73241b183df7d026bfb100b7c033f0467ad81ca150920eb3185c6cc467aedec`.
This component result is not the final PR-head make-test gate; later integrated
source changes must retain applicable evidence and pass that gate.
