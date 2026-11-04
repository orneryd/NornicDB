# Restore generated read performance (PR #1682 / NOR-214)

## TODO

- [x] Reproduce distinct scoped-CALL preparation on current release f62 and historical b6.
- [x] Generate ephemeral Record/Reading data from repository documentation, with exact result assertions.
- [x] Add and execute the missing datetime-scan and CALL/UNWIND/ID-lookup substitute shapes on immutable baselines and the candidate.
- [x] Integrate retained WITH projection, UNWIND/FOR binding and CALL body/column readers into current owners.
- [x] Verify the integrated focused behavior and refreshed LL/SLL structure/diagnostic corpus; exclude only per-parse ownership numbers from structural comparison, with negative controls.
- [x] Repair fixture registration while preserving explicit closure; pass static inventory and affected lifecycle tests.
- [x] Reconcile the current lexical population with the actual retired/replacement readers; 3,960 reviewed candidates pass, assertions unchanged. Recheck after final production edits.
- [x] Build the missing local UI assets.
- [ ] Finish measured allocation experiments; preserve results, diagnostics, cancellation and the current release refactors.
- [ ] Close the historical preparation latency gap and the generated labeled-lookup allocation gap; repeat paired checks on frozen integrated source.
- [ ] Review remaining retained-syntax consumers and keep NOR-321 excluded.
- [ ] After the original production AC, profile existing Northwind and current Graphify script query shapes, reusing repository fixtures/benchmarks.
- [ ] Run final affected/race checks and fresh `make test` on final source; fix every failure before push.
- [x] Complete the five-part manual ownership review authorized in place of the unavailable remote checker; no clean checker result is claimed.
- [x] Resolve review findings: retain grammar-owned aggregate aliases; migrate unscoped CALL runtime/static/admission consumers to the retained import/branch owner; reconcile the exact lexical inventory.
- [x] Reproduce and fix the exposed public READ-procedure sidecar regression with canonical scan hydration modes, and keep filters before sidecar decoding.
- [x] Make the old-snapshot/replay class test use an explicit manual replay schedule; retain strict old-snapshot assertions and pass 20 repetitions.
- [x] Fix the keyword-variable rejection regression in the canonical DISTINCT modifier reader; focused integrated keyword/import/generated controls pass.
- [ ] Qualify the remaining datetime allocation correction, including bounded decode-state retention and error recovery.
- [ ] Push the reviewed candidate into PR #1682, update exact-head PR/Linear evidence, and perform task-owned workspace cleanup after readiness.

## Contract and source

The maintainer approved deterministic generated substitutes because the original 24-case R17 application fixture is unavailable. These results do not qualify that original suite. NOR-321 is excluded; #1663 must consume the resulting syntax owner when restacked.

Comparison base: `f62b1b137a87efa4d7d35a64a57a3c96f92e0c1d`. Historical reference: `b6d8e762ab9a9bd1e954aca5dd4f4257805fa8c2`. The original reference head is `bf091924889f1903fe05fd74bb2ca45e8e7b645f`; this branch is the current-release adaptation. The maintainer requested a draft review checkpoint before final runtime verification and tuning finish. No third-party/generated parser, storage payload, Badger-key or storage-version change is made.

## Measured results and limits

Apple M2 Max. Twenty scoped CALL sections with distinct aliases avoid literal-normalized semantic-verdict reuse. Current-release samples measured 97–100 ms / 141.8–142.2 MB / about 1.714M allocations. After retained bodies and observer changes, intermediate candidate samples measured 7.2–7.6 ms / about 4.23 MB / about 38.1K allocations, versus historical b6 6.4–6.7 ms / 4.29–4.31 MB / about 43.1K. Historical latency parity is still open. These are warm-process/parser samples with distinct text, not process-cold production qualification.

The later whole-source CALL routing correction measured 6.64–7.42 ms / 2.77–2.81 MB / about 26K allocations in three 50-execution samples; a contemporaneous historical b6 run measured 6.89–8.07 ms / 4.29–4.30 MB / 43.1K allocations. These samples overlap timing variability while reducing bytes and allocations; they ran alongside a worker's package checks, so final frozen paired measurements remain necessary. A separate 500-execution query-only CPU profile measured 6.63 ms / 2.79 MB / 26.1K allocations. The source hashes are recorded in `pr1682-whole-routing-source.json`; none of these intermediate runs is the final test gate.

The generated fixture has 10K Readings (1 KiB documentation slices plus identifiers/source/hourly datetime) and 2K Records. The datetime scan asserts exactly 24 values in its time window; the lookup asserts exactly the requested 25 IDs and sources. Paired three-iteration medians before the type-iteration experiment:

| Shape | b6 ns/op | f62 ns/op | Candidate ns/op | b6 B/op | Candidate B/op | b6 allocs/op | Candidate allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Datetime scan, distinct text | 49,704,931 | 60,294,125 | 59,704,583 | 51,572,850 | 49,934,560 | 393,033 | 431,165 |
| CALL/UNWIND lookup, distinct text, no index | 631,446,792 | 647,663,458 | 631,715,389 | 1,107,980,146 | 1,092,204,458 | 6,503,987 | 9,005,794 |

The later filter-before-materialization correction removes that lookup gap: three integrated samples measured 76.2–77.1 ms, 4.63–4.76 MB and 192.6–198.8K allocations after the sidecar read-contract fix. It uses the streamed row instead of a second body lookup, resolves names at the scan snapshot, and propagates malformed-node errors. This is intermediate-source evidence; final frozen checks remain required. The private bounded MessagePack decode-state correction reduces the datetime scan to about 49.08 MB / 421.2K allocations, preserving large inputs and rejecting pooled error-state retention. The remaining roughly 28K-allocation gap versus historical b6 is still under investigation. Repeated identical-query controls use default result caching and include the first execution, so they do not establish uncached execution throughput. Literal-varying controls may reuse normalized semantic verdicts.

The earlier optional-match large-graph measurements used a fixture without the newly added datetime property. They remain historical evidence for that earlier fixture, not paired qualification of this updated data. Reading.id index controls showed a substantial reduction in scan work; index creation was excluded from timing.

## Verification

The review checkpoint passed `go test ./... -run '^$' -count=1`: all test packages compile, and that command runs zero tests. Integrated keyword/import/generated controls, private MessagePack utility race controls and the 3,964-entry lexical inventory pass. This is not a full `make test` result.

Integrated generated-result tests, CALL/projection/iteration controls, fixture lifecycle checks, lexical population and refreshed parser corpus parity pass on intermediate integrated source. Workers' isolated results are proposals until independently integrated. Source changes require renewed affected/final checks.

The earlier full `make test` failed on missing UI assets and fixture/lexical inventories. It was stopped after later production changes superseded its source; its log is preserved. This is not a passing gate. Final `make test` is still required. Broader `make verify` is deferred to CI under the maintainer's requested gate; `make verify-list BASE=origin/release/v1.4.4` was recorded, not treated as verification.

## Remote graph and duplicate review

The older b01 ingest failed after 2.94M relationships with `Neo.TransientError.Transaction.Outdated` during relationship SET. The serialized f62 refresh extracted 70,879 nodes / 3,591,401 edges and imported 71,348 nodes, then timed out reading the existing relationship inventory. Even a small remote read subsequently timed out. The indexed graph is incomplete and must not be treated as current.

The importer now bounds inventory requests to 200 source nodes while preserving their complete fan-out, repo isolation and stale-deletion guards. Its 80 Python tests and real-executor batched query controls pass; this does not prove remote completion. A single background retry waits for health, then a read-only pilot, before extracting the latest immutable release head. It records its chosen SHA and verifies the completion marker. No overlapping ingest is running.

The remote checker never completed analysis. The maintainer explicitly directed us to ignore it this round and manually review the code for duplication and leaf patches. No checker/hook rules or thresholds were edited. Five review units (correctness, security, clarity, reuse and consistency) reviewed the frozen source; findings are being corrected in the shared owners. The public alias case already passes due statement normalization, while a retained-clause component reproduces rejection of newline/tab/comment-separated aliases. Its correction uses the existing grammar AS offset. The CALL import finding is repeated parsing of retained branch syntax, and the inventory finding named two deleted type readers, now removed. Aggregate alias correction passes focused component/public controls. A subsequent query-only CPU profile attributed 14% of its samples to CALL routing parsing a suffix. The existing grammar frame reader now accepts an original-source offset so all callers use the same full-source owner; procedure, OPTIONAL CALL, raw/accepted coordinates, diagnostic and cancellation controls pass. An independent correctness review found no issue in this coordinated migration. Manual review is not a successful duplicate-checker run.

## Evidence and delegation

Logs/profiles/source receipts are under `/Users/timothysweet/.local/share/nornicdb-pr-readiness/pr1682-*`. Key logs: `pr1682-make-test.log`, `pr1682-body-parser-parity.log`, `pr1682-body-lexical-final.log`, `pr1682-generated-integrated-tests.log`, `pr1682-type-capacity-query-bench.log`, and the three `*-generated-shapes-bench.log` files. The source receipt `pr1682-pre-type-iteration-source.json` identifies the pre-experiment candidate; it is not final-source qualification.

CALL-body worker `062aca2a6eb848bfa3c3c0777081d143` lost its connection after focused checks; its edits were reviewed/integrated by the parent. Generated-fixture worker `0f1793c333334e99b2231f1ba058f329` completed and its tests were independently rerun. Type iteration `c98c7469258a4690994e2c6733df150b` was independently integrated. Projected filtering follow-up `0e379f0b7b574e4182793e60ff38c8fe` and retained CALL import migration `4abc181234674e0f8942902b7edb62d7` completed and were independently integrated. Their full-package results qualify their isolated snapshots only; parent follow-up found and fixed the public sidecar omission. Datetime allocation experiment `b5cb5b74a8fd4e6a991e0402010ad347` completed; its global decoder-pool proposal was rejected after defensive review `5fc3df9fdf2a4a0d8369bbb784616972` demonstrated retained buffers and raw-copy recorder contamination. The parent integrated private bounded state instead. Keyword correction `3d1791c3bccb40f9855ba4090f296620` completed and was independently integrated. The remaining allocation investigation `75d8179a97404dea8ceb481f4c171014` is active in its isolated worktree. Security review `7095aa10f76c45f7ad91217d6651cd86` completed with no findings; it was source review only. Read-only scan investigation: `7ce9e07ae1ed4d899af9508f5229535b`. No worker owns final acceptance.

NOR-214 remains assigned to Tim Sweet, Urgent, v1.4.4, Dev In Progress. The PR body and Linear progress comment explicitly retain the open gates. Cleanup is deferred while experiments, source verification and ingest remain active.
