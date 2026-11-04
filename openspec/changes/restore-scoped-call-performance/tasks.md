# Restore scoped CALL performance — PR #1682 / NOR-214

## TODO

- [x] Reproduce the reported query shapes on generated, ephemeral Record/Reading data with repository-documentation payloads and exact result assertions. The original 24-case application fixture is unavailable; these substitutes do not qualify it.
- [x] Preserve current release refactors; adapt retained syntax through CALL imports, routing, projection aliases and iteration readers instead of restoring the original PR stack. NOR-321 remains excluded; #1663 must consume this syntax owner.
- [x] Fix keyword-variable and comment/newline alias controls, preserve cancellation/diagnostics, and qualify the integrated grammar-reader corpus.
- [x] Filter rejected nodes before full materialization, resolve tokens at the scan snapshot, and preserve query-metadata/full-vector sidecar contracts.
- [x] Compile temporal constructor arguments once while retaining the canonical evaluator, statement clocks, row bindings and errors.
- [x] Reuse per-traversal working slices and allocate path binding maps only when populated; preserve owned result paths.
- [x] Reuse the row's statement-context binding instead of copying the row for every text operand.
- [x] Revoke a large commit's cache facts once at final publication; retain unknown-scope invalidation and stale-fill protection.
- [x] Share the HTTP/Bolt per-database executor configuration, including zero-entry result-cache policy; retire the dead CLI configurator and preserve the live base-executor invalidation capability.
- [x] Decode required filter properties first and defer other value spans through one token walk and one value-decoding owner; release borrowed spans before returning.
- [x] Reuse the sequential scanner for dense labels and indexed candidates for sparse labels. Reproduce and fix the exposed dense-label tombstone visibility defect using the existing marker/readiness owners.
- [x] Review newly discovered lexical-population members and refresh stale stored-entity source references without changing census rules or assertions.
- [x] Reject the shared global decoder, aggregate admission and atomic context experiments after measured allocation/latency regressions or no workload benefit. Preserve their evidence outside the checkout.
- [x] Use the existing Northwind seed/query owners and the Graphify inventory query; avoid a rewritten synthetic query suite.
- [x] Finish dense/sparse focused race controls on the final integrated source (13.620 s).
- [x] Pass full `make test` on the frozen integrated source: 147 package passes, 74 fresh and 73 cached; storage census 831.099 s. The earlier failed/superseded runs remain distinct from this passing gate.
- [x] Run the isolated native Northwind matrix on pushed checkpoint `5a7c16e7`: synchronous default, adaptive write-behind and strict durability, each with default result caching and result caching disabled. Keep GC enabled and preserve all graph counts and 14 result fingerprints.
- [x] Report acknowledgement separately from completion. Graceful post-query drain is not an ingest-to-flush latency measurement; restart verification is not a crash-recovery proof.
- [x] Profile all 14 in-process execution queries and repeat historical controls with the same Go 1.27.3 toolchain. Indexed CALL is +1.17% versus the historical baseline; the other generated controls improve.
- [x] Resolve initial positive allocation deltas using three-execution repeats and identical closed-database files. Every query uses fewer bytes/allocations than release f62. Retain the measured canonical DFS accumulator and active-edge cleanup;4096-child bytes−11.05%,allocations−6.70%,time−7.99%.
- [x] Pass full `make test` on published follow-up2d036:147 package passes,36 fresh and111 cached; blocking storage census710.772s. The frozen source receipt matches the pushed checkpoint. Any later retained tuning needs a new final gate.
- [x] Publish candidate tables and limits in docs/performance/pr1682-read-query-results.md. The maintainer requested an immediate draft push for review while the follow-up full gate runs.
- [x] Finish write-behind tuning, completion-boundary evidence and the frozen performance qualification before starting the fresh full `make test`.
- [ ] Publish the maintainer-requested review checkpoint with current PR/Linear AC evidence; full `make test` remains pending, so do not declare merge-ready.
- [x] Retain one ordered compact cache-fact slice:24–25% fewer bytes at100K facts, unchanged allocation counts, exact old/new fact emission controls. Reject split slices for extra small-commit allocations.
- [x] Finish independent current-source DeepSeek reviews. The initial immutable-snapshot review could not see uncommitted proposals and is not acceptance evidence.
- [x] Exclude new WAL compression/frame buffer reuse after native kernel-memory comparisons exposed larger peak tails across strict/WB modes. Restore all8 experimental WAL source/test files to HEAD; preserve patches, measurements and format/race proof outside the checkout. Existing WAL behavior remains.
- [x] Qualify retained cache-fact-only strict/WB native ACK/completion/kernel-RSS controls:12 alternating runs preserve all9 counts; completed times and peaks remain within baseline variation.
- [x] Finish default-sync immediate completion controls, final6-cell native query matrix,3 reopens, all5 generated R17 controls and both actual Graphify importer modes. All counts/hashes match; indexed R17+3.8% remains within5%.
- [ ] Pass the fresh full `make test` on the retained frozen source. It starts only after performance qualification completes; plan-only `make verify-list` passes. No new optimization experiments are planned.
- [x] Correct the benchmark seed acknowledgement owner: consume terminal summaries and retry only typed retriable server statuses. Fail-first reproduction, fixed controls and focused race pass for serial/parallel errors and unknown commit outcomes.
- [x] Reproduce and correct the shared seed verifier’s omitted ORDERS population check, using the exact existing plan total. Migrate all3 engine callers; focused race passes1.716s. Optional external-engine runtimes remain unexecuted.
- [ ] Complete task-owned workspace cleanup after readiness, preserving active ingestion and required evidence.

## Source and measurement controls

Target/base: `f62b1b137a87efa4d7d35a64a57a3c96f92e0c1d` (`release/v1.4.4`). Historical reference: `b6d8e762ab9a9bd1e954aca5dd4f4257805fa8c2`. Original PR reference: `bf091924889f1903fe05fd74bb2ca45e8e7b645f`. Pushed checkpoint: `5a7c16e7f4cf87ab5ad65457f24a85bf91b94821`. Its integrated source passes the full gate and the native matrix. The measured traversal follow-up is ready for a draft review checkpoint under the maintainer request; its full runtime gate is running.

Historical comparisons before `pr1682-pinned-r17-qualification` auto-selected different Go toolchains and are diagnostic only. Controlled comparisons explicitly pin Go 1.27.3, identical documentation bytes, parser, normalization, CPU and GC settings. Generated data is 10K Readings with 1 KiB text and hourly datetimes plus 2K Records, in native in-memory Badger. Distinct statements prevent result-cache hits; warmed parser/process state is stated rather than called process-cold.

The final native Northwind matrix reuses the script defaults: 96 categories, 144 suppliers, 1,200 customers, 48K products, 48K orders and 1–6 order lines; batch 2,000, parallel 4, seed 42. Expected population: 97,440 nodes and 312,050 relationships. Graph-only mode, nornic parser, normalization disabled, GOGC=400 and GOMEMLIMIT=8GiB (a Go soft memory limit, not a hard RSS limit) remain identical. Only result-cache policy changes between cache cells. Synchronous default uses the existing batched WAL sync policy; strict mode additionally requires synchronous durability. Write-behind acknowledgement is not labelled durable completion.

## Current evidence and limits

- `pr1682-native-post-traversal`:frozen follow-up native six-cell matrix and three reopen checks PASS; all14 fingerprints and9 counts agree. The follow-up full make-test gate passed147 packages (36 fresh,111 cached). The later retained source is qualified separately under `pr1682-retained-final`; its full gate is running. Complete tables:docs/performance/pr1682-read-query-results.md.

Evidence lives under `/Users/timothysweet/.local/share/nornicdb-pr-readiness/`.

- `pr1682-pinned-r17-qualification`: corrected compiler controls and historical baseline.
- `pr1682-final-profile`: intermediate integrated results after the dense allocation correction: indexed CALL 5.68–5.85 ms / about 3.68 MB / 41.9K allocations; unindexed CALL 87.8–88.3 ms / about 4.34 MB / 45.8K allocations; datetime scan 36.6–37.3 ms / about 30.53 MB / 274.5K allocations. The later visibility fix has no extra lookup when policy is disabled, but final-source measurements remain required.
- `pr1682-final-profile/decoder.log`: 64 B through 64 KiB, integer/string predicates and 0/50/100% kept. Selective hints remove rejected-value allocations; all-kept allocation counts match unhinted decoding. Extra framing adds latency when all large values survive; this is not hidden as a universal win.
- `pr1682-dense-visibility-fail-first.log`: dense scan wrongly returned the tombstoned row. `pr1682-dense-visibility-fixed.log` passes the correction and existing controls.
- `pr1682-final-inventories-corrected.log`: lexical population, URL sink inventory and runtime-fixture ownership pass. An earlier attempted command named a nonexistent package; its failed log is retained and not counted.
- `pr1682-native-final`: completed six-cell matrix, source receipts, all 14 query tables, reopen checks, same-toolchain historical controls and current Graphify importer profiling. The initial six positive1x allocation deltas do not persist in repeated and identical-file controls; no allocation regression is established. Earlier `pr1682-server-matrix` checkpoint and `pr1682-server-candidate` rejected-experiment runs remain baseline/diagnostic evidence, not final-source qualification.

No storage key/payload/version, third-party implementation, generated parser or checker rule changes are proposed. Broader `make verify` is deferred to CI under the maintainer's explicit gate.

## Duplicate review and remote ingestion

The remote code graph is incomplete after a relationship-inventory timeout. The maintainer explicitly waived the checker this round and requested current-source review for duplication and leaf patches. No clean checker run is claimed. Five review units examined the earlier integrated source; incremental reuse/consistency review examined the scan changes. DeepSeek source review `e9139de4ef1f4d83ae86f907094e4d08` completed in 402 seconds and exposed the tombstone defect, now reproduced and corrected. Its static observations are not runtime proof.

Dense and sparse scans are physical strategies under the same scoped owner. The ready marker establishes index completeness, production writes update labels atomically with bodies, and both strategies use the same snapshot and overlay. The dense strategy reuses the existing generic scanner rather than collecting every candidate and point-reading its body (the rejected candidate used about 631 MB/op versus about 4.34 MB/op after correction). Sparse scans avoid unrelated bodies; dense filters may inspect other labels before residual label checks, as permitted by the optional hint contract. Full-node caches were already bypassed by generic filtered scans; this change does not introduce that behavior.

A single task-owned background waiter may retry the release-head refresh when the remote becomes healthy. Do not remove its baseline checkout or overlap another ingest. Remote completion is not established by the importer unit tests.

## Final audit correction checkpoint (2026-11-04)

- [x] Preserve the retained-candidate full-run failure: numeric wire ownership, URL sink structure, installed rollback review and nil-context census; storage census itself passes (833.652 s).
- [x] Review the three benchmark report writers and re-pin their unchanged JSON boundary after exact seed-population verification changed their enclosing bodies.
- [x] Trace all three changed cache-fact expressions through `VisitAffectedFacts` and durable cache invalidation; replace the two obsolete graph-endpoint classifications with the three current exact expressions.
- [x] Compare the rollback review's compiled static closure with parent `2d0362da`: all 197 members remain, 196 bodies are identical and only `physicalCacheProjection.add` changes. Preserve all thirteen source pins, two compensation callers and the existing human-approved phase boundary.
- [x] Reproduce and correct the source-pinned audit records without changing audit implementations, checker rules, contexts, runtime code, storage formats or dependencies.
- [x] Pass all four affected packages, including the installed rollback record's five mutation controls and the blocking nil-context census.
- [ ] Pass fresh integrated `make test` after the manifest corrections. The operator requested an audit-only review checkpoint while this gate runs; do not report the full gate passed prematurely.

Review evidence: `pr1682-retained-final/rollback-closure-delta.json`,
`capture-corrections-before.log`, `audit-corrections-focused.log`,
`audit-corrections-packages.log` and `audit-correction-rollback-runtime.log`
under the persistent readiness evidence directory. Performance qualification
remains tied to unchanged production Go/module inputs from `34614a39`.
