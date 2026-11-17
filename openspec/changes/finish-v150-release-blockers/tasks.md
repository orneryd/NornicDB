# Combined v1.5.0 blocker completion (#1848)

- [x] Reproduce and correct CALL/UNION branch binding loss; preserve public route matrix.
- [x] Reproduce quoted-expression failures; consolidate quote/comment lookup and fix full postfix receiver chain.
- [x] Enforce requested Bolt READ scope through canonical resolved statement effects; allow dynamic reads and EXPLAIN.
- [x] Reproduce and correct misleading disabled-lifecycle startup claim.
- [x] Finish deleted-entity receiver checks and structurally bounded null tests; preserve allowed empty views, nested containers and own-delete property SET.
- [x] Reject dynamic-token parameter type errors before execution on local/internal/Fabric routes; preserve request-independent caches.
- [x] Avoid hydrating node payloads during namespace detection; migration and reader failures remain covered.
- [x] Stop background snapshots admitting cold bodies into the query cache; preserve pinned payloads, detached images and ordinary admission.
- [x] Verify integrated public behavior and affected packages; preserve focused race evidence and report broader CI-only gates.
- [x] Complete first-build exhaustive membership qualification for the tested memory-backed 6,000-node population; preserve qualification limits.
- [x] Qualify vector-anchor first-use and 80-key working set against released v1.4.3; all six runs verify exact live vectors and clean up.
- [x] Qualify corrected small HTTP/Bolt reads against released v1.4.3; retain cold-grammar and history-phase cost disclosures.
- [x] Bound durable managed-vector anchor batching; preserve exact replay, per-database identity and generation-safe cache proofs.
- [x] Correct the merged metrics test fixture to use the canonical metric constructor; focused permission checks and make-test lints pass.
- [x] Correct the reproduced shipping blockers and record required-correctness costs accurately.
- [x] Pass combined census checks without weakening analyzers; record the bounded duplicate review and its unresolved advisory opportunities.
- [x] Reconcile approved lexical/runtime records; missing-entry and false-owner controls reject, restored inventory passes.
- [x] Preserve owned Go cache results and verify immutable HTTP/Bolt delivery, nested callbacks, cycles and public partial-PULL invalidation.
- [x] Qualify history churn against v1.4.3 and disclose required-correctness allocation costs and the remaining create-phase cost; preserve retention and conflicts.
- [x] Pass final local `GOTOOLCHAIN=go1.27.3 make test` before dispatching CI again.
- [ ] Update PR, release ledger and Linear with exact-head evidence; clean only inactive task resources.

Performance acceptance: equivalent v1.4.3 workload/configuration; <=15% regressions tracked, >15% regressions blocking. No confirmed blocker deferred. No CI dispatch until all blocker corrections and local checks complete. No performance measurements while tests/builds run.

## Current local verification

- Affected native package sweep: convert passes (1.165s), storage passes (806.931s), Bolt passes (486.804s), server passes (860.398s). Cypher completes in 657.074s with one numeric-writer fingerprint failure; no additional product assertion failure was reported. This is not a full `make test` pass.
- Approved exact lexical records and two scratch-field classifications are applied. Positive population, generated stored-census equality and stored-write ordering checks pass; missing-owner and false-owner mutations reject. Analyzer rules are unchanged.
- The explicitly approved constructor/publication pins, obsolete-site retirement and rollback closure refresh pass the strict context census and installed source/helper/new-caller rejection controls (250.159s). Missing/stale ordered-JSON writer records reject; the restored approved record passes.
- Small-read paired measurements versus v1.4.3: HTTP 113.083→122.416 µs (+8.25%), Bolt 67.625→76.667 µs (+13.37%). All responses match. One shorter Bolt sample was +15.29%; longer confirmations were approximately +12–13.4%.
- Fresh vector-anchor measurements versus v1.4.3: first update 29.945→21.156ms; warm single-key 33.150→10.656ms; rotating 80-key working set 32.214→19.952ms. Every vector coordinate, named vector and final property is checked.
- The public 8K/16K/32K Bolt update/restart regression passes in 110.433s. A 1,024-anchor component drops 1,024→13 syncs and 5.071s→115.609ms versus the prior candidate (not v1.4.3).
- Matched history totals are 55.571→63.816s (+14.84%) and 18.35→21.42 GB allocated, with all 1,016 oracles passing. The create phase remains +42.6%; do not describe that phase as corrected. First-ever grammar prediction also remains costlier; disclose the accepted grammar-correctness cost separately from warm-query measurements.
- The disk-interrupted mutant partitions and final full `make test` remain required on the integrated source. No new CI dispatched.

- The first full run found one additional stale mutation anchor after publication-frame consolidation. Its early-unlock corruption now targets the current frame declaration and is rejected by the unchanged census (43.093s). Other mutation partitions are finishing; the final frozen full run is queued after them. No test is skipped or assertion weakened.

## Final local gate

`GOTOOLCHAIN=go1.27.3 make test` exits 0: 158 passing packages, including 142 cached results. Fresh Cypher, database, search, storage, context census and all four mutation partitions pass. No failure markers are present. The 46 candidate files match the frozen source manifest; this final status note changes documentation only. Full `make verify` remains deferred to CI as requested. No new skipped tests or analyzer-rule changes were introduced. Exact-head CI and combined release verification remain separate gates.
