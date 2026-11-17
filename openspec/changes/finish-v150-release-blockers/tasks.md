# Combined v1.5.0 blocker completion (#1848)

- [x] Reproduce and correct CALL/UNION branch binding loss; preserve public route matrix.
- [x] Reproduce quoted-expression failures; consolidate quote/comment lookup and fix full postfix receiver chain.
- [x] Enforce requested Bolt READ scope through canonical resolved statement effects; allow dynamic reads and EXPLAIN.
- [x] Reproduce and correct misleading disabled-lifecycle startup claim.
- [x] Finish deleted-entity receiver checks and structurally bounded null tests; preserve allowed empty views, nested containers and own-delete property SET.
- [x] Reject dynamic-token parameter type errors before execution on local/internal/Fabric routes; preserve request-independent caches.
- [x] Avoid hydrating node payloads during namespace detection; migration and reader failures remain covered.
- [x] Stop background snapshots admitting cold bodies into the query cache; preserve pinned payloads, detached images and ordinary admission.
- [ ] Independently verify integrated public behavior and affected packages/races.
- [x] Complete first-build exhaustive membership qualification for the tested memory-backed 6,000-node population; preserve qualification limits.
- [x] Qualify vector-anchor first-use and 80-key working set against released v1.4.3; all six runs verify exact live vectors and clean up.
- [ ] Complete cold parser, transport cache-copy and snapshot-overlap performance correction against released v1.4.3.
- [ ] Resolve every confirmed remaining blocker; record required-correctness costs accurately.
- [ ] Finish duplicate dispositions and combined census checks without weakening analyzers.
- [x] Reconcile approved lexical/runtime records; missing-entry and false-owner controls reject, restored inventory passes.
- [x] Preserve owned Go cache results and verify immutable HTTP/Bolt delivery, nested callbacks, cycles and public partial-PULL invalidation.
- [ ] Resolve the measured history-churn allocation gap against v1.4.3; keep retention and conflict semantics intact.
- [ ] Pass final local required checks before dispatching CI again.
- [ ] Update PR, release ledger and Linear with exact-head evidence; clean only inactive task resources.

Performance acceptance: equivalent v1.4.3 workload/configuration; <=15% regressions tracked, >15% regressions blocking. No confirmed blocker deferred. No CI dispatch until all blocker corrections and local checks complete. No performance measurements while tests/builds run.

## Current local verification

- Full `make test` continues on the pushed candidate's production source. Bolt passes (1392.723s). Cypher runtime checks completed; `TestNumericInputOwnership` reports seven reviewed but unregistered/changed container/compiler boundaries. Exact inventory reconciliation awaits approval; no analyzer rule changes.
- Initial UI setup and fixture-owner failures are corrected: clean UI build passes; all three affected fixture guards pass (101.685s), namespace focused races pass (2.090s).
- Retained WAL tail headers and payloads survive reused scan buffers: focused truncation race checks pass (2.865s).
- Two source-only allocation candidates pass focused checks in isolated trees; they are not integrated or performance-qualified yet. No benchmarks run concurrently with this suite, and no new CI dispatch.
