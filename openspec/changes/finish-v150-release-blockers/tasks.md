# Combined v1.5.0 blocker completion (#1848)

- [x] Reproduce and correct CALL/UNION branch binding loss; preserve public route matrix.
- [x] Reproduce quoted-expression failures; consolidate quote/comment lookup and fix full postfix receiver chain.
- [x] Enforce requested Bolt READ scope through canonical resolved statement effects; allow dynamic reads and EXPLAIN.
- [x] Reproduce and correct misleading disabled-lifecycle startup claim.
- [x] Finish deleted-entity receiver checks and structurally bounded null tests; preserve allowed empty views, nested containers and own-delete property SET.
- [x] Reject dynamic-token parameter type errors before execution on local/internal/Fabric routes; preserve request-independent caches.
- [x] Avoid hydrating node payloads during namespace detection; migration and reader failures remain covered.
- [ ] Stop background snapshots admitting cold bodies into the query cache; preserve pinned payloads, detached images and ordinary admission.
- [ ] Independently verify integrated public behavior and affected packages/races.
- [x] Complete first-build exhaustive membership qualification for the tested memory-backed 6,000-node population; preserve qualification limits.
- [ ] Compare cold parser, cache-copy, snapshot overlap and vector-anchor working sets against released v1.4.3.
- [ ] Resolve every confirmed remaining blocker; record required-correctness costs accurately.
- [ ] Review duplicate candidates/dispositions and all source inventories without weakening analyzers.
- [ ] Pass final local required checks before dispatching CI again.
- [ ] Update PR, release ledger and Linear with exact-head evidence; clean only inactive task resources.

Performance acceptance: equivalent v1.4.3 workload/configuration; <=15% regressions tracked, >15% regressions blocking. No confirmed blocker deferred. No CI dispatch until all blocker corrections and local checks complete. No performance measurements while tests/builds run.
