# NOR-177 internal MATCH visibility

- [x] Assign NOR-177 and reproduce indexed range and bound MATCH failures on release 0a01ce500.
- [x] Consolidate matching visibility at canonical row admission owners
      (pipelineNodeMatchesPattern, its initial-node candidate loop, and the
      ordered index window that truncates before rows are admitted).
- [x] Verify visible ordering/pagination and privileged system reads on both parsers
      (focused nornic and ANTLR checks; system namespace asserted on the visibility
      decision and the admission test because the system database refuses graph reads).
- [x] Review integrated changes and the initial duplicate inventory.
- [x] Correct same-snapshot count eligibility, buffered visibility composition and pinned relationship traversal.
- [x] Address reviewer feedback and consolidate reported test/count owners.
- [x] Fix CI census classifications and preserve quota fault count capability.
- [x] Reproduce and repair the cache-overlap fixture at the typed count boundary, retaining cancellation controls.
- [x] Qualify a CI-only Go memory budget and coverage attempt diagnostics without weakening checks.
- [x] Reproduce fulltext reuse assertions under ambient memory pressure; isolate no-pressure setup and retain real release/refill checks (five paired repetitions).
- [ ] Complete latest integrated full-suite CI, release-failure reconciliation and updated-head duplicate review.
- [ ] Push release PR with AC evidence; complete CI and update Linear.
