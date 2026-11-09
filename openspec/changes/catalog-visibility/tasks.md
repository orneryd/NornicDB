# Catalog visibility

- [x] Inspect current Linear report and release authorization owners.
- [x] Reproduce catalog projection, limits, HTTP existence, aliases/constituents, graph functions and Fabric dispatch cases on release 12c2b2b24.
- [x] Apply selection visibility before metadata lookup, projection and aggregation.
- [x] Focused permitted/unrestricted, denied-target, live-grant and cancellation controls pass.
- [x] Public focused checks pass on both parsers; integrated race and tagged vet checks pass.
- [x] Record manual owner review and unavailable graph retrieval; update Linear and push candidate #1770.
- [ ] Complete exact-head CI before readiness.

Shared release corrections from #1769 are integrated by rebase for testing. The integrated unchanged context census passes (126.031s); focused default/ANTLR and race checks pass. Merge #1769 first. Rebase onto its release integration and finish exact-head full make-test CI and advisory review before readiness.

- [x] Integrate shared CI corrections by rebase for combined validation.
- [x] Independently repeat affected default/ANTLR and race checks on the integrated source.
- [x] Integrated blocking context census passes without a policy or assertion change.
- [ ] Refresh generated stored-entity source positions from the final shared-fix head and rerun freshness.
- [ ] Final-head full make-test CI and advisory review.

## CI server-shard follow-up

- [x] Reproduce both CI failures locally on `7b9ac63b7` (6.704s): stale nil-claims error expectation and unauthenticated slow-log fixture never reaching its gate.
- [x] Assert early unattributed-request denial and no persisted mutation; supply the shutdown fixture's authenticated principal before driving the common statement runner.
- [x] Preserve shutdown time budgets and resource/slow-log assertions. Focused public/server race controls pass (15.390s).
- [ ] Run full final-head make-test CI after shared-fix integration; earlier server-shard run is failed, not passed.
