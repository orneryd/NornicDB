# Catalog visibility

- [x] Inspect current Linear report and release authorization owners.
- [x] Reproduce catalog projection, limits, HTTP existence, aliases/constituents, graph functions and Fabric dispatch cases on release 12c2b2b24.
- [x] Apply selection visibility before metadata lookup, projection and aggregation.
- [x] Focused permitted/unrestricted, denied-target, live-grant and cancellation controls pass.
- [x] Public focused checks pass on both parsers; integrated race and tagged vet checks pass.
- [x] Record manual owner review and unavailable graph retrieval; update Linear and push candidate #1770.
- [ ] Complete exact-head CI before readiness.

Remaining gates: independent source review; shared release census/fixture corrections (#1769), rebase after they merge, exact-head full CI. Current context census still reports the same single WAL substitution (zero nil flows), not a pass.
