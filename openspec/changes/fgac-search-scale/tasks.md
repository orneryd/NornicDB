# Tasks

- [x] Inspect current release head and Neo4j's documented index authorization contract.
- [x] Add a failing public-path regression for repeated restricted keyword queries.
- [x] Measure baseline node keyword and vector search on named restricted and unrestricted fixtures; compare the integrated head before PR.
- [x] Implement shared field-aware BM25 scoring and bounded, revisioned visibility metadata.
- [x] Batch repeated vector owners and reuse revision-scoped admission decisions while retaining result and access semantics; exact completion scans remain for complete pages.
- [x] Add regression tests for hidden fields, complete visible pages, updates, policy changes, restart, cancellation, and unrestricted cost.
- [x] Run focused, package, race, vet, and matched benchmark checks; document CI duplicate and combined release verification as pending.
- [ ] Open one PR against `release/v1.5.3` with the verification evidence.
