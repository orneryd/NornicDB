# Preserve search publication consistency

- [x] Reproduce refused-clear index loss and failed replacement shown ONLINE on release ce22592d2.
- [x] Reproduce loss of unrelated property vectors under wholesale reset.
- [x] Retire wholesale clear; reuse canonical per-node storage notifications and catch-up before return, including partial resets.
- [x] Qualify retained/refused/property vectors and regeneration controls.
- [x] Report absent per-index state from replacement service build outcome; no persisted layout change.
- [x] Verify old-refresh ordering against newest vectors and text with eager/lazy rebuilds.
- [x] Focused search/cypher/nornicdb checks pass under default and ANTLR.
- [ ] Complete successful per-index population vs not-ready/failed global build state contract; source review identified an additional boundary, not yet resolved.
- [ ] Reproduce remaining report49 workload variants if qualification does not match the original scheduling.
- [ ] Exact-head CI, race/census and broader affected-package qualification.

Parent validation: search counts use an explicit 0.9 similarity threshold for orthogonal unit-vector fixtures; the default search returns zero-similarity candidates too. No assertion is relaxed to hide that distinction. Clear retains a database lifetime ticket and catches up published refreshes before returning rather than weakening immediate completion to eventual delivery.

No new test skip/ENV gate, storage/WAL format or public Go signature change.
