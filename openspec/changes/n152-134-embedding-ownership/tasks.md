# N152-134 TODO

- [x] Read repository guidance and investigate canonical cache owner using DeepSeek; parent independently review proposal.
- [x] Reproduce exact12 plain/typed/fallback miss/hit cells and2 duplicate-hit cells on f77; five existing controls pass.
- [x] Add deterministic double-check miss and aliased batch-miss controls; confirm RED before production edits.
- [x] Capture production-unchanged baseline binary and quiet8-cell benchmark with CPU profile.
- [x] Copy cache retention and every result boundary using slices.Clone; preserve batch provider answers, keying, counters, errors and LRU.
- [x] Focused/package normal, race and vet pass; matched benchmark records necessary ownership cost, not speedup.
- [x] Manual duplicate review and scoped verification complete; staged checker attempted with ingestion failure; full qualification pending.
- [ ] Commit/push draft PR, update Plane and preserve evidence; retain active review resources.

Parent scope decisions: no redundant clone helper; no same-key batch value arbitration change; no provider-concurrent-mutation guarantee. The bounded256-entry benchmark covers1024/4096 dimensions with single hits and16-result hot/duplicate/half-miss batches. Proposed b.N-sized unbounded caches are rejected for memory safety; typed routing and shared provider-buffer boundaries are asserted in correctness tests instead of multiplying identical-owner benchmark cells.
