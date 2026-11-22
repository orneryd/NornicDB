# N152-90 tasks

- [x] Recover the supplied public retrieval benchmark and read-count/search controls; retain provenance and historical limits in evidence.md.
- [x] Confirm the current release's per-candidate full-chain read cost on both parsers at 1000/2000 nodes.
- [x] Capture a current-release CPU/memory-profiled public first-page baseline at 12.5K and 49K nodes with controlled providers.
- [x] Demonstrate the new suffix-read assertion fails before production changes.
- [x] Reuse existing prefix admission/evaluation in boundChainCount; cache syntax only and isolate mutable predicate selection per evaluation.
- [x] Keep final correlation and WHERE ownership intact; retain safe error deferral for speculative paths through the existing logical error owner.
- [x] Prove fresh parameters/entities, concurrent shared plans, volatile/deferred predicates, zero-hop bindings and missing/complete-path errors under both parsers.
- [x] Verify current read reductions: 1712/2712 all-admitted reads at 1K/2K, first-page 1081/2712, unchanged candidate/result identities and no store scan.
- [x] Run focused race on the initial error-corrected candidate (42.799s) and search ranking/grouping/provider/cursor lifecycle controls (1.338s).
- [x] Investigate the 1K reranked slowdown with CPU profiles; retain complete immutable predicate plans and reuse canonical explicit value/error evaluation.
- [x] Pass focused tests after that optimization (3.122s), including complete-plan errors, PROFILE, disabled programs and concurrent reuse.
- [x] Re-run final race after complete-plan optimization and byte-identical owner extraction: 42.759s.
- [x] Inspect sibling traversal consumers: match_multi bound-pattern entries consume a pattern, not the subquery body's WHERE. They keep the same shared traversal owner; this change does not invent a WHERE contract for them.
- [x] Run verify-list and scoped localization verifier; record full-verifier skips.
- [x] Run the staged duplicate checker after final production correction; graph search timed out. Manual owner review recorded separately.
- [x] Finish matched final latency/allocation measurements, including 1K/2K and plain controls; record ranges/limits in benchmark-results.md.
- [ ] Open release/v1.5.2 draft PR with exact measured scope and update Plane.
- [ ] Complete graph-backed duplicate discovery and full release qualification before merge readiness.

No claim covers the unreproduced private provider-inclusive latency report or
16-way stress. The benchmark uses explicit embeddings, controlled reranking,
real search execution, disabled result caches and deterministic graph data.
