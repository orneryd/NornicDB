# PR #1690 / full NOR-136 release acceptance

Release baseline: `b098f7e708b4c701a743ffb0310a9b179a84c8f2`.
Original test reference: `56b1e8e2416b5c4b36d7d7751b24a08d8f1c429b`.
Operator expanded this PR to ALL NOR-136 criteria, including NOR-138/140/143.

- [x] Confirm #1683 merged, fetch current release, preserve original PR head and isolate checkout.
- [x] Assign NOR-136 to Tim Sweet and confirm v1.4.4 scope in Linear.
- [x] Port original acceptance tests to canonical APIs without transaction forwarding adapters.
- [x] Reproduce retained-history/write-behind failure on unmodified release under both parsers.
- [x] Correct snapshot completion ordering and pass both-parser race checks for the initial slice.
- [x] Reproduce stale label counts after recovered relabels across native, ordinary and transaction-aware recovery.
- [x] Complete shared recovery finalization and mixed semantic/physical WAL ordering; validate all affected public consumers.
- [ ] Reproduce/qualify unflushed-peer autocommit and explicit writers over Execute, Bolt and HTTP, both parsers.
- [ ] Qualify exact acknowledged increments, disjoint writers, flush and restart.
- [ ] Qualify mixed update/delete/property rename/relabel recovery, independent count recounts and bounded repeated stress.
- [ ] Qualify old-image prevention, node/relationship resurrection, endpoint deletion and UNIQUE holder controls across memory/write-behind modes.
- [x] Run paired representative write-behind throughput/allocations and CPU profiles without concurrent full-suite load.
- [ ] Independently review the complete integrated implementation; preserve refactored owners and storage formats.
- [ ] Run final combined `make test`; fix every failure. The earlier slice gate was canceled as superseded, NOT passed.
- [x] Record informative duplicate analysis and manual reuse evidence honestly; never modify checker/hooks to obtain a pass.
- [ ] Update complete PR/Linear AC mapping, commit and push the review candidate.
- [ ] Clean completed task-owned resources, preserving evidence and active work.

Completed DeepSeek proposals: protocol reproduction, native restart/count/UNIQUE,
cost harness, shared protocol fixture convergence, public mutation matrix and
unflushed UNIQUE strengthening. Parent independently reviewed and integrated them.
Completed final read-only review: `afb7df73de58487e9db77592a413021e`.

Evidence: deterministic release reds (both parsers, lost properties/increments,
restore loss, and relabel/delete underflow preventing reopen); paired cost has
six samples per side/case, unchanged allocations, near-identical bytes, median
latency -1.6% to +0.4% on the final runtime source. Blocking compiled context census passed. First integrated
race run exposed an adaptive-flusher fixture race; its failing assertion is
preserved and the rerun uses the existing worker-stop seam for controlled cases.
The final full make-test gate is still pending.

Explicit-ID create/create node and relationship cases reproduced on unmodified
release and pass after including creates in the committing write set. Final
both-parser race and fresh-cache make test are running. Operator requested a
draft review push before their completion; readiness remains pending those gates.

The completed remote run on pushed 332543ff5 analyzed 76 functions, returned two
reuse findings, and skipped none. Both have consolidation changes and explicit
dispositions; the parameterized UNIQUE assertion reuses the existing typed-error
extractor, and the complementary corruption controls now share one test setup.
The superseded pre-disposition full run was canceled without observed failures
after CLI/Cypher/ANTLR passed; its log and exact cancellation receipt are retained.
The next full run uses the converged source and normal Go cache eligibility.

Follow-up staged analysis completed on tree a4b53bd9 (published 666aebcb8):
seven analyzed, one additional concurrency-test reuse finding. Its scalar and
list cases now share the existing relationship-UNIQUE concurrency table, exactly
preserving the original three schedules. The superseded converged sweep was
canceled before edits; no full pass is claimed. Finish staged checker convergence
before restarting the final full gate.
