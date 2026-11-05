# PR #1406: publication and accounting readiness for v1.4.4

Target `release/v1.4.4`; preserve original author provenance. Do not merge this PR.

- [x] Pin release `b1b018d3f` and main `f2ddd27cf`; preserve original author head `0780dc7c`.
- [x] Refresh the remote code graph from release and independently verify its revision and counts.
- [x] Reproduce and correct stale deletion indexes, out-of-order publication and replay retirement through current owners.
- [x] Preserve 36 publication schedules and original post-validation peer-write/conflict assertions; focused race proofs pass.
- [x] Include hidden stored entities, cascades and replacements in canonical accounting; the 576-case HTTP/Bolt quota matrix passes normally and with race detection.
- [x] Correct late replicated accounting and pending allowance overspend; retain reservations through nonterminal timeouts.
- [x] Route accepted explicit entity transactions to peers; require a leader for explicit writes and preserve direct follower forwarding.
- [x] Preserve direct/implicit/explicit producer policy, including single-item bulk forwarded by a follower.
- [x] Recover accepted semantic WAL through actual application startup in both buffering modes; preserve receipts across replay and deletion/reopen.
- [x] Retire the approved raw Cypher/property/old-batch replication commands with reserved numeric slots and explicit rejection.
- [x] Share accounting/admission/policy across manager views; preserve actual wrapper routes, reset policy and Drop/recreate behavior.
- [x] Initialize usage before the first MERGE lifecycle lease; preserve all five HTTP/Bolt read-failure and connection-reuse cases.
- [x] Retire terminal native handles and close pending native frames without falsely discarding accepted logical work.
- [x] Use identity-only count snapshots; explicitly hydrate complete images for bytes. The 16K-vector regression and byte-upgrade/read-failure controls pass.
- [x] Project count-only deltas without copying full images; preserve the full-copy API and late byte-policy upgrade.
- [x] Integrate reviewed source in the primary checkout, refresh constructor inventory and generated census, and retain all 33 census mutants.
- [x] Migrate the approved transaction deletion Go API and callers/docs; keep the shared native/staged deletion kernel.
- [x] Migrate transaction node updates to per-operation `UpdateGraphNode(node, policy)`; preserve client and authoritative stored-image operations within the same frame.
- [x] Migrate manual suppression to the approved typed `SetManualSuppression(kind, id, suppressed)` API; focused normal/race public consumers and eight exact quota contracts pass.
- [x] Preserve original failing evidence and record the unavailable historical performance-harness limitation.
- [x] Finish paired main/release/candidate concurrent UNIQUE measurements with one compiler/configuration and CPU profiles; all 18 samples pass. See `docs/performance/pr1406-publication-accounting.md` for measured costs and limits.
- [x] Converge the reported replication, namespace-reader, accounting and staging owners; preserve distinct public contracts and retire redundant Go helpers with migration docs.
- [x] Reproduce and correct duplicate in-memory WAL publication, transport shutdown/late registration, native expansion lock cycles and temporal value-lease phases; preserve original assertions.
- [x] Restore database-config wrapped-NotFound handling and exact reporting/admission size-read contracts; public quota and configuration controls pass.
- [x] Reconcile exact credential-egress and typed state inventories after source review; physical source guards pass and census reports 269 writes / 307 field writes / zero unresolved owners.
- [x] Requalify paired main/release/current-candidate performance after final checker/API corrections: all 18 completed samples pass; candidate +2.57% time / +3.71% bytes / +1.80% allocations versus main. Preserve and report interruption after sample 14.
- [x] Preserve the uncached integrated `make test` pass: 114 packages, all 33 census mutants, staged source tree `8e63e98e56538c99d0de640c9dfa3ffdaa7b4480`.
- [x] Integrate the 13 follow-up checker-row corrections; retain original test assertions and runtime contracts. Matched adjacency bytes/allocations are unchanged (+2.28% idle / +2.16% pending median time); shared transport lifecycle preserves struct sizes and constructor allocations.
- [x] Reproduce the inherited whole-namespace DROP cleanup failure through native and managed public writes; preserve the four failing admission assertions.
- [x] Integrate failed-DROP namespace fencing and durable cleanup recovery in the current lifecycle owner; frozen combined normal/race proofs pass.
- [x] Reinitialize attached managed quota accounting after same-engine Restore; all eight public empty/nonempty quota cells pass normal/race through existing managers.
- [ ] Requalify concurrent UNIQUE performance after integrating the recovery corrections.
- [x] Preserve and reproduce the recovery-candidate full-suite failures: restore the original zero-allocation embedding proof and avoid the genuine develop fixture's persisted metadata slot; focused proofs pass with assertions/fixtures unchanged.
- [x] Join queued direct-write notifications before the model fixture compares quiescent delivery with stored state; preserve every original ordering assertion.
- [x] Integrate the 15 recovery-checker owner consolidations; combined focused normal/race and matched read/lifecycle/MERGE controls pass. Final integrated checker and full-suite qualification remain separate gates.
- [ ] Complete the unchanged duplicate checker on final integrated source and resolve every finding.
- [ ] Complete `make test`, including all 33 census mutants; fix every failure and rerun after source corrections.
- [ ] Commit and push the reviewable PR, retarget `release/v1.4.4`, and publish exact verification evidence.
- [ ] Resolve the inherited DROP-cleanup acceptance gap before reconciling the old parent dependency; update Linear readiness without claiming NOR-44/NOR-36 complete.

Only the named UniqueDisjointBatches performance scope moved from NOR-166/#1655. Its historical TrueNAS fixture is unavailable; the maintainer approved reproducible local best-effort allocation/ns evidence, not a claim that the historical percentages were reproduced.

`make test` is the sole full local verification gate by maintainer instruction. Focused races, source guards and profiling supply affected-contract evidence; broader checks remain for CI. Both integrated uncached runs (`GOFLAGS=-count=1 make test`) completed nonzero. The first failed storage, replication, server and observability. The second passed every package except storage, including all 33 census mutants; its three failures were ordinary manual-suppression contention, mixed client/stored-image sidecar semantics, and a WAL fixture affected by prior feature configuration. Corrections preserve the original assertions. A later uncached run passed 114 packages, including all 33 census mutants, on staged tree `8e63e98e56538c99d0de640c9dfa3ffdaa7b4480`. That pass predates the follow-up checker and recovery corrections; fresh final-source qualification remains required.

The unchanged checker passed the earlier staged port (250 functions), which does not qualify the later integrated changes. The first integrated run reached its supported 300-second search deadline and is incomplete. The supported 1200-second run completed, analyzed 692 functions and reported reuse/refactor findings; all are being corrected before retrying. The second completed run analyzed 770 functions and reported 18 remaining rows; the subsequent owner consolidations and approved Go API migrations address that population. A subsequent 908-function run reported 13 rows, addressed by the four reviewed follow-up patches. The unchanged checker then passed 914 functions on staged tree `f9a3dd7d490979c9aff4f5b385969a486d3050f7`. Recovery corrections remain off-tree and require fresh checker qualification after integration. Hooks and analyzer remain unchanged.

Evidence: `/Users/timothysweet/.local/share/nornicdb-pr-readiness/post1532`.
