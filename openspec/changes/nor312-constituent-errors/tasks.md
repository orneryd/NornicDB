# TODO

## PR review correction

- [x] Preserve fail-first evidence for the access-inventory classifier: 14 of 25 extractor cases fail on PR head c3bfc42f2 (including success guards, unrelated returned errors, renamed bindings and conditional return/continue).
- [x] Track the assigned error's AST binding, inspect failure paths and the actual error result, and leave unsupported control flow unclassified. Distinguish block fallthrough and conditional policies from unconditional propagation.
- [x] Re-extract all direct sites: storage 38 (31 propagate, 3 skip, 2 fallthrough, 2 conditional), Cypher 1 (propagate). Pin both conditional guards for policy review. This syntactic audit does not establish end-to-end correctness; retain the public-path regression tests and unresolved umbrella scope.
- [x] Correct point-read benchmark comments: map order makes unqualified work vary from one to N sources, so it is not a fixed worst-case fan-out baseline.
- [ ] Complete focused checks, canonical duplicate analysis, Go/security follow-up reviews and push the correction to PR #1869. Broader runtime qualification remains with CI.

- [x] Worker reproduced 22 exact direct-storage error-returning failures on e8cd436e5; public Cypher controls passed.
- [x] Converged the composite first-match read, write, broadcast and sum walks into the shared owners (`readFirstConstituent`, `writeFirstConstituent`, `walkReadableConstituents`, `sumConstituentCounts`, `sumConstituentDegree`), and folded the qualified selector's participant opener into the selector that resolves it.
- [x] Kept every published identity and access contract of the converged routes: direct sentinel for ordinary node/edge reads and update/delete, `errors.Is` for MVCC providers and the projected tail, failing-source head payload beside its error, source order and qualified provenance, no same-engine plain fallback after a provider not-found, zero (not partial) sums, and the existing bulk-operation error contract.
- [x] Preserved the sidecar maintenance admission: `UpdateNodeEmbeddingSidecar` writes derived embedding state through the readable participant that holds the node, as on the base, instead of the entity writer admission. The unapproved writer-admission change in the staged convergence is removed, and `constituentWriteAdmission` keeps the two admissions in one owner.
- [x] Added `pkg/storage/composite_sidecar_maintenance_contract_test.go`: sidecar maintenance reaches a read-only constituent, a participant without the capability still reports the deterministic unsupported error, and `UpdateNode` still refuses a read-only constituent. Fail-first recorded: the staged convergence returned `constituent "read" is read-only` for the maintenance write.
- [x] Ran the unchanged canonical staged duplicate checker: `exit 0`, "Analyzed 111 added or rewritten functions … No duplicates found", compared against the ingested baseline `5ca76aad3`. The earlier candidates (test fixture duplicate, the positional label-count adapter pair, the selector/opener pair) are converged; no threshold, receipt or checker edit was made.
- [x] Integrated `origin/release/v1.5.2` (`5ca76aad3`, including NOR-71/NOR-182/NOR-77/NOR-183) by a normal branch merge; the combined source builds and the focused storage, cypher and multidb checks pass on the merged head.
- [ ] Retained no-error-channel APIs: eleven storage APIs keep their audited partial behavior. Not changed here; their signatures and approved contracts are outside this PR.
- [ ] Retained drain scope from `NOR312-DRAIN-REPRODUCTION.txt`: the fixture reproduces on the combined source (namespace/WAL/composite `CountPendingEmbeddingIDs` all report 0 while a readable namespace still holds a pending embedding behind an unrelated namespace-DROP fence), and still reports 1 before the failed DROP and after it is resumed. Not corrected here: the count API has no error channel and the DROP fence's blast radius is an operator decision.
- [ ] Publish a draft PR targeting `release/v1.5.2` with milestone `v1.5.2`. This PR does not close the umbrella ticket.
- [ ] Put exact sources, commands, results and unresolved no-error API scope inline in both existing Plane descriptions.
- [ ] Let CI run broader checks; no full local suite, race, stress or `make verify` requested.
