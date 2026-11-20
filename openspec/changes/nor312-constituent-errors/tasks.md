# TODO

- [x] Worker reproduced 22 exact direct-storage error-returning failures on e8cd436e5; public Cypher controls passed.
- [x] Converged the composite first-match read, write, broadcast and sum walks into the shared owners (`readFirstConstituent`, `writeFirstConstituent`, `walkReadableConstituents`, `sumConstituentCounts`, `sumConstituentDegree`).
- [x] Kept every published identity and access contract of the converged routes: direct sentinel for ordinary node/edge reads and update/delete, `errors.Is` for MVCC providers and the projected tail, failing-source head payload beside its error, source order and qualified provenance, no same-engine plain fallback after a provider not-found, zero (not partial) sums, and the existing bulk-operation error contract.
- [x] Preserved the sidecar maintenance admission: `UpdateNodeEmbeddingSidecar` writes derived embedding state through the readable participant that holds the node, as on the base, instead of the entity writer admission. The unapproved writer-admission change in the staged convergence is removed, and `constituentWriteAdmission` keeps the two admissions in one owner.
- [x] Added `pkg/storage/composite_sidecar_maintenance_contract_test.go`: sidecar maintenance reaches a read-only constituent, a participant without the capability still reports the deterministic unsupported error, and `UpdateNode` still refuses a read-only constituent. Fail-first recorded: the staged convergence returned `constituent "read" is read-only` for the maintenance write.
- [ ] Retained no-error-channel APIs: eleven storage APIs keep their audited partial behavior. Not changed here; their signatures and approved contracts are outside this PR.
- [ ] Retained drain scope from `NOR312-DRAIN-REPRODUCTION.txt`: the fixture reproduces on the combined source (namespace/WAL/composite `CountPendingEmbeddingIDs` all report 0 while a readable namespace still holds a pending embedding behind an unrelated namespace-DROP fence), and still reports 1 before the failed DROP and after it is resumed. Not corrected here: the count API has no error channel and the DROP fence's blast radius is an operator decision.
- [ ] Run the unchanged canonical staged duplicate checker; inspect every candidate.
- [ ] Integrate the current `release/v1.5.2` head and verify the combined source.
- [ ] Publish a draft PR targeting `release/v1.5.2` with milestone `v1.5.2`. This PR does not close the umbrella ticket.
- [ ] Put exact sources, commands, results and unresolved no-error API scope inline in both existing Plane descriptions.
- [ ] Let CI run broader checks; no full local suite, race, stress or `make verify` requested.
