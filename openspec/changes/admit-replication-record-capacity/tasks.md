# Admit replication record capacity before acceptance

- [x] Reproduce oversized HA Bolt auto-commit and explicit-commit unknown outcomes on release6a29b276d.
- [x] Reuse the physical WAL size predicate for conservative metadata-capacity admission.
- [x] Measure replication JSON/base64 sizes without copying retained command bytes.
- [x] Route canonical Standalone/HA and Raft submissions through admission before queue ownership.
- [x] Verify native-fitting/replication-oversized refusal, clean close and reopen through public Bolt.
- [x] Verify exact JSON size, growing metadata boundary, standard/large policy and Raft queue controls.
- [x] Independently review native/replication capacity and lifecycle boundaries; add native-capacity ceiling test using the complete 65000-byte physical-key namespace bound rather than assuming small names.
- [x] Pass fitting-command zero-allocation, genuine accepted uncertainty, quorum and focused race controls (replication8.386s/storage3.699s; public Bolt race3 repeats17.801s).
- [ ] Publish candidate, dispatch exact-head CI and disposition duplicate findings.
