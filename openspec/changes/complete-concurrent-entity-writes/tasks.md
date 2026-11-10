# Concurrent entity writer readiness (NOR-213)

- [x] Read Linear acceptance criteria, PR source and advisory inventory.
- [x] Reproduce the original conflict on current release/v1.5.0.
- [x] Review lock ownership, refreshed reads, commit validation and reservation lifetime.
- [x] Resolve or evidence-disposition every entry in the original advisory inventory.
- [x] Exercise both parsers and public Bolt/HTTP writer schedules.
- [x] Rebase onto current release and verify integrated affected packages/races.
- [x] Qualify allocation/throughput costs with matched workload and CPU profile.
- [x] Refresh generated inventories and pass the complete strict census package.
- [ ] Push candidate, review latest-head advisory, and pass exact-head full native make-test CI.
- [ ] Update PR acceptance map and Linear; retire completed task-owned resources.
