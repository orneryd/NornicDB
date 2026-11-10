# Concurrent entity writer readiness (NOR-213)

- [x] Read Linear acceptance criteria, PR source and advisory inventory.
- [ ] Reproduce the original conflict on current release/v1.5.0.
- [ ] Review lock ownership, refreshed reads, commit validation and reservation lifetime.
- [ ] Resolve or evidence-disposition every duplicate advisory entry.
- [ ] Exercise both parsers and public Bolt/HTTP writer schedules.
- [ ] Rebase onto current release and verify integrated affected packages/races.
- [ ] Qualify allocation/throughput costs with matched workload and CPU profile.
- [ ] Refresh generated inventories; push candidate and pass exact-head full make-test CI.
- [ ] Update PR acceptance map and Linear; retire completed task-owned resources.
