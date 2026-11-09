# NOR-291: database-scoped accepted-command projections

- [x] Confirm current release base and original PR merge; preserve existing recovery semantics.
- [x] Reproduce same logical ID in two databases through public application and replication streaming.
- [x] Correct the canonical projection identity, including startup reconstruction.
- [x] Verify retry, prune/reopen, native failure, replica results, malformed history and concurrency controls.
- [x] Compare current-release and candidate throughput/allocations without concurrent local tests.
- [x] Reproduce and correct the same database-scope collision in pending WAL receipts.
- [ ] Independently review reuse and CI duplicate findings; record every disposition.
- [x] Push release/v1.4.4 PR #1754 with explicit Linear acceptance mapping.
- [ ] Pass full make-test CI on the final head; update Linear and mark ready.
- [ ] Clean task-owned temporary resources while preserving active work and evidence.
