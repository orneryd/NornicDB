# Quoted constraint property identity (NOR-46)

- [x] Read reopened Linear scope and branch from current release c79157537.
- [x] Reproduce quoted dotted-property declaration/enforcement through both public parser paths.
- [x] Reuse the existing quote-aware property reader in constraint admission, retaining exact decoded identity.
- [x] Verify ordinary and escaped-backtick property controls and rejected duplicate atomicity.
- [x] Affected constraint/schema race checks pass (76.212s); expanded public controls pass in three race runs (13.754s), including parenthesized and escaped-dot names and rejected backfill cleanup.
- [x] Lexical/source audit passes with the data record updated to the actual shared-reader call shape; rules unchanged.
- [x] Resolve the initial independent review: retain decoded FOR qualifiers, fix quoted composite commas, add successful backfill and grouping/punctuation controls; prove block routing and parser harness populations.
- [x] Correct exposed projected-read data loss by retaining separate variable/property identities in the existing semantic walker. Retire the flattened collector and migrate every caller; node/relationship scan/index public controls pass.
- [x] Expanded final focused race matrix passes twice (55.623s), without skips. The broad affected sweep had 5,826 passing tests and two legacy-diagnostic failures; both are corrected without weakening assertions and pass three race repetitions. One Neo4j-reference metadata oracle was skipped because no reference server is attached.
- [x] CPU-profile four correct-on-both-baselines selector workloads: all reduce time, bytes and allocations (five 100ms samples each). Ordinary selection: 586.4->507.8 ns/op, 80->32 B/op, 4->1 allocations.
- [ ] Recheck the final integrated source independently and resolve current-head duplicate advisory entries.
- [ ] Complete the blocking context census on final source.
- [x] Open separate draft #1750 against release/v1.4.4 with an explicit AC map.
- [ ] Pass exact-head full make test CI and resolve discovered failures.
- [ ] Preserve required evidence and clean task-owned artifacts before readiness.
