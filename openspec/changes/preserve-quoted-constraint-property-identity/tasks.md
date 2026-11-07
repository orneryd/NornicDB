# Quoted constraint property identity (NOR-46)

- [x] Read reopened Linear scope and branch from current release c79157537.
- [x] Reproduce quoted dotted-property declaration/enforcement through both public parser paths.
- [x] Reuse the existing quote-aware property reader in constraint admission, retaining exact decoded identity.
- [x] Verify ordinary and escaped-backtick property controls and rejected duplicate atomicity.
- [x] Affected constraint/schema race checks pass (76.212s); expanded public controls pass in three race runs (13.754s), including parenthesized and escaped-dot names and rejected backfill cleanup.
- [x] Lexical/source audit passes with the data record updated to the actual shared-reader call shape; rules unchanged.
- [ ] Complete independent source review and current-head duplicate advisory dispositions.
- [ ] Push a separate release/v1.4.4 PR with an explicit AC map.
- [ ] Pass exact-head full make test CI and resolve discovered failures.
- [ ] Preserve required evidence and clean task-owned artifacts before readiness.
