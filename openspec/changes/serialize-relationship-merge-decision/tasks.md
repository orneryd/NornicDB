# Serialize relationship MERGE decisions

- [x] Reproduce duplicate relationship/counter through public Bolt on release.
- [x] Lock bound endpoints in identity order before MERGE existence decision.
- [x] Verify unlocked, start-locked, keyed and incoming schedules on both parsers.
- [x] Preserve already-locked MERGE control.
- [ ] Extend concurrent opposite-direction/self-loop and rollback/deadlock controls.
- [ ] Resolve report2 absence-guard schedules independently; do not prohibit Neo4j-valid CREATE duplicates.
- [ ] Reproduce/disposition report9 unrelated uniqueness, report16 historical relationship view and report17 schema admission.
- [ ] Performance qualification of the changed lock boundary.
- [ ] Exact-head CI and combined release verification.
