# NOR-25 / PR1583 readiness

- [x] Fetch release 29ecd7909 and preserve reference fa1509f2d.
- [x] Assign NOR-25 to Tim and move to selected v1.4.4 release.
- [x] Reproduce original locality/import/wildcard cases and consolidated NOR-152/NOR-305 on release.
- [x] Reconcile #1640 dependency with current compiled binding owners; retain current compiler, not the historical stack.
- [ ] Correct proven defects in canonical owners; retain only necessary historical fixes.
- [ ] Qualify public Execute/Bolt/HTTP/Fabric paths, headers, empty aggregates and invalid-scope controls.
- [ ] Independently review integrated source and duplicate dispositions.
- [ ] Profile representative workloads against immutable release baseline after local tests finish.
- [ ] Push updated candidate, update outcome-focused PR/Linear AC, pass full test CI.
- [ ] Cleanup task-owned workers/temp/binaries/cache; preserve other work.

## Latest integrated validation

- Original release baseline: 29ecd7909; fetched release now f45b1a51a (#1663). Rebased onto f45b1a51a before candidate 4c575471a.
- Compilation of all packages passed before the latest scope-boundary corrections; zero runtime tests executed in that command.
- Public transition checks previously passed 443 cases across Execute/Bolt/HTTP on both parsers; GraphQL qualified default parser only.
- Weak implicit input identity correction passed all 287 live reference rule cases on both parsers.
- Full generated reference replay passed 6,066 cases / 24,264 comparisons per parser before the final generated-selector correction. That earlier result is not a full qualification of the current local edits.
- Candidate 4c575471a is pushed; full CI 42330766946 passed storage, storagecensus, capturecensus and lint, and failed cypher, server, command and other shards. Corrections are local until independently checked and pushed. No current candidate full-suite pass.

## CI correction checklist

- [x] Integrate remote rollback fixture correction; public command test passes on integrated source (62.007 s).
- [x] Preserve grouped binding identity and original expression column spelling; focused CALL/quoted identity tests pass (3.347 s).
- [x] Preserve DISTINCT/function-form diagnostics; focused rejection tests pass (2.307 s).
- [x] Use shared benchmark storage fixture owner.
- [x] Verify Bolt identity corpus and fixture ownership guard on integrated source.
- [ ] Integrate and independently verify Fabric participant admission correction.
- [x] Separate execution admission orchestration from result metadata module and verify boundary guard (2.594 s, including lexical population and public undefined-variable controls).
- [x] Run remaining failed lexical corpus and focused affected groups (85.353 s); complete affected-package/full-suite qualification still pending.
- [ ] Push all verified CI corrections and dispatch full sharded make test on the immutable new head.
