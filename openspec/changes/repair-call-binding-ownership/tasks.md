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
- [x] Integrate and independently verify Fabric participant admission correction (cypher 4.040 s; server 12.341 s); single explicit typed admission entry, no forwarding wrapper.
- [x] Separate execution admission orchestration from result metadata module and verify boundary guard (2.594 s, metadata boundary and public undefined-variable controls; lexical population belongs to internal/cyphertext and was not executed by that command).
- [x] Run remaining failed lexical corpus and focused affected groups (85.353 s); complete affected-package/full-suite qualification still pending.
- [ ] Push all verified CI corrections and dispatch full sharded make test on the immutable new head.

- Public quoted-column cache-order regression passes on both parsers (1.061 s), covering rejected and accepted statements in both cache orders.

## Second CI correction

- Run42336067612 on55ea092e9 passed command/server/storage/capturecensus/lint. Cypher failed only TestProjectionPassThroughUsesSyntax for WITH (x) AS x; other/storagecensus still pending at observation.
- Preserve grouped variable binding identity independently from the bare-projection PassThrough fact. Retained CST writes grouped once; metadata excludes grouped syntax without changing semantic identity.
- Focused grouped identity, quoted/cache spelling and pass-through checks pass (2.663 s). Final-head full CI still required; no whole-suite success claimed.
- Graphify advisory check completed on55ea092e9, indexed baseline35e3bddbf (stale vs releasef45b1a51a). Inventory has14 top-level candidate entries and110 compiler-owner pairs. Dedicated worker is reviewing all entries; no clean-duplicates claim.

- Corrected source-inventory ordering after statement_admission.go move; actual internal/cyphertext TestFoundationLexicalPopulation passes (2.634 s). No inventory entries or rules changed. Superseded run42338766887 canceled; incomplete, not passed.

## Additional duplicate-review corrections
- [x] Consolidate declared/runtime COLLECT column validation while preserving established diagnostic keys and status.
- [x] Retire private raw compilation/root and test-only RETURN forwarding functions; migrate direct callers.
- [x] Remove only the two inventory rows for retired symbols; unchanged lexical census passed (2.530s).
- [x] Focused COLLECT/public subquery/projection checks passed (1.290s); RETURN function controls passed (0.956s).
- [ ] Include these corrections in a fresh final-head CI qualification; current run targets earlier pushed 51dccbe5d.

## Final duplicate-review convergence
- [x] One parameterized benchmark registration replaces twelve forwarding entries and three shape runners; all six workload families and their warm/cold sizes remain. Public workload result controls pass (1.530s); all 22 benchmark variants execute successfully.
- [x] Both CREATE-only UNWIND gates share the unchanged top-level mutation restriction; all TestUnwind controls pass (5.835s), lexical census passes (1.895s).
- [x] Sequential baseline/current cost check: same harness on 4c575471 and this candidate, three 100ms samples for each of 22 variants. Median latency, bytes and allocations do not increase by >5% in any variant. This is a comparison to the earlier candidate, not a new release-base qualification or statistical significance claim.
- [x] CPU/allocation profiles captured during benchmark execution with no concurrent local Go tests. Inventory and every reported candidate receive a source-backed disposition, with omitted localization identities explicitly unverified.
- [ ] Publish final source and full disposition comment; qualify exact final head in sharded make test CI.
