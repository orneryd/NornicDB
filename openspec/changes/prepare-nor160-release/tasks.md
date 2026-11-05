# PR #1688 / NOR-160 release readiness

Current release baseline: `b098f7e708b4c701a743ffb0310a9b179a84c8f2`.
Original own layer: `e1c1aa6f7de05d93c1dd2564500d1df02b77ebbc` over
`de70c0e8375e9c74800d5ab6d7da5a6505650cb8`.
Reference: `refs/codex/reference/pr-1688-20261104`.
Target: `release/v1.4.4`, rebase/port current owners; no merge commits.

Linear is the release-scope authority. NOR-160 is Urgent in v1.4.4 and assigned
to Tim Sweet. Older open PR primary issues checked during selection are now in
v1.4.5. NOR-306/307/308/309/381 remain separate; this unit is transaction read
reuse and storage delivery, with no migration or persisted-format change.

- [x] Fetch release, preserve original head/own-layer parent, isolate worktree.
- [x] Read repository/CONTRIBUTING workflow, current Linear AC and PR scope.
- [x] Assign NOR-160 to Tim and record takeover without claiming acceptance.
- [x] Reproduce current-release public storage mutation/type/vector defects; if already fixed, retain a meaningful regression without unnecessary production edits.
- [x] Reuse/port existing original/bound Bolt harness and deterministic fixture; do not invent a second workload.
- [x] Profile current-release primed explicit and autocommit relationship reads before proposing performance changes.
- [x] Correct only demonstrated defects in canonical current owners; preserve refactored APIs and bounded lifetime/selection behavior.
- [x] AC1: both parsers, original and bound forms, fully consumed equal row multisets. Full generated shape: 70,802 nodes, 96,160 relationships, 5,101 inputs, 1,390 matches; private saved graph unavailable, do not imply private replay.
- [x] AC2: paired time/Bop/alloc costs for primed reads and affected native selection/delivery. Use current release as primary baseline, preserve de70 historical comparison where reproducible; do not carry old-head gains forward. CPU profiles required; isolate timing from #1690 validation/other Go work.
- [x] AC3: unchanged failing-then-passing realistic supported-type/vector mutation probes, plus snapshot/overlay/live visibility/write/rollback controls. Distinguish an old proposed CallerCopy API from an existing supported contract before treating dispatch as a defect.
- [ ] AC4: focused/race and affected-package checks on current integrated source, with failures/skips/cache use reported. Local release gate is make test; broader verify remains CI per operator policy.
- [ ] Run unchanged remote duplicate analysis; disposition each finding, consolidate actionable overlap, no checker/hook edits.
- [ ] Update PR body with every Linear AC and exact evidence using write-pr-description; update Linear with actual verification/status using write-linear-ticket when rewriting its description.
- [ ] Commit/push verified candidate for review; do not merge without the operator's instruction.
- [ ] Cleanup task-owned completed resources via cleanup-pr-workspace, preserving active #1690 and other work; do not clear shared Go cache during active tests.

DeepSeek bounded reproduction: `d2017a90cc604bcc837fcd303922a08f` owns only two
ported public storage tests and sig/readiness-nor160 evidence; no production edits.
Read-only harness/current-owner research: `7550bcb2ae1d48d1bc248fa9a46932e8`.
Parent owns architecture, integration and independent acceptance.

## Added release regressions (maintainer requested)

- [ ] NOR-211: reproduce system-database EXPLAIN CREATE DATABASE and CREATE COMPOSITE DATABASE; establish Neo4j behavior, correct both parser/public paths, assert catalog remains unchanged.
- [ ] NOR-300 bounded case: reproduce CREATE INDEX ... OPTIONS without a map; require matching syntax-error rejection and no schema mutation, plus valid map/omitted OPTIONS controls. Broader NOR-300 work stays separate.
- [x] Preserve fail-first/current-correction evidence and map both cases in PR acceptance criteria.

Do not run benchmarks or profiles while #1690 make test is active. Begin added runtime qualification after that verification completes.

## Integrated corrections and review

- [x] Existing bounded snapshot cache admits complete repeated point reads; one-off reads avoid new cache-copy allocations. Projection/sidecar/visibility/own-write controls retained.
- [x] One generic structural copy owner preserves supported Go types, custom caller-copy dispatch and reference cycles; durable normalization remains separate and unchanged.
- [x] Retire copyNodeForCaller and migrate canonical callers; remove redundant copy-policy branches/arguments.
- [x] Namespace relationship projection reuses already-owned property maps; native buffered between reads now detach complete images, while header reads omit retained properties.
- [x] Reproduce acknowledged buffered property mutation (7 becomes 99) before fixes; pass race controls, including explicit synchronous drain with the existing manual-replay fixture.
- [x] NOR-211 standard/composite/replacement planning and unchanged catalog: isolated parsers and persistent public HTTP tests. Reuse existing catalog fixtures and one fixed-command metadata owner. Result columns remain caller-owned.
- [x] NOR-300 index and constraint bare OPTIONS: one shared tail validator, both-parser negatives/no-schema-mutation and positive controls; parameter admission ordering remains in the broader NOR-300 scope.
- [x] Native focused/race checks for affected storage and Cypher cases pass. DeepSeek read-only audit failed with RemoteDisconnected/JSONDecodeError; parent completed ownership/caller review, not a worker approval.
- [x] Final paired benchmark qualification and final profile, with no concurrent tests/profiling.
- [ ] Fresh unchanged-checker analysis using authenticated remote HTTPS; credential stays outside Git and worker context. Graph revision must be reported and matches verified in current source.
- [ ] Freeze candidate; full make test and exact source receipt; push only after the gate passes.

## Fresh inventory convergence

- [x] Unchanged remote analyzer completed on staged tree `7bea3ecd0440a45d5fb97c68cf59ce95ba6edfd3`: 131 functions, 11 reuse candidates, zero cap skips. Remote baseline remains stale `35e3bddbf`; verify current source independently.
- [x] Combine NOT IN/list comma and projection alias/modifier tests into named case tables without losing query literals or status assertions.
- [x] Share malformed MATCH admission/fixture execution while retaining syntax-only versus semantic-detail assertions.
- [x] Combine index-drop commit/rollback lifecycle across both parsers and procedures; share rejected constraint-transaction lifecycle without weakening publication assertions.
- [x] Retire both Bolt constructor variants in favor of one typed-mode owner; migrate all canonical callers. Failed worker output independently reviewed; integrated tests remain the parent's acceptance.
- [x] Reuse the existing cleanup-owned executor fixture for procedure entry tests; preserve distinct nonempty SET/required-WITH side-effect and empty-input metadata/error contracts.
- [x] Final integrated convergence race checks and final matched cost run after all workers stop.
- [ ] NOR-211 Enterprise 5.26.30 reference confirms plan/no rows/no columns/no mutation; resolve existing NornicDB name-column contract with maintainer before claiming exact metadata parity.
