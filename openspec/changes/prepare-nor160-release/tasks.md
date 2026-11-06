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
- [x] Run unchanged remote duplicate analysis; disposition each finding, consolidate actionable overlap, no checker/hook edits. Graph baseline is stale; current-source review is recorded separately.
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

## Post-gate corrections

The frozen `b0c64a14904ba32b9ace7ac76f462dc6ddeb020f` full `make test`
completed in 4,372.6 seconds with exit 2: 143 packages passed; five tests in
four packages failed. Source fingerprints were unchanged during the run. This
is a failed gate, not a pass or an interrupted run.

- [x] Integrate endpoint capability visibility correction with point-read agreement and both-parser traversal controls. Native focused tests pass.
- [x] Replace nondeterministic buffered-replay test scheduling with the existing manual-drain fixture; distinguish the actual replay and outcome callbacks, retaining the outer-lease assertions. Native integrated focused tests pass.
- [x] Integrate and independently verify the context-initializer root correction: native TestNilResultCorrelationRealSources and TestStorageNilContextCensus pass (zero nil flows), without changing census rules or admitting nil contexts.
- [x] Integrate schema OPTIONS binding/trivia correction; native focused both-parser cases pass. Review four new lexical members and two retired scanners, move the retired minimum scanner to its canonical replacement; strict 3,971-member population passes.
- [x] Remove the measured extra 528 bytes/two allocations in shallow nested copies, preserving types, cycles and repeated-reference ownership. Native integrated copy tests pass; final isolated timing qualification remains below.
- [ ] Regenerate and review the stored-entity source inventory after final source integration; keep ordering assertions blocking.
- [ ] Repeat integrated runtime/race and isolated cost checks on the corrected source, then run the full frozen native make test again.

The strict stored-entity check remains **failing**, not waived. A new per-copy
cursor is now explicitly classified as traversal progress (not persisted or
engine-retained state). The other finding follows schema OPTIONS binding from
`executeCreateConstraint` into the general expression executor. DeepSeek job
`2a498cee093b46e8877803b50e584847` is establishing the exact SSA condition and
correcting any mixed parse/execution boundary in the existing contract owner;
it may not edit census rules. Regenerate/review the inventory only after this
source correction is integrated. The current generated inventory is an
intermediate diagnostic containing the failures, not a readiness artifact.

## Maintainer verification-policy update (2026-11-05)

Candidates may now be committed and pushed for review before a local full gate.
Run the exact `make test` command and advisory duplicate analysis in GitHub CI;
remove the installed Codex Git hooks locally. This supersedes the earlier
local-test-before-push instructions above, not the obligation to fix regressions
before readiness. No full pass is claimed for this candidate until CI reports
its exact checked head. The previous failed b0c gate remains preserved.

The maintainer approved zero-column/zero-row results for both CREATE DATABASE
and EXPLAIN CREATE DATABASE. DeepSeek job
`f69a0ba6d4d643598f7c740e339f96ef` is implementing that result contract in an
isolated worktree; this review checkpoint precedes its integration.

- [x] Remove local Codex-installed Git hooks and hooksPath, without changing tracked hook/checker sources.
- [x] Add an admin-gated exact make-test dispatch mode to the existing CI workflow.
- [x] Rebase review checkpoint onto the merged NOR-136 release head, preserving both corrections.
- [x] Push candidate; dispatch exact-SHA make test and authorize the advisory PR Graphify check.
- [x] Integrate approved zero-column/zero-record database-command output alignment; compile affected packages. Runtime verification is assigned to CI; replace superseded requests for this follow-up head.
- [ ] Report actual CI outcomes and remaining AC without claiming pending checks pass.

The approved CREATE/EXPLAIN output correction is integrated in the normal,
replacement, IF NOT EXISTS and supported composite owners. ALTER/SHOW output is
unchanged. Typed AdministrationOutcome still carries newly created identities
for access grants. Existing consumers/tests are migrated; affected package/test
binaries compile (`go test ./pkg/cypher ./pkg/server -run '^$'`: no tests run).
The worker's full local Cypher run was canceled when verification moved to CI;
it is incomplete and is not counted as a pass. The latest release inventory is
regenerated with zero violations and no lexical membership changes. The first
review CI request is superseded by the following pushed head.

## Release CI failure correction (2026-11-05)

- [x] Inspect release verification 42009807718 and PR CI 42011914534: NOR-136 acknowledged increments fail with write-behind enabled.
- [x] Preserve a deterministic failing reserved-version/predecessor interleaving without adding production test hooks.
- [x] Extend the canonical batch mutation-preimage validator; preserve uniqueness error classification, first-mutation folding and storage formats.
- [x] Add implicit/native, buffered/replayed and healthy retry controls; preserve unchanged NaN payloads through the existing stored-value comparator.
- [x] Finish the repeated focused checks on the integrated candidate: five repetitions passed (39.530s).
- [x] Push the reserved-version correction (38bd71f65), dispatch exact-head make test CI and request advisory duplicate analysis; that test run terminated and is not a pass.
- [ ] Report actual final CI outcomes; no pending, interrupted or skipped gate is a pass.

## Complete advisory inventory dispositions

- [x] Recover the untruncated 38bd71f65 CI inventory: 126 findings / 518 matched pairs, 98 current-source ownership records, one AST owner call; no cap skips.
- [x] Give every entry an explicit source-backed retention or corrective action in docs/development/pr1688-duplicate-dispositions.md; do not waive generated or historical matches without ownership review.
- [x] Retire property forwarding aliases and inert overlay code; converge endpoint assertions using the existing owner.
- [x] Converge fixed-command metadata construction while preserving command-specific scope transfer.
- [x] Integrate and independently validate schema and Bolt convergence, including every retained scenario population; final schema tests cover both parsers, public admission/no-mutation and private error contracts.
- [x] Preserve all 225 advisory records, including 518 matched pairs, in the disposition ledger. Add CI resource/log retention without changing the test gate.
- [x] Push the original full disposition ledger and corrections (2444cb481); update PR/Linear evidence and request corrected-head CI.
- [ ] Evaluate actual full CI completion; exit 143 and skipped artifact upload are incomplete evidence, not a passing gate.

## Frozen delivered-head advisory follow-up

- [x] Expand the 2444cb481 advisory: account for all 129 recovered candidates / 515 pairs, 133 ownership records and one AST call (263 records); disclose two extra live-requery matches and stale baseline.
- [x] Converge factory/raw database-pinning assertions, timeout barriers, quarantine probes, relationship workload seed/iteration owners, DB-manager session setup and constraint matrices without losing scenario populations.
- [x] Share the zero-row aggregation seed/params/oracle across direct Cypher and Bolt public paths; retain both boundaries and all eight cases each.
- [x] Independently validate the integrated follow-up under race (Bolt 22.412s; Cypher 13.923s) and normal lexical population (2.151s).
- [x] Preserve raw Go diagnostics alongside resource samples without changing make-test flags/assertions.
- [ ] Push the follow-up and report its exact-head CI result; do not call an in-progress or superseded run a pass.

## Full-suite cancellation admission correction

- [x] Preserve the c7cbdf481 CI failure and local red reproduction: the old plain-commit probe expected conflict scanning before the new node-preimage read.
- [x] Cover prepared receipt, plain update-preimage and plain create/conflict admissions in one matrix; preserve cancellation/no-publication/peer/receipt/healthy-retry assertions and add same-identity CREATE retry.
- [x] Independently verify five normal repetitions (2.098s) and three integrated race repetitions (64.437s), retaining the acknowledged-increment correction.
- [ ] Collect the complete c7cbdf481 CI result, then run the exact full make-test gate on the correction; no superseded or failed run is a pass.

## Completed c7 full-suite audit correction

- [x] Preserve full CI 42070124669 raw/resource logs: storage cancellation probe, stale census location, suppression-owner assertion and 30-minute storagecensus package timeout; this is a failed gate.
- [x] Regenerate the unchanged census: only embedding-sidecar source location changes from line 206 to 207; no rule, ownership or write population change.
- [x] Check suppression retirement at its current storage decision owner and assert the injected edge-body write is analyzed, rather than expecting the retired Cypher caller. Integrated census freshness/order/bypass checks pass (43.302s).
- [x] Investigate repeated audit work with DeepSeek job 38489709d13543ca9d3d999288939eab; cancel experimental route after measurements in favor of independent test-package partitioning, with no experimental analyzer changes integrated.
- [x] Move the complete standalone mutant matrix to testing/storagecensusmutants, retaining inputs/edits/expectations byte-for-byte, using exported Census and slices.Contains. Both packages remain under unchanged make test and its original 30-minute per-package timeout.
- [ ] Validate the complete moved mutant matrix and remaining audit package; replacement CI must pass both.
- [ ] Push integrated audit corrections and obtain a complete exact-head make-test CI pass.

## Parallel CI and mixed-mode progress correction

- [x] Dispatch parallel workflow run 42119526395 from main for immutable PR head b8a83d72; confirm its actual target checkout in plan and shard logs.
- [x] Preserve the Cypher source-audit failure caused by scanning the auxiliary workflow checkout; keep real-source duplicate rejection controls intact.
- [x] Reproduce the mixed-mode progress defect on unchanged b8a83d72: 26/30 two-core six-mode runs exhaust the unchanged retry budget.
- [x] Trace an accepted physical conflict after empty-BEGIN buffer replay (epochs 134→140); retain all genuine snapshot/preimage conflict checks.
- [x] Evaluate the broader replay-detector experiment: increment controls passed, but CI exposed incorrect cascade counters. Withdraw the experiment and retain the canonical pre-acceptance ordering correction.
- [x] Minimize CI counter seeds 68/73 to ten/eleven steps; replace the implementation-only detector assertion with two outcome-based schedules. They fail with the withdrawn experiment and pass with the retained ordering correction.
- [x] Integrate DeepSeek's existing direct-write ordering phase before accepted preparation, without introducing another owner or fallback.
- [x] Preserve earlier candidate focused/race evidence as historical, not final-combination qualification.
- [x] Push candidate 6fd44b5d and dispatch parallel run 42123957134; it fails counters and both audits, not a passing gate.
- [ ] Collect a complete passing exact-head CI result, update PR/Linear and clean completed task resources.

## Current CI fallout correction

- [x] Reproduce 6fd counter failures locally; preserve the original and minimized traces.
- [x] Restore the original replay criteria, retain pre-WAL ordering, and pass the full generated model/named schedules in both modes (72.695s).
- [x] Reproduce the write-order census failure (22.454s); prove repeated immutable-boolean control flow without function exemptions and retain all negative controls.
- [x] Reuse the existing stable-SSA-value owner; retire the unused older graph traversal instead of leaving competing proofs.
- [x] Pass the actual ordered-write census and regenerate only changed source positions/gate paths, retaining all field/decision records.
- [x] Refresh the two unchanged accepted-completion context offsets; full context audit and stale-scope controls pass.
- [x] Complete the full stored-write census (184.397s), final public/conflict/counter race controls (69.024s), and 30 repeated six-mode progress runs (144.715s).
- [x] Push integrated fallout correction d1771d5e and dispatch parallel CI 42128104230; its storage census passes. Preserve the late numeric-audit auxiliary-checkout failure from the preceding run.

## Shared source-audit checkout boundary

- [x] Preserve the late numeric-wire failure and reproduce it locally with an auxiliary checkout fixture.
- [x] Share repository-boundary traversal between numeric/scanner audits, preserve existing nested-module/experiment coverage, and retire the scanner-only CI directory-name exclusion.
- [x] Prove both audits pass for the foreign checkout and reject its same-path contents when owned by the current checkout; preserve frozen numeric ownership inventories.
- [x] Complete clean-tree audit and stored-census freshness checks (0.606s/0.642s/1.297s/11.317s); remove all temporary fixture files.
- [ ] Replace the superseded CI run with complete parallel CI on the final immutable head.

## bbd4208d parallel CI failure inventory

- [x] Preserve command and Cypher logs from run 42129453479 against bbd4208d92367cdac94734a936c0acba07cd2a43.
- [x] Identify command assertion: TestSolReviewRealBinaries/live-config-and-pprof did not observe a ready disposable CLI server within its readiness window; child output remains withheld to protect the secret probes.
- [x] Distinguish the Cypher runner shutdown/exit 143 from an asserted product failure: no test failure or OOM cause is established by that log.
- [x] Finish the remaining CI shard and record all additional failures before replacing this run: other passes, including storagecensusmutants (1496.164s). No additional named failures; command and the terminated Cypher shard are the complete failure inventory.
- [ ] Reproduce and correct the CLI startup cause without weakening secret-redaction assertions.
- [x] Independently pass integrated real-binary and occupied-port diagnostic tests (37.934s). Preserve the readiness budget and credential assertions; classify child exit/transport/HTTP status without exposing child logs. This diagnostic correction does not establish the original CI startup cause.
- [ ] Complete local Cypher checks and requalify the entire shard in CI; local tags and any cached results must be reported explicitly.
- [x] Complete all six local Cypher packages with `-tags noui,nolocalllm -p 1 -parallel 4 -timeout 30m` (exit 0): Cypher 463.060s, ANTLR 77.698s, fn 0.555s, internal/text 0.272s, resultmetadata 0.261s, testutil 0.728s. No package reported cached results. This does not establish the CI shutdown cause or replace native CI qualification.
- [ ] Push integrated corrections and obtain a complete exact-head make-test result.

## Complete 1f84a988 CI inventory

- [x] Collect complete run 42137139063: command and Cypher now pass; lint, storage, server and both censuses pass. The only failure is the 30-minute cumulative storagecensusmutants package timeout.
- [x] Preserve the active-case trace: unresolved_write_callback was running for 44 seconds when the package budget expired. No named assertion failure is reported. The prior full matrix passed in 1496.164s; that does not waive this timeout.
- [x] Refresh final-head NOR-211 public HTTP controls in separate native/ANTLR processes (1.593s/2.248s), without cache reuse.
- [x] Partition all 37 unchanged mutant cases into four bounded test packages with one canonical runner; preserve every input, expectation, short-mode condition and the original per-package timeout. AST source hashes match for the full case literal and per-case assertion callback. Partition control passes (0.322s), and all four entry packages compile; this is not full matrix execution.
- [ ] Verify exact matrix preservation, complete all partitions and blocking censuses, then push and obtain a full exact-head CI pass.

## Copilot: property-only preimage hydration

- [x] Confirm unresolved thread PRRT_kwDOQjyHhc60N0A3 against pushed 3cdd2049: write-behind full-property validation passes a nil projection into the full-embedding hydration path, although comparisons use only properties and labels.
- [x] Preserve a clean-baseline public commit failure on a malformed embedding chunk; full reads still reject it. Force physical predecessors and establish warm/cold cache states after fault-write invalidation; no production instrumentation is retained.
- [x] Use existing nodeItemBodyOnly for full-property validation and retain borrowed projected reads for constraint-only validation.
- [x] Verify final regressions, chunk persistence and cancellation under race (three repetitions, 8.064s), blocking stored/context censuses (19.956s/82.463s) and go vet. Reuse existing raw-value, write-behind, sidecar and vector-fixture owners; name tests by behavior.
- [x] Profile matched public-update benchmarks: large sidecars reduce median bytes from 3,564,119 to 2,548,002 and allocations from 842 to 682; scoped latency and small-payload results are recorded in the performance report.
- [ ] Push the correction and resolve the tested review thread.
- [ ] Complete full CI on the resulting head; earlier-head or incomplete runs are not final qualification.
