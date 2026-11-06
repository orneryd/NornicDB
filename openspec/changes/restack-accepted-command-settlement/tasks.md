# NOR-288 / PR1691 current-release restack

Original head182893c0dd15db761ef604c7a6eefb285e4d7b64 is preserved in refs/codex/reference/pr-1691-182893c0 and the saved original patch. Original base4e382ba2a55203f645545222134d6f1ea0898063; current release69115e8a2139b744ad2c7c797240b9d1f9dafcdb. The initial one-commit rebase has72 conflicted paths; no side is selected mechanically.

- [x] Select oldest urgent open-PR item from current Linear release project; assign NOR288 to Tim.
- [x] Preserve original PR metadata, diff, original head and base.
- [x] Clean shared Go cache after confirming no active Go jobs; remove completed profile executables while retaining logs/profiles.
- [x] Create separate baseline-probe and rebase candidate worktrees; leave dogfood database untouched.
- [x] Reproduce accepted conflict and subsequent public write on unchanged release.
- [x] Reproduce delayed DROP retry against a recreated database.
- [ ] Qualify metadata/timestamp live/replay boundaries and old-wire contracts on current release.
- [ ] Qualify Raft pre-native acknowledgement failure and genuine pre-acceptance conflicts.
- [ ] Qualify pending DROP consumers: later DROP, CREATE/replace, Qdrant delete, final-sync failure and stopped waiter.
- [x] Resolve rebase conflicts by adapting required fixes to current canonical owners; retire obsolete API edits already covered by release with explicit mapping.
- [ ] Cover quota accounting including preceding async fixtures; never mask with a larger wait.
- [ ] Run focused affected/public/race checks and paired cost qualification without local test contention.
- [ ] Review duplicates and independent findings with per-entry dispositions; keep checker/hooks unchanged.
- [ ] Update PR description/Linear AC outcomes, target release/v1.4.4, push candidate and pass exact-head CI make-test gate.
- [ ] Cleanup task-owned resources after readiness, preserving references/evidence and other active work.

Delegation: DeepSeek84714943751d4b57b7b0dac310cdd703 owns only public accepted-conflict test;3185553b47ab496eb2ab04a77b3b7c65 owns only codec boundary test;ce5f9f4806eb430183ed28f55b1c0ac5 owns only DROP retry test. All operate on baseline production, not the conflicted candidate. Parent owns architecture, integration and acceptance.

Current-release reproduction outcomes: accepted native conflict leaves its image unapplied and authority retained; delayed DROP is incorrectly retryable and deletes the replacement generation. Wire probes reproduce nil EmbedMeta collapse and Unix-epoch loss; unset timestamps and literal version-two decode controls already pass on release. A maintenance-after-Close probe also reproduces a nil database panic in PruneMVCCVersions. These are fail-first observations, not qualification of the ported candidate.

AcceptedCommitError Go API decision remains pending: retire Unwrap and use the existing typed Err field, without adding a Cause forwarding getter. No decision is inferred from continued independent work.

Integrated progress (working candidate, not a qualified commit): all72 original conflict paths resolved; production storage/replication/multidb/server/cypher packages compile. The independent accepted-conflict recheck passes; the ported original public three-mutation test passes including cold replay and quota image/count comparison. Selected multidb DROP/accepted cases pass (3.815s). Replication accepted/Raft-apply cases pass after phase-aware assertion migration (3.233s). Storage boundary rechecks pass (1.917s); broader storage checks exposed and fixed a duplicate-replay admission conflict and obsolete nil-context fixture calls. Their next integrated rerun is pending. Full affected packages, race/cost qualification, original caveat reconciliation, API decision, PR retarget/push and CI remain incomplete.

Original synchronous replication-WAL overflow fault injection is retired: the current FIFO adapter has no such fallback. Its accepted pre-native transport fault scenarios now run through a test Storage implementation that performs a real WAL append+Sync before returning the acknowledgement failure, without changing the production FIFO owner. This qualifies the transport settlement contract, not a claim that the current adapter can fail at that retired phase.

Current release's direct commit ordering already validates endpoints before preparation under retained exclusive stripes. Reacquiring those stripes from the old PR would deadlock; the additional mixed-stripe pre-decision check is restricted to buffered admission. Accepted replay bypasses the newer client body-admission check so concurrent copies coalesce on one accepted logical owner; ordinary client admission retains the check. The duplicated-replay regression failed before this correction and passes after it.

Latest focused storage rerun passed: `go test -tags noui,nolocalllm ./pkg/storage -run 'Accepted|SemanticTransaction|MVCCPruningRejects|WALStatement|Prepared.*Endpoint' -v -count=1 -timeout=180s` (10.317s). It includes genuine pre-acceptance endpoint conflict controls, accepted duplicate replay, native durability modes, replay after Close, serialized recovery, and captured wire vectors. The test-name-only removal of the old PR1655 prefix happened during compilation; runtime behavior and assertions were unchanged, but final frozen-source gates still need a fresh run.

Latest release is now60ff9d644 (merged PR1738). The clean reproduction worktree was rebased to it, and fresh unchanged-release runs still fail accepted conflict settlement, delayed DROP retry, nil metadata, epoch encode/replay and pruning after Close. Original zero/normal control observations remain explicit. The candidate still needs its final rebase onto60ff9d644 after the original conflict port.

The actual replication-WAL hop exposed an additional AC gap: native compact relationship bodies mapped an unset instant to1754. Replaced the hop-neutral waiver with absolute unset assertions; they fail before the narrow compact-header correction. The correction uses reserved V2 header bits2/3 with unchanged offsets/lengths/raw timestamp bytes and unchanged legacy V1 behavior. Pinned legacy vectors and native close/reopen/backup/restore controls pass; an actual database written by unchanged release60ff9d644 opens under the candidate with its 2 nodes/5 historical edges and exact timestamp meanings intact. No old records are rewritten, no new storage keys or migration arms are introduced, and no lost historical timestamp is inferred. New semantics require updated readers; old-binary downgrade is not supported. These limits must remain on the PR.

Independent baseline probe fixtures are preserved under external `nor288-current/release-red-reproductions`. The candidate uses the original canonical regressions instead of shipping copied server/DROP/codec fixture owners: the public conflict matrix gained its no-conflict control, and the existing DROP fixture gained normal-drop, exact replacement data/count and predecessor-removal controls. The new pruning-after-Close regression remains in `badger_mvcc_prune_lifetime_test.go`.

Checkpoint update: candidate commit88b8fe766 rebased cleanly onto release60ff9d644. All eight affected-package focused checks passed on that commit. The subsequent Err-only API proposal has been implemented by two DeepSeek lanes but remains a pending compatibility decision, not an approved contract. Integrated tests are running on those working-tree changes. A third DeepSeek lane supplied the shared public managed Cypher cost harness; it has not been benchmarked yet.

The fresh release60ff Raft pre-native fault reproduction fails because its returned error does not retain an AcceptedCommitError boundary. Source and log remain in the external release-red-reproductions evidence; the candidate equivalent is included in focused replication tests.

Err-field proposal integrated focused run passed: storage, replication, multidb, Cypher, server, Qdrant, Bolt, errors and northwind_power; storagefault compiled but selected no tests. Log: external err-field-integrated-focused.log. This is focused validation, not full make test. API compatibility approval, paired cost qualification and full CI remain pending.

## Release and CI failure correction

- [x] Capture full failed release verification42175360279 logs and per-job outcomes.
- [x] Identify release storage regression: TestWriteOrderModel/writeBehind=true seed19 fails rejected backup restore with derived count underflow for test:LINK. No assertion waiver.
- [x] Identify runner shutdowns in release Cypher/conformance jobs (exit143); no assertion failure established by their incomplete logs.
- [x] Correct parent CI dispatch: c40d10504 was incorrectly treated as a branch by checkout; restart42219227768 with full c40d10504f21392188c6e3a48f3ed30b5e6860d7. Checkout/planning now passes, shards running. No workflow relaxation.
- [x] DeepSeek604db97c460749f080eec7ee54bfc73f supplies the count/restore correction; parent independently reproduces the exact underflow and verifies the candidate.
- [x] Independently review integrated storage correction and run focused affected checks, including three combined race repetitions.
- [ ] Push correction, update AC evidence and rerun exact-head CI.

Remote Graphify lookup still returns HTTP400 plain HTTP sent to HTTPS; source inspection is the fallback, not a clean duplicate result.

Current CI42219227768 checked out the full immutable c40d10504f21392188c6e3a48f3ed30b5e6860d7 (verified checkout log), passed planning/lint and exposed two additional failing shards:
- stored-entity census: retired field rows plus missing pendingTop writer classification and stale generated inventory; DeepSeekfc0008033b0a418682fbfcadc17f85db owns source-verified reconciliation, not rule changes.
- capture census: changed compensation closure proof, obsolete cross-unit acceptedEntityCommand.apply row, and changed calculateStorageSizeFromEngine reconciliation source; DeepSeek779b5b1696394912bf0d69126162d6ba owns source comparison and controls. No human approval is inferred from a digest refresh.

Independent unchanged-release60ff reproduction attempts: seed19/writeBehind=true count40 passed; three complete TestWriteOrderModel runs passed in153.039s. The CI failure remains established but not reproduced under these local schedules. Preserve this distinction until deterministic ownership is proven.

## Integrated correction progress

- [x] Deterministic unchanged-release reproduction of stale cascade ACK and exact LINK count underflow on Backup/Restore; candidate backup probe passes three repeats and conflict regressions pass five parent race repetitions.
- [x] Preserve both observed-update preparation contracts: caller cancellation and Close cancellation/join before acceptance. The original Close assertion remains. A deterministic automatic-snapshot admission deadlock was reproduced and corrected with the existing cancellable mutex API; combined race checks pass.
- [x] Fix real standalone replication caller-context loss before leader normalization; accepted submission still uses its completion owner. Adapt HA deadline diagnostics to the opaque accepted-error boundary with the existing typed diagnostic assertion. Parent replication race checks pass.
- [x] Review six moved/changed catalog error exits and five new DROP lexical-population members plus the moved DropDatabase classification; unchanged structural/credential output checks pass.
- [x] Register all52 missing fixture constructor sites with canonical ownership APIs, preserving explicit close/reopen/callback phases. Strong typed fixture inventory and allocator mutation controls pass after building UI assets for its untagged go-list child (72.573s).
- [x] Complete runtime engine-phase context threading and source-verified stale record retirement; preserve the two previously approved independent publication-completion boundaries. Full capturecensus package passes on the integrated source (251.810s), without census rule or assertion changes.
- [x] Independently inspect cascade validation-boundary coverage: retained entity-authority locks prevent the alleged overlap; deterministic public native-commit ordering controls pass under race.
- [ ] Freeze source, regenerate stored-entity inventory and reverify census controls; then commit/push and run new exact-head CI.

Current old-head CI42219227768 is complete and failed; all seven shard results were captured. No full pass is claimed.

Release verification42175360279 attempt3 completed: interrupted Cypher and conformance checks passed on rerun; storage count-underflow remains the actual release failure, corrected here. New CI-head verification remains required.

Frozen-source correction checks: full capturecensus PASS251.810s; read-only storagecensus package PASS244.043s (mutant subpackages still running); all packages compile with zero selected tests; focused replication/server/multidb PASS7.010s/5.994s/5.259s; combined storage race PASS9.838s, three repetitions. Full CI make-test gate remains pending on the next pushed SHA.
