# Current verification

Implementation base: f77ec7706de099588ac232b32b6908860511d3da; uncommitted candidate.

## Fail-first proof

- Exact archived real composite HTTP fixture, production unchanged at70ea82df6:
  `env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/server -run '^TestNOR78CompositeRemoteHTTPErrorCleanup$' -count=1 -timeout=5m -v`
  Expected FAIL2.346s: both parser d=0 cases report extra TransactionNotFound;
  active successful rollback, later404 and local-zero controls pass.
  Log `/tmp/n152-145-original-red.log`.
- After ff-only to f77, same real fixture plus truthful metadata checks:
  `... ./pkg/server -run '^Test(NOR78CompositeRemoteHTTPErrorCleanup|HTTPStatementFailureReportsRetiredTransaction)$' -count=1 -timeout=5m`
  Expected FAIL2.870s: both original failures plus begin/execute stale Transaction.
  Log `/tmp/n152-145-integrated-red.log`.
- Direct client boundary tests before production changes:
  `... ./pkg/storage -run '^TestRemoteHTTP(StatementTerminalObservation|UnknownStatementOutcomeRetainsRollbackFailure)$' -count=1 -timeout=3m`
  Expected FAIL1.119s: missing observed-closure handling and lost secondary error;
  unknown-outcome controls pass. Log `/tmp/n152-145-owner-red.log`.

## Candidate checks

Normal selection attempted:
```
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/storage ./pkg/fabric ./pkg/server -run '^Test(Remote|Credential.*Remote|RollbackParticipants|NOR78|HTTPStatementFailureReportsRetiredTransaction|HTTPCompositeRequestCleanupFailures|HTTPCompositeRollbackPanicContained|CompositePublicStatementPanicCannotCommit|ExplicitTransactionWorkflow|ExplicitTransactionRollback|ExplicitTransactionCommitPersistsCreate|HandleOpenTransaction)' -count=1 -timeout=8m -v
```
Build failed with host `no space left on device` before runtime tests. No pass is
claimed. `/tmp/n152-145-focused-green.log` preserves the failure. Its temporary
Go build directory was automatically removed; host free space was676MiB after
exit. Parent is coordinating safe cleanup; no shared cache was deleted.

`make verify-list BASE=origin/release/v1.5.2` exited0: localization, UI, llama,
build, tests, allocations, conformance and differential selected; docs skipped.
Verifier selftests emitted missing temporary-fixture workflow sed warnings but
completed their assertions and returned0; raw log retained.

`env GOMAXPROCS=2 GOMEMLIMIT=1500MiB make verify BASE=origin/release/v1.5.2 ONLY=localization`
passed9s. UI, llama-libs, build, tests, allocations, conformance, differential and
docs were not executed by this scoped run. This is not full qualification.

`scripts/code-intelligence-hook.sh check-staged` exited1: analysis/ingestion
failed against configured http://localhost:7474/nornicdbcode. The canonical
CONTRIBUTING policy makes this advisory for review candidates; indexed analysis
remains incomplete, never a clean/empty search. Log `/tmp/n152-145-duplicate.log`.

After parent-confirmed absence of active Go work, shared Go build cache cleanup
completed and restored336GiB free space. The failed build is retained above.
Bounded normal reruns at the same candidate source passed:

- Storage selection `^Test(Remote|Credential.*Remote)`:70 top-level tests passed,
  1.873s. One external-reference test skipped (`TestRemoteEngineHTTPNullParameters`,
  unset `NORNICDB_NEO4J_REFERENCE_HTTP_URI`).
- Fabric/server selection from the attempted combined command:38 top-level tests
  passed in aggregate; fabric1.712s, server7.832s. One external-reference test
  skipped (`TestRemoteHTTPPinnedNeo4j`, pinned reference HTTP URI unset).
- Original real two-server defect, begin/execute metadata, direct terminal/unknown
  transport boundaries, repeated Commit and Rollback-to-Commit checks all pass.
- Focused race passed: storage3.181s, fabric2.769s, server27.505s;12 top-level tests, no skips or race warnings. Dedicated package vet passed with empty output.

Source-only whitespace checks pass.20 whitespace warnings are confined to the
verbatim historical failure transcript (trailing spaces and space-before-tab
indentation); the evidence was preserved rather than rewritten.

Focused race command (passed):
```
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -race -p=1 -tags=noui,nolocalllm ./pkg/storage ./pkg/fabric ./pkg/server -run '^Test(RemoteHTTPStatementTerminalObservation|RemoteHTTPUnknownStatementOutcomeRetainsRollbackFailure|RemoteEngineHTTPExplicitTxLifecycle|RemoteEngineQueryCypherAndHTTPClosedBranches|CredentialedRemoteHTTPClientErrorsAreRedacted|RemoteParticipantEndsOnce|RollbackParticipants|NOR78|HTTPStatementFailureReportsRetiredTransaction|HTTPCompositeRequestCleanupFailures|HTTPCompositeRollbackPanicContained)' -count=1 -timeout=8m -v
```
Log `/tmp/n152-145-focused-race.log`. Cold race compilation after cache cleanup
is separate from runtime package durations.

Dedicated vet command passed (exit0, empty `/tmp/n152-145-vet.log`):
```
env GOMAXPROCS=2 GOMEMLIMIT=1500MiB go vet -p=1 -tags=noui,nolocalllm ./pkg/storage ./pkg/fabric ./pkg/server
```

No N145 Go processes remain. No task-owned container, image, isolated cache or
additional worktree was created. The active candidate worktree/branch, shared
Go cache, raw reference and verification logs are retained for parent review and
publication; other active tasks depend on the shared cache. No commit or push
was performed by the implementation agent. Full verifier/default-tag/platform
qualification remains pending; indexed duplicate discovery remains incomplete.
