# N152-122 validation

## Checked source and scope

Base `f77ec7706de099588ac232b32b6908860511d3da` plus task source/tests. The user assigned rebasing and existing PR review to another agent; no rebase occurred. These are focused working-tree results, not proof of combined current-release qualification.

All Go jobs use `GOMAXPROCS=2 GOMEMLIMIT=1500MiB`, `-p 1`, and `-tags noui,nolocalllm`.

## Original fail-first evidence

Two original test source blocks were copied from Plane N152-122 without changing their assertions: `nor315_unknown_yield_admission_test.go` and `nor315_nil_procedure_result_modes_test.go`.

Original selected tests: `TestNOR315UnknownYieldRejectedBeforeInvocationInEveryMode`, `TestNilResultProcedure.*`, `TestEmptyResultProcedureCallControlOnBothParsers`, `TestFailingProcedureHandlerStaysAnErrorOnBothParsers`, `TestProcedureArityAdmissionAcrossExecutionModes`, `TestProcedureMetadataAdmissionDoesNotInvokeHandlers`, `TestExplainProcedureArityWithoutExecution`. Run with `go test -p 1 -tags noui,nolocalllm ./pkg/cypher -run '<anchored selection>' -count=1 -v`.

FAIL 1.193s before any production edit: exact eight expected RED cells across both parsers. Registered invalid YIELD RUN/PROFILE invoked the handler first; registered and built-in invalid EXPLAIN returned no error. Original nil/empty/error/valid/arity controls passed. Log: `/tmp/n152-122-original-red.log`.

## Causal correction found by broader controls

The initial compiler attempt used `procedureDeclaredReturns`, whose signature fallback treated CREATE PROCEDURE's fabricated `(value :: ANY)` display output as closed metadata. Four existing `TestFabricProcedureWriteIsTheWriteConstituent` cells failed with unknown YIELD w instead of reaching the second-write rejection. That preliminary run failed in 4.363s; `/tmp/n152-122-green.log`. No test was weakened or removed.

Source evidence: `procedure_ddl.go` leaves persisted Outputs empty for CREATE PROCEDURE, while `buildProcedureSignature` emits the value placeholder and the body supplies its actual columns. Parent approved the authoritative nonempty Spec.Returns gate, matching existing pipeline admission. Signature-only display metadata remains runtime validated; genuine Returns declarations remain fail-fast.

## Final focused normal

`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p 1 -tags noui,nolocalllm ./pkg/cypher -run '^(TestNOR315UnknownYieldRejectedBeforeInvocationInEveryMode|TestNilResultProcedure.*|TestEmptyResultProcedureCallControlOnBothParsers|TestFailingProcedureHandlerStaysAnErrorOnBothParsers|TestProcedure.*|TestExplainProcedureArityWithoutExecution|TestPipeline.*Procedure.*|TestFabric.*Procedure.*|TestLocalParticipantOwnsDeclaredYieldProcedure|TestTemporalAsOfDeclaresBothApprovedHeaders|TestSharedProcedure.*|TestCreateCallDropProcedureDDL|TestPersistedProcedurePrecompiledOnStartup|TestLoadPersistedProcedures.*)$' -count=1 -v`

PASS 4.313s: 81 top-level passes, 0 failures, 2 external Neo4j reference skips (`TestProcedureDifferentialNeo4j`, `TestSharedProcedureMetadataMatchesPinnedNeo4j`). Original eight, nested/pipeline handler counts, aliases, undeclared/signature-only metadata, CREATE PROCEDURE/startup, runtime alternatives, built-in ResultSchemas fields and participant ownership controls passed. Log `/tmp/n152-122-green-declarations.log`. Linker emitted its duplicate `-lobjc` warning.

## Remaining checks

Final race, dedicated vet, scoped localization and verifier planning passed as recorded below. Full make verify and indexed duplicate review remain separate gates. Staged source whitespace and formatting checks pass; no performance claim is made.

## Final race, vet and scoped verifier

- Race: the exact final focused normal command above with `-race` added, PASS 22.705s, 81 top-level passes, the same two external Neo4j skips, zero race warnings. `/tmp/n152-122-race.log`.
- Dedicated vet: `GOMAXPROCS=2 GOMEMLIMIT=1500MiB go vet -p 1 -tags noui,nolocalllm ./pkg/cypher`, PASS exit 0, empty `/tmp/n152-122-vet.log`.
- Planning: `make verify-list BASE=origin/release/v1.5.2`, PASS exit 0. Selected localization, UI, llama libraries, build, tests, allocations, conformance and differential; docs skipped. The verifier's own fixture checks emit temporary missing-CI-file warnings but pass. `/tmp/n152-122-verify-list.log`.
- Scoped verification: `GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOFLAGS='-p=1 -tags=noui,nolocalllm' make verify ONLY=localization BASE=origin/release/v1.5.2`, PASS exit 0, including localization/catalog/inventory tests, generated manifest and catalog check. All other verifier steps explicitly unselected. `/tmp/n152-122-localization.log`, `.verify/runs/20261121-205916`.
- Required staged duplicate checker failed during analysis/ingestion at localhost:7474 before candidates (`/tmp/n152-122-duplicates.log`). Automatic worker graph retrieval also lacked a baseline. Manual reuse analysis is recorded; neither is a clean indexed review.

## Scope and cleanup

Only declared-YIELD admission is corrected. Broader procedure-decider convergence remains open; actual dynamic/value-dependent result checks and the direct pipeline boundary are retained intentionally. Keep the umbrella work item in Development.

Per the cleanup skill and parent instruction, preserve the open PR branch/worktree and shared Go cache `/Users/timothysweet/Library/Caches/go-build`, which other release tasks use. This task created no Docker resources or separate worker worktrees; the read-only worker completed. Archive the task logs, verifier run, patch and committed head under `/Users/timothysweet/.local/share/nornicdb-release-evidence/n152-122-20261121`; no further runtime tests are needed after evidence-only edits.
