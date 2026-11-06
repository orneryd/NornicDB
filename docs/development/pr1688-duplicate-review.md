# PR #1688 duplicate review

First analyzed staged tree: `3600ffc9062fd7b03b586e7b6e02d407ac61c7e5`; release base `b098f7e708b4c701a743ffb0310a9b179a84c8f2`. The unchanged analyzer inspected 76 functions, with no cap skips, against remote indexed revision `35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`. That remote revision is stale; all candidates below were inspected in the current checkout. One C-header extraction warning (`pkg/simd/neon_simd_arm64.h`) limits non-Go extraction. The authenticated local HTTPS transport is process-local and changes no analyzer, hook or repository configuration.

| Candidate and existing owner | Comparison and options | Integrated correction | Evidence |
| --- | --- | --- | --- |
| `nor300RequireSyntaxError` versus `requireConstraintFailure`, `requireSyntaxError`, `requireTransactionCommandRejected`, `requireSyntaxErrorStatus` | Same error assertion lifecycle, different expected protocol status. Copying each assertion risks inconsistent checks. A shared syntax-only helper would weaken constraint status checks. | One `requireErrorStatus(t, err, wantCode, statement)` owns all 53 calls. Exact SyntaxError versus ConstraintValidationFailed codes and downstream assertions remain. Retired all five former helpers, no forwarding wrappers. | Focused Cypher tests and race checks; final full gate pending. |
| `nor300IndexNames` versus `readIndexNames` | Both execute schema listing and extract names; constraint versus index listing is a query argument. Order is significant for before/after assertions. | One `readSchemaNames` owns all 53 callers; NOR-300 queries explicitly ORDER BY name. Retired NOR-300 index and constraint listing copies. | Both-parser malformed OPTIONS rejection and valid-map controls; final full gate pending. |
| Explicit-transaction versus ordinary Bolt test-server startup | Binding, serving, waiting for readiness, closing and joining are identical. Executor type and connection limits differ and must remain explicit. | One `serveBoltIntegrationServer` binds and owns lifecycle for both constructors. Constructor-specific executors/configuration remain. No production server change or mutable listener assignment. | Public explicit/autocommit full-fixture equality tests and existing Bolt lifecycle/race tests. |
| `GetEdgesBetween` versus `GetEdgesBetweenVisibleAt` | Both admit ordered endpoint namespaces and select a directed pair. Current reads require the latest between-index plus acknowledged write-behind overlay. Historical reads require requested MVCC versions, including deleted/updated old images and pinned reader registration. Reusing latest reads loses old versions; reusing the old full-graph historical scan materializes unrelated records. | Shared `pendingReadConflict` now owns ordered pair admission. Historical pair selection extends the existing `readAdjacentEdgesAtWithView` owner to filter the opposite endpoint before retaining results. Current pair reads retain the existing indexed/overlay owner. No full outgoing result intermediary, second copying pass or legacy wrapper. | `TestNOR160CurrentAndHistoricalBetweenReads` covers peer update, deletion, reverse direction, detached results and cancellation. Matched 200-node/2,000-edge current/snapshot benchmarks qualify cost; final measurements and full gate pending. |

All four findings led to actual consolidation. No similarity threshold, exclusion, checker source or hook was changed. A final source snapshot must be analyzed after these corrections; this first inventory is not a clean-checker claim.

## Fresh analysis and current-source dispositions

The next unchanged analyzer run completed in 239.0 seconds on staged tree
`7bea3ecd0440a45d5fb97c68cf59ce95ba6edfd3`: 131 functions (two moves),
11 reuse candidates, no refactor targets, no cap skips, and no empty comparison.
The indexed revision and C-header limitation above remained unchanged. The
following are all 11 directional findings, grouped by their seven unique pairs.
These dispositions describe the integrated correction after that snapshot; they
are not a claim that the raw report contained zero candidates.

| Directional findings | Actual common/different behavior | Current-source correction and proof |
| --- | --- | --- |
| `TestNotInOperatorRejected` ↔ `TestListLiteralTrailingCommaRejected` (2) | Same fixture, seed, rejection and positive-control lifecycle; different malformed syntax and seed property. | One named-case table in `TestNotInAndListLiteralMalformationsRejected`; preserves 3+5 and 5+5 rejected/accepted queries and separate namespaces/seeds. Parent checked query-literal multisets. |
| `TestMalformedProjectionAliasesRejectedLikeNeo4j` ↔ `TestEmptyProjectionAndDanglingModifiersRejectedLikeNeo4j` (2) | Same rejection/control loops; different input populations. | One `TestProjectionClauseSyntaxRejectedLikeNeo4j` table, retaining 3+3 and 14+7 queries, exact status checks and separate fixtures. Parent checked query-literal multisets. |
| `TestDanglingComparisonBeforeReturnRejectedLikeNeo4j` → `TestMatchRejectsRelationshipReuseWithinOnePattern` (1) | Both submit one malformed MATCH through the public executor. Only relationship reuse additionally requires `RelationshipUniquenessViolation` detail. | One `TestMalformedMatchStatementsRejectedLikeNeo4j` table owns fixture and execution. Syntax-only and exact semantic-detail assertions remain distinct branches using existing assertion owners. Both former top-level owners retired. |
| `TestIndexDropProcedureExplicitTransactionKeepsDropUntilCommit` ↔ `TestIndexDropProcedureExplicitTransactionRollbackPublishesNothing` (2) | Same transaction/drop/independent-reader lifecycle; commit publishes the remaining index and rollback does not, then must allow retry. | One parser × procedure × terminal-operation table, eight leaves. Exact BEGIN/drop/inside/outside/other-namespace/terminal/retry assertions retained; rollback retry now checks exact names instead of only length. |
| `TestPipelineProcedureRequiresWithAfterSet` ↔ `TestExecuteMatchWithCallProcedure_ErrorAndEmptyPaths` (2) | Common cleanup-owned executor construction and classified-error assertions are reusable. The first requires a seeded mutable node, rejects SET→CALL without WITH without changing value 1, and verifies valid WITH changes it to 2. The second requires an empty graph, checks standalone RETURN and CALL, malformed MATCH rejection, and empty-input deterministic YIELD columns without evaluating a missing vector index. | Both reuse existing `newTestExecutor` and `requireErrorStatus` directly. Their remaining public Execute calls and assertions express different input/state/result contracts. Combining those calls into forwarding test adapters would only move the statements; combining the seeded and empty populations would invalidate one control. No second fixture/error implementation remains. Focused integrated tests exercise both exact sequences. |
| `TestExplicitTransactionMergeUpdateViolationsFailTheStatement` → `TestExplicitTransactionConstraintViolationFailsTheStatement` (1) | Shared fresh transaction / BEGIN / rejected mutation / terminal sequence; different constraint fixtures, 15 versus four mutation statements, and different publication oracles. | One `constraintRejectedInExplicitTransaction` lifecycle helper owns 19 sequences. Original exact constraint status, COMMIT versus ROLLBACK and count/ordered-value no-publication checks retained. The separate multi-statement failure and peer-commit cases keep their additional observable steps. |
| `startBoltIntegrationServerWithExplicitTx` → `startBoltIntegrationServer` (1) | Constructors differ only in executor capability and connection budget; bind/serve/cleanup is already shared. | Retired the explicit constructor. One typed `boltIntegrationMode` constructor selects ordinary executor/10 connections or real explicit-transaction executor/32; preserves 8192-byte buffers and shared lifecycle. Migrated all callers, no forwarding wrappers. Parent independently checked mechanical edits outside definition files. Worker failed before returning a verified result, so acceptance comes from parent review and integrated checks. |

No analyzer, hook, threshold or exclusion changed. Later test-body convergence
uses the same production source as this analysis. The inactive-visibility clock
fast path was already present in its snapshot.
Final full-suite verification and benchmark results are recorded on the PR;
raw reports and receipts are retained outside Git with the task evidence.

## Subsequent source convergence

Later source changes extend the shared schema tail owner with typed execution-
time map/parameter binding and replace the two vector substring readers. The
constraint-contract router now selects its already parsed syntax before calling
the single execution owner; the mixed result/handled/error forwarding entry
point and redundant execution wrapper are retired. Copy traversal uses one
per-call inline/spill visited-state owner shared by maps, lists and references.
The endpoint capability checks live visibility before any positive shortcut.
Integrated native focused and race tests, strict lexical membership and stored-
entity ordering checks pass. The earlier inventory is not an analysis of this
later source; a fresh advisory CI report is pending. No duplicate-analyzer rule,
threshold or exclusion was changed. The explicitly requested removal concerns
installed local Codex hooks only; tracked hook/checker source remains intact.

## Release CI increment correction

Release verification `42009807718` on `8f5835d7f` and PR make-test run
`42011914534` failed the NOR-136 acknowledged-increment invariant. A deterministic
observer-controlled test reproduces a reserved-but-unpublished peer version:
BEGIN includes its sequence but reads the predecessor, then previously committed
a stale update without conflict. The original stress assertion is unchanged.

The correction extends `validateNodeMutationPreimagesLocked` (formerly the
constraint-only preimage validator), retaining its first-mutation fold and single
batch read cut. Write-behind compares predecessor properties with the existing
`sameStoredValue` owner and labels with `sameLabelSet`; constraint-specific error
classification still uses the existing owner. It introduces no parallel read,
comparison, retry or flush implementation, forwarding wrapper, or storage format.
The revision-only experiment was discarded: ordinary written cache images have
no covered revision, so that experiment did not catch the reproduced failure.

The existing stored-value comparator now treats an unchanged NaN bit pattern as
the same stored value. Its numeric ordering remains unchanged for other values.
The regression matrix covers implicit/native followers, peer images before and
after replay, a healthy subsequent update, and preserved NaN payload bits.
The broader checker/graph result is not claimed clean: configured Graphify
retrieval still returns HTTP 400 (HTTP sent to an HTTPS port). Advisory duplicate
analysis and the full suite must be evaluated on the pushed CI head.

This validation adds full-property predecessor comparison for write-behind node
updates; no allocation or latency improvement is claimed for this correctness
fix. Non-write-behind unconstrained updates retain the previous early return.
