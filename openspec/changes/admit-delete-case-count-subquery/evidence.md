# Recovery, owner trace and verification evidence (N152-147 / NOR-80)

Checkout: `/private/tmp/n152-147-count`, HEAD `122130b27b6161ca117b63f56a265e4bbdcbe9af`
("fix(cypher): preserve inherited conditional outputs (N152-48) (#1916)").
Ticket snapshot: `/tmp/n152-147-ticket.json` (Plane snapshot of the historical description HTML, 168 597 bytes, 7 `<pre>`
blocks). Repository instructions read: `AGENTS.md`, `CONTRIBUTING.md`,
`.agents/standing-design-principles.md`, `openspec/config.yaml`.

At the initial recovery stage nothing was executed. The initial engine/reference
results below are quoted from the ticket and source statements describe the recorded
HEAD. Follow-up sections distinguish fresh implementation runs and their actual base.

## Recovered artifacts

The ticket supplies one combined reproduction matrix for NOR-79/80/81/82 as HTML `<pre>`
block 0, plus two reference scripts (blocks 1, 2), two Neo4j result logs (blocks 3, 4) and two
captures of one NornicDB test run (blocks 5, 6). Extraction: `<pre>` inner text, HTML tags
stripped, entities unescaped, no re-wrapping — blank lines, tabs and trailing newline preserved.

| File | Bytes | SHA256 | Provenance |
|---|---|---|---|
| `evidence/nor79_to_82_static_claims_test.go.txt` | 17 241 (391 lines) | `ced90f8c9729737ab2b8d81416426a418540bb990143fe852d176f8ee53918c5` | Ticket `<pre>` block 0, verbatim. Package `cypher`; the ticket's own file name is `pkg/cypher/nor79_to_82_static_claims_test.go`. |
| `evidence/nor80-static-claims.txt` | 720 (20 lines) | `857747c4ac84869fdea182d0352f429b23a722ba9f8fdefcbd65d4800dc8d941` | `var nor80StaticClaims` block of the file above, raw lines 103-122 (line 123 begins `var nor81StaticClaims`), extracted unchanged. This is NOR-80's scoped fixture: its three probes and reference verdicts. |
| `evidence/nor80-reference-results-excerpt.txt` | 1 717 (81 lines) | `15aaf511b7866cab69982e9733196d63cc9d95e60aa4453f8ab65d33c2044615` | Ticket `<pre>` block 4, verbatim, recorded as `/tmp/nor80-81-reference-results.log`: probes 0-3 × RUN/EXPLAIN/PROFILE against Neo4j 5.26.30. |

Recovery checks (cheap, non-executing):

- `gofmt < evidence/nor79_to_82_static_claims_test.go.txt` parses the recovered text and emits
  byte-identical output (both digests `ced90f8c…18c5`), so the recovered file is syntactically
  valid Go and already gofmt-formatted. That proves parse and formatting only; the file has not
  been compiled into this checkout.
- The scoped fixture hash was recomputed from the extracted block, so it pins exactly the text
  quoted in this change; it is a derived excerpt of the full artifact, not an independent source.

The full artifact is NOR-79/80/81/82 combined. Only `var nor80StaticClaims`
(lines 103-123) and its harness are this ticket's scope; the sibling claim tables
(`nor79StaticClaims`, `nor81StaticClaims`, `nor82StaticClaims`) are recovered because they are
part of the same supplied file, and are **not** evidence for this change.

### NOR-80 fixture, verbatim scope

```go
var nor80StaticClaims = []norStaticClaim{
	{
		name:    "procedure_yield_where_count_subquery",
		query:   "UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { MATCH (m) } > 0 RETURN label",
		accepts: true,
	},
	{
		// The source's CALL-bearing COUNT { CALL { ... } RETURN ... } variant,
		// kept exactly as supplied.
		name:    "procedure_yield_where_count_call_subquery",
		query:   "UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { CALL { MATCH (m) RETURN m } RETURN m } > 0 RETURN label",
		accepts: true,
	},
	{
		// The ticket's second root.
		name:    "delete_case_count_subquery",
		query:   "UNWIND [] AS s MATCH (n) DELETE CASE WHEN COUNT { MATCH (m) } > 0 THEN n END",
		accepts: true,
	},
}
```

All three probes were supplied inline in the ticket (the ticket explicitly instructs replaying
the CALL-bearing COUNT variant exactly), so none is reconstructed. `params` is nil for all
three; the reference supplied no parameters for them.

### Recorded reference and proven scope

Reference: Neo4j 5.26.30 community, cached image
`sha256:174ca8edaafb189d2676027a738815b04afd56d7dc720bd08bbb37afb469483e`, ephemeral isolated
store on Buttocks (2 CPU / 4 GiB, no published ports). 36 commands (12 probes × 3 statement
kinds) completed before shutdown; the ticket records that `docker stop` exceeded its default
timeout and exited 137 with `OOMKilled=false`, which is not a clean-exit claim and not a failed
query.

NOR-80 reference outcomes (from `evidence/nor80-reference-results-excerpt.txt`):

| Probe | RUN | EXPLAIN | PROFILE |
|---|---|---|---|
| 0 `procedure_yield_where_count_subquery` | `exit=0` | `exit=0`, `READ_ONLY` | `exit=0`, `READ_ONLY`, `Rows: 0` |
| 1 `procedure_yield_where_count_call_subquery` | `exit=0` | `exit=0`, `READ_ONLY` | `exit=0`, `READ_ONLY`, `Rows: 0` |
| 2 `delete_case_count_subquery` | `exit=0` | `exit=0`, `WRITE_ONLY` | `exit=0`, `WRITE_ONLY`, `Rows: 0` |
| 3 `reduce(a = 0, x IN toInteger('1') \| a)` (NOR-81 negative control, not this scope) | `exit=1` | `exit=1` | `exit=1` |

`Type mismatch: expected List<T> but was Integer` for probe 3.

Proven scope for this change:

- Freshly verified against Neo4j 5.26.30 for this matrix: all 12 supplied probes (36 commands):
  NOR-79 rejection and acceptance, NOR-80 probes 0-2, NOR-81 acceptance and its reduction
  control, NOR-82 `date()` / `duration('P1D')` / `[]` / `n.j`. The reference script for the
  `[]` and `n.j` variants is in the ticket; their fresh acceptance superseded an older inferred
  rejection (see the artifact's provenance comment).
- NOT covered: NOR-79's "120 generated parameter cases" (never supplied inline; no canonical
  population of that size in the repository), NOR-82's "64 inherited H2 false accepts" (same),
  and the ticket's triage findings kept in an attachment (D12). The supplied ticket snapshot has
  no attachment field or attachment body, so attachment-only content is unread and uncovered.
- The recovered constructor-family verdicts (NOR-79's 258 cases) come from the canonical
  `staticConstructorTypesOracle` (`pkg/cypher/static_constructor_types_test.go:23`), which is
  recorded in-repository data, not a fresh Neo4j run.

### Recorded NornicDB run

Head `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5` (`v1.5.1-2-gd6ec7766e`). Ticket command:

```
env GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1500MiB go test -tags=noui,nolocalllm -p=1 -count=1 \
  -timeout=180s -run '^TestNor79To82' -v ./pkg/cypher
```

Result `FAIL`, package time 2.612 s in the recorded log (the ticket's summary line reports
1.938 s for the Mac package; the two are separate captures of the same execution, see below).
Counted from the recorded log by statement and probe:

- 144 leaf cells: 60 failing / 84 matching.
- Failing: NOR-79 `parameter_list_comprehension_subscript` 12, NOR-80 `delete_case_count_subquery`
  12, NOR-81 `any_over_timestamp` 12, NOR-82 `toUpper_head_dotk_plus_date` 12,
  NOR-82 `toUpper_head_dotk_plus_duration` 12.
- Matching: NOR-79 `parameter_arithmetic_into_toUpper` 12, NOR-80
  `procedure_yield_where_count_subquery` 12, NOR-80
  `procedure_yield_where_count_call_subquery` 12, NOR-81 controls 12 + 12, NOR-82 `[]` 12,
  NOR-82 `n.j` 12.
- Verdicts are identical in every cell across both parsers, auto and explicit transaction, and
  RUN/EXPLAIN/PROFILE; `TestNor79To82NegativeControlsReuseCanonicalVerdicts` passed.

`<pre>` blocks 5 and 6 are **one** execution captured twice: after normalising the
`(0.03s)`-style durations, 562 vs 563 lines differ only by the capture prefix
(`# github.com/orneryd/nornicdb/pkg/cypher.test` + `ld: warning: ignoring duplicate libraries`
vs the three OpenSSH post-quantum warning lines) and one parent duration. Both show 73 FAIL /
85 PASS lines. No second run is claimed.

Every NOR-80 DELETE cell's recorded error:

```
Neo.ClientError.Statement.SyntaxError: DELETE expression "CASE WHEN COUNT { MATCH (m) } > 0 THEN n END" refers to an undefined variable
```

## Continuing applicability to this checkout

- `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5` is **not** an ancestor of this HEAD; the merge base
  is `cb0a574f75b183dfebfba6c4726b72244f7f374b` (`v1.5.1`), so the ticket head is 2 commits and
  this HEAD is 60 commits past `v1.5.1`.
- The owners of the recorded failure are unchanged between that head and this one
  (`git diff --numstat d6ec7766…HEAD`):
  `pkg/cypher/set_semantic_validation.go` no diff,
  `pkg/cypher/executor_mutations_delete_guard.go` no diff,
  `pkg/cypher/static_operator_types.go` no diff,
  `pkg/cypher/subquery_value_expression.go` no diff.
- `pkg/cypher/pipeline_mutation.go` differs by 3 added / 1 removed line, entirely inside
  `pipelineApplySet` (fine-grained access control: `auth.FineGrainedRequestFromContext`), not in
  `pipelineApplyDelete`.
- `pkg/cypher/show_admin.go` differs by 112 added / 8 removed lines in the SHOW/user listing and
  running-transaction paths; `undefinedVariableError` (`show_admin.go:706`) is outside those
  hunks and is not on the DELETE path.
- `git log --oneline cb0a574…HEAD -- pkg/cypher/set_semantic_validation.go
  pkg/cypher/executor_mutations_delete_guard.go pkg/cypher/static_operator_types.go` is empty, so no
  commit in range touches the DELETE scope check, `deleteExpressionRootIdentifier` or the static
  operator checker. The CASE-related commits in range (`ce579d39b` NOR-440, `122130b27` N152-48)
  change arithmetic operand handling and conditional frames; `9b8608319` (CASE keyword operand
  boundaries, `set_semantic_validation.go`) predates `v1.5.1` and is already in the base.
- Consequence: the ticket's recorded failure continues to describe this source for the failing
  statement. It does **not** describe this checkout's whole tree, and no other recorded result
  (sibling cells, reference container behaviour) is re-established here.

## Causal owner, as read in this checkout

`pkg/cypher/match_semantic_validation.go`

- 268 `semanticScopeWalk`; 623-627 the `pipelineClauseDelete` case calls
  `validateDeleteClauseScope(mutationScope(), clause.text)`, then `deleteTargetTypeError`, then
  `validateStaticClauseTypes`. This ordering is why the first error a DELETE statement receives
  is the scope error.
- 785-810 `undefinedExpressionVariable`: simple identifier/property-chain fast path, otherwise
  `expressionFreeVariables`.

`pkg/cypher/set_semantic_validation.go`

- 50-64 `validateDeleteClauseScope`: line 53 `trailingWordAfterOperand` (accepts this target),
  line 60 `deleteExpressionRootIdentifier` + `scope.contains`.
- 67-110 `trailingWordAfterOperand`, 111-118 `isExpressionOperatorWord` (`CASE`, `WHEN`, `THEN`,
  `ELSE`, `END` are operator words, so `CASE … END` leaves no trailing word).
- 136-142 `deleteUndefinedVariableError` → `Neo.ClientError.Statement.SyntaxError`,
  detail `UndefinedVariable`, message `DELETE expression %q refers to an undefined variable`.
- 203-211 `validateWriteExpressionScope`; 213-221 `firstUndefinedSetExpressionVariable`;
  228-392 `expressionFreeVariables`, with 337-345 (subquery bodies are not expressions) and
  417-428 `setExpressionKeyword`.

`pkg/cypher/executor_mutations_delete_guard.go`

- 152-175 `deleteExpressionRootIdentifier`: leading identifier via
  `cyphertext.ScanIdentifier`, `""` for a following `(` (function call) and for
  `null`/`true`/`false`. `CASE` is returned as a variable name.
- 351-370 `staticDeleteTargetTypeError`: `checker.check` per target with
  `scope.complete = false`; comment keeps undefined targets with the DELETE scope owner.

`pkg/cypher/static_operator_types.go`

- 415-440 `check`/`derive`; 470-477 CASE routing; 600+ `checkCase`; 956-989
  `staticPatternExpressionType`/`staticSubqueryExpressionOperand` (COUNT/COLLECT/EXISTS result
  types; `COUNT` → Integer).

`pkg/cypher/pipeline_mutation.go`

- 23-41 `pipelineApplyDelete`: `deleteClauseTargets`, label-qualifier rejection, then
  `expressionFreeVariables` + `deleteUndefinedVariableError(name)`. 43-70 row evaluation,
  `evaluateRowValue`, `isDeleteTargetValue`.

Reachability of row evaluation for a `COUNT { … }` operand: `pkg/cypher/subquery_value_expression.go`
provides the subquery-value evaluator (`subqueryExpressionKeywordAt`, line 77; the row evaluator
rewrites `COUNT { … } <op> <integer>` at `pkg/cypher/row_expression.go:1544-1550`), so the runtime
path has an owner for the construct. That is code reading, not a run: whether a non-empty DELETE
`CASE` with a `COUNT { … }` operand evaluates correctly is unverified and out of scope.

## Existing tests and owners this change touches or must keep

| Test | Location | Why it matters here |
|---|---|---|
| `TestDeleteRejectsInvalidTargetsBeforeMutation` | `pkg/cypher/delete_test.go:141-175` | Pins DELETE target rejections by detail only: `InvalidDelete` (`DELETE node:Other`), `UndefinedVariable` (`DELETE missing`), `InvalidArgumentType` (`DELETE 1 + 1`), and that nothing is deleted. The `UndefinedVariable` row guards the negative control. |
| `TestUndefinedVariableInUpdateClauseIsRejected` | `pkg/cypher/subquery_update_scope_test.go:17-87` | Asserts the message text "Variable \`m\` not defined" for `SET`/`REMOVE`/`DETACH DELETE m`, in memory and server-stack executors, auto and explicit transaction. See open item 1. |
| `TestMalformedOperandsAreSyntaxErrors` | `pkg/cypher/malformed_operand_test.go:15-160` | Direct `trailingWordAfterOperand` cases, including `MATCH (n:DT {id: 3}) DETACH DELETE n COMMIT` (#741, Neo4j "Invalid input"). The neighbouring check this correction must not disturb. |
| `deleteClauseTargets` cases | `pkg/cypher/deleted_entity_reads_test.go:275-283` | `REMOVE`, `DETACH DELETE a, b` splitting. |
| `TestQueryBodyContainersMatchGrammar` / manifest | `pkg/cypher/query_body_containers_test.go:126-190`, `pkg/cypher/query_body_manifest_test.go:71` | The `deleteSt` witness is already `MATCH (n) DELETE CASE WHEN COUNT { CALL { @X@ } } > 0 THEN n END`, so the grammar carriers admit the shape today. |
| `staticTypeNeo4jVerdicts` | `pkg/cypher/static_types_neo4j_verdicts_test.go:14-18+` | Canonical Neo4j 5.26.30 verdict matrix; holds both NOR-81 negative controls (lines 17-18) and has no DELETE entry. |
| `staticConstructorTypesPopulation` / `Oracle` / `Params` | `pkg/cypher/static_constructor_types_test.go:23,27,39` | Canonical constructor population the recovered artifact pins by length and SHA256 (NOR-79 sibling scope). |
| `TestProcedureYieldSubqueryScope` (`COUNT { MATCH (…) } > 0` in YIELD WHERE) | `pkg/cypher/procedure_yield_subquery_scope_test.go:28,33,60` | Execution-level owner of the matching-control shape. |
| `COUNT { … }` in WHERE | `pkg/cypher/subquery_call_in_where_test.go:26-30,94-97` | Execution-level owner of the control shape. |
| Helpers | `pkg/cypher/quantified_group_predicates_test.go:211` (`forEachParser`: `config.ParserTypeNornic` + `config.ParserTypeANTLR`), `pkg/cypher/test_helpers_test.go:14` (`newTestMemoryEngine`) | Required by the recovered artifact; both exist. |

## Validation contract

- `AGENTS.md` / `CONTRIBUTING.md`: fail-first reproduction, then the minimal root-cause fix,
  then regression; exercise the public path (`Execute`), not a helper; record head, base,
  commands, results and every skipped or unexecuted check; never present a focused pass as a
  full-suite pass.
- `make verify-list BASE=origin/<pr-base>` previews the path-filtered plan and
  `make verify BASE=origin/<pr-base>` runs it (steps `localization ui llama-libs build tests
  allocations conformance differential docs`); `make test` is the package set. The temporary
  local PR verification policy in `CONTRIBUTING.md` (through 2026-10-31) requires the result
  comment with head SHA, base SHA, command, results and skips.
- Focused command for this package (the ticket's own): `env GOMAXPROCS=2 GOGC=50
  GOMEMLIMIT=1500MiB go test -tags=noui,nolocalllm -p=1 -count=1 -timeout=180s -run
  '^TestNor79To82' -v ./pkg/cypher`; plus `-run 'TestDeleteRejectsInvalidTargetsBeforeMutation|
  TestUndefinedVariableInUpdateClauseIsRejected|TestMalformedOperandsAreSyntaxErrors'` for the
  neighbouring contracts.
- The recovered artifact already encodes the matrix contract: fresh engine and executor per
  cell, both parsers, auto and explicit transaction, RUN/EXPLAIN/PROFILE; accepted RUN must
  return zero rows from the `UNWIND []` seed; EXPLAIN/PROFILE assert a non-nil result and the
  verdict only; rejections must be `Neo.ClientError.Statement.SyntaxError` in the expected
  message class.
- `openspec/config.yaml`: bug fixes start from a failing reproduction and record the failing
  baseline; implementation tasks stay unchecked until verified, and artifact creation is not
  implementation.

## Open verification items and limits

1. Two DELETE undefined-variable message shapes exist in committed tests: detail-only
   (`delete_test.go:148`) and literal "Variable \`m\` not defined"
   (`subquery_update_scope_test.go:26-31`). The traced route (`semanticScopeWalk` →
   `validateDeleteClauseScope` → `deleteUndefinedVariableError`) would render
   `DELETE expression "m" refers to an undefined variable` for a top-level `DETACH DELETE m`,
   which does not contain that literal, so either that statement is rejected by an earlier owner
   on that route or the assertion is satisfied elsewhere. The exact owner of the second message
   was **not established statically here**. The parent's focused run settles it; both tests are
   in the regression set either way.
2. The stricter scan changes admission for targets whose leading root is bound but a nested
   reference is not (`DELETE n.list[x]`, `DELETE id(x)`). No test in this checkout pins the
   permissive result, and Neo4j rejects it, but the change is a behaviour change inside the same
   causal unit and needs the parent's decision.
3. Downstream acceptance (`trailingWordAfterOperand`, `staticDeleteTargetTypeError`, `checkCase`,
   `staticSubqueryExpressionOperand`, `deleteTargetTypeError`, row evaluation of a `COUNT { … }`
   operand) is static analysis. The recorded failure never reached those owners, so this change
   cannot claim the statement would be accepted end to end.
4. The recovered artifact has not been compiled in this checkout; only gofmt parse/format was
   checked.
5. The Neo4j reference and the NornicDB failure are the ticket's recordings, not fresh runs; the
   ticket's own caveat about its container stop (exit 137, `OOMKilled=false`) is carried here as
   recorded.
6. Attachments, comments and history beyond the supplied description are not in the snapshot;
   attachment-only triage (D12) and any comment history are unread.
7. Unrecovered populations (NOR-79's 120 generated parameter cases, NOR-82's 64 inherited H2
   false accepts) are not covered, and no whole-issue or whole-family claim is made.
8. Sibling NOR-79/81/82 defects were not investigated further than separating their cells from
   NOR-80's; other worktrees own them.


## Follow-up source and test preparation

Tracked source fast-forwarded cleanly to `3aa1f1dceb758155f249d6c984c546e088ae40f9`;
untracked recovery artifacts were preserved. No original fixture bytes changed.
`validateDeleteClauseScope` has exactly one production caller. The root helper has
exactly one caller (that scope check); `cyphertext` is used in its file only by the
helper, so removing both is justified consolidation rather than a new scanner.

The message-path gap is resolved by current source: `executor_entry.go:875`
invokes `admitStatement`; `statement_admission.go:60-74` defers type validation only
when binding admission succeeds (or a participant signature is deferred).
`projectedStatementScope` produces binding errors through
`boundCompilation.bindSymbol` → `failUndefined` → `createUndefinedVariableError`.
Thus actual unbound DELETE symbols retain `Variable ... not defined` before the
later semantic scope check. The valid CASE symbol structure passes binding and
reaches the erroneous root heuristic. The approved edit keeps the latter owner's
`deleteUndefinedVariableError(expression)` unchanged in format and argument.

Prepared public tests: exact three recorded probes ×2 parsers ×2 tx modes ×3
statement modes =36; six non-empty CASE targets ×2 parsers ×2 tx modes =24;
five unknown-reference targets ×2 parsers ×2 tx modes ×3 statement modes =60.
Tests remain unrun pending CPU scheduling; production remains unchanged until
fail-first evidence is preserved. Recovery-stage statements above describe the
historical inspection state, not completed implementation/verification.


## Fail-first and implementation

At release base `3aa1f1dceb758155f249d6c984c546e088ae40f9`, before production
edits, command
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^TestDeleteCountSubqueryAdmission$' -count=1`
failed in 1.285s: all 12 DELETE cells falsely rejected, all 24 procedure-filter
controls accepted. Full output `/tmp/n152-147-red.log`. This freshly reproduces
the historical error on the actual implementation base.

Applied the approved correction: `validateDeleteClauseScope` now receives the
current context and reads free variables through `firstUndefinedSetExpressionVariable`,
the existing canonical expression reader. Its sole caller supplies context.
`deleteUndefinedVariableError(expression)` and the preceding malformed-operand
check are unchanged. The unused leading-root helper and its now-unused
`cyphertext` import were removed. No new parser, consumer-specific skip or fallback.
The focused green suite is running; no success is claimed before completion.


## First focused run: admission fixed, non-empty runtime gap found

Command
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^Test(Delete|DetachDelete|UndefinedVariableInUpdateClauseIsRejected|MalformedOperandsAreSyntaxErrors|QueryBodyContainersMatchGrammar|ProcedureYieldSubqueryScope)' -count=1`
failed in 5.025s. The original 36 admission cells and 60 scoped negative cells
passed, as did the selected existing DELETE/update/malformed/body tests. Only the
24 non-empty CASE/COUNT mutation cells failed, with `could not evaluate expression:
COUNT { ... }`. Full log: `/tmp/n152-147-focused-green.log`.

Read-only trace: `pipelineApplyDelete` calls `evaluateRowValue` directly with a row
that lacks `rowStatementContextKey`; `evaluateRowSubqueryBody` correctly refuses to
substitute an operation context for the missing row statement context. Projection
and predicate consumers use the existing context-aware adapters. The DELETE
constant precheck also needs investigation for expressions with no outer free
variables but graph-dependent subqueries. No runtime patch has been applied;
parent review is pending. Admission-only success is not claimed as completion.


## Runtime convergence and normal pass

Parent approved reuse of `evaluateRowExpressionWithContext` for reached DELETE
targets. `pipelineApplyDelete` ensures the existing expression-failure slot once
at entry, preserving an existing statement owner. After each evaluation it checks
that slot before unresolved/type handling and before collecting mutation targets.
No error is suppressed or converted into an empty success. The pre-row constant
check remains for context-free expressions; existing `rowContextShapeOf` context
needs defer graph/context-dependent targets to reached rows. No new classifier,
COUNT evaluator or DELETE-only expression parser was added.

Added the all-null non-empty case and explicit boundary checks: a zero-row DELETE
whose all-null CASE contains a division-error COUNT body succeeds without reaching
that body; the corresponding reached body reports the original ArithmeticError
and preserves both nodes. The integrated normal command (same selection as above)
passed in 3.889s: 36 recovered admission cells, 28 non-empty mutation cells,
60 undefined-reference cells and 8 runtime-error boundary cells (132 total new
cells), plus existing selected DELETE/update/malformed/body guards. Log:
`/tmp/n152-147-focused-final.log`. Focused race checks are pending.


## Preserved transcript whitespace

`git diff --cached --check` reports eight trailing-whitespace warnings in the
byte-preserved historical `evidence/nor80-reference-results-excerpt.txt`, lines
6, 14, 27, 35, 48, 56, 72, 77 (`EXPLAIN ` / `PROFILE ` headings). Parent explicitly
required retaining raw bytes rather than normalizing or hiding this result.
`git diff --cached --check -- '*.go'` passes. The full diff check is therefore not
reported as clean. No checker rule or attribute was altered.


## Final bounded verification and cleanup inventory

Base `3aa1f1dceb758155f249d6c984c546e088ae40f9` plus the staged N147 patch.
Every local check used `GOMAXPROCS=2 GOMEMLIMIT=1500MiB`, sequentially:

- Normal: `go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^Test(Delete|DetachDelete|UndefinedVariableInUpdateClauseIsRejected|MalformedOperandsAreSyntaxErrors|QueryBodyContainersMatchGrammar|ProcedureYieldSubqueryScope)' -count=1`
  PASS 3.889s; 84 matching top-level declarations, including 132 new cells.
  `/tmp/n152-147-focused-final.log`.
- Race: `go test -p=1 -race -tags=noui,nolocalllm ./pkg/cypher -run '^Test(DeleteCountSubquery|DeleteRejectsInvalidTargetsBeforeMutation|DeleteMultiRowOneConnected|DeleteConnectedNodeExplicitTransaction|UndefinedVariableInUpdateClauseIsRejected|PublicDeleteStopsAtCanceledNativeIncidentRead|StatementDeleteStagesNothingOnCanceledNativeIncidentRead|BoundDelete|ForeachDelete|ConnectedDelete)' -count=1`
  PASS 6.057s; 15 matching top-level declarations. Public and direct cancellation,
  bound DELETE, connected/FOREACH and atomic guards included.
  `/tmp/n152-147-focused-race.log`.
- Vet: `go vet -p=1 -tags=noui,nolocalllm ./pkg/cypher` PASS, empty output;
  `/tmp/n152-147-vet.log`.

No full-suite, fresh Neo4j run, benchmark or newer-release-head qualification is
claimed. The inherited reference is explicitly recorded evidence; the original
36-cell reproduction and integrated Nornic checks are fresh. Parent owns canonical
verification and staged duplicate-check result incorporation. The admission and
runtime paths converge on existing owners; no new scanner/evaluator is introduced.

No commits, pushes or external ticket writes occurred. No task-created cache,
container or image exists to clean. Keep the active worktree/branch and staged
changes for integration; retain the original fixture, transcript and /tmp test
logs as evidence. Shared build caches and other agents' work remain untouched.
No N147 test/build process remains running after vet.


## Parent-requested verification plan, localization and duplicate analysis

Commands executed on staged source at HEAD
`3aa1f1dceb758155f249d6c984c546e088ae40f9`:

- `make verify-list BASE=origin/release/v1.5.2` exited 0. Selected localization,
  ui, llama-libs, build, tests, allocations, conformance and differential; docs
  skipped by path filter. `/tmp/n152-147-verify-list.log`.
- `make verify BASE=origin/release/v1.5.2 ONLY=localization` exited 0. Localization
  passed in 7s: localization package 3.839s, catalog 0.506s, inventory 0.688s;
  generated manifest and catalog checks included. ui, llama-libs, build, tests,
  allocations, conformance, differential and docs were not selected and did not
  run. The wrapper's “All checks passed” applies only to this scoped invocation,
  not the full verification suite. `/tmp/n152-147-verify-localization.log` and
  `.verify/runs/20261121-192304/` preserve logs.
- `scripts/code-intelligence-hook.sh check-staged` exited 1: configured
  localhost:7474 / nornicdbcode analysis/ingestion failed. The hook printed its
  historical mandatory-check warning, but the canonical CONTRIBUTING policy
  (lines 117/119) explicitly permits candidate pushes before full checks, says
  local Codex hooks are not push gates, and treats duplicate analysis as advisory.
  Therefore this is failed/incomplete analysis, not a publication prohibition or
  a clean duplicate result. No rule was bypassed or disabled.
  `/tmp/n152-147-duplicate.log` preserves the exact output.

The verifier fetched the release ref, which is now
`70ea82df61970944f017290e59ec1cea7f2d61fa`; this worktree stayed on 3aa1f1dce.
The plan used merge base 3aa1f1dce and included staged changes. No newer-head
integration proof is claimed. Parent owns commit/rebase and integrated tests.
Verifier fixture checks printed temporary-fixture missing-ci.yml warnings but
passed their own policy assertions; those warnings remain in the raw logs.
An empty task-created `--list` file appeared during these verifier fixture checks;
it was inspected (0 bytes) and removed. Logs and active worktree remain preserved.
