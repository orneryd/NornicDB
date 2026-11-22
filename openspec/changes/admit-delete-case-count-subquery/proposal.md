# Admit COUNT subquery operands in DELETE target expressions (N152-147 / NOR-80)

The bounded correction now reuses canonical scope and context-aware expression
owners for DELETE. The original investigation and recorded reference are retained
below; the follow-up evidence records actual implementation failures and tests.
The parent owns final integration and release qualification.

## Issue references

- Plane **N152-147** — release tracking card for historical Linear **NOR-80** ("Accept COUNT subqueries in
  procedure filters and DELETE expressions"), NOR-80 id `797e64ae-d86c-4f73-bd07-033b539a7bf6`,
  parent card N151-44. Ticket snapshot read from `/tmp/n152-147-ticket.json`.
- Siblings **NOR-79**, **NOR-81**, **NOR-82** are reproduced by the same supplied artifact but
  are **out of scope**: other worktrees own them. Their cells are present only because the
  ticket supplied one combined matrix; nothing in this change corrects or claims them.
- `openspec/config.yaml` names `docs/plans/cypher-convergence-plan.md` and
  `docs/plans/cypher-convergence-issues.md` as the plan owners. **Neither path exists in this
  checkout**, so no convergence plan step can be cited. Recorded as a gap, not invented.
- No earlier artifact for this ticket in this checkout: a tree-wide search for
  `NOR-80|NOR80|nor80|N152-147` outside the recovered fixture returns nothing, so this is the
  first OpenSpec record of the ticket here.

## Established defect (recorded evidence, not produced here)

The ticket records a 12-probe × 2 parsers × auto/explicit × RUN/EXPLAIN/PROFILE matrix
(144 leaf cells) run against NornicDB head `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5`
(60 failing / 84 matching) with Neo4j 5.26.30 community as the reference. For NOR-80 that is
36 cells: the two procedure-filter probes match the reference in all 24 cells, and the DELETE
probe is falsely rejected in all 12 cells (2 parsers × 2 transaction modes × 3 statement kinds).

Rejected statement — reference verdict **accepted** (`WRITE_ONLY` plan, 0 rows):

```
UNWIND [] AS s MATCH (n) DELETE CASE WHEN COUNT { MATCH (m) } > 0 THEN n END
```

Every failing cell fails with the same engine error:

```
Neo.ClientError.Statement.SyntaxError: DELETE expression "CASE WHEN COUNT { MATCH (m) } > 0 THEN n END"
refers to an undefined variable
```

Matching controls (unchanged, no correction claimed):

```
UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { MATCH (m) } > 0 RETURN label
UNWIND [] AS s CALL db.labels() YIELD label WHERE COUNT { CALL { MATCH (m) RETURN m } RETURN m } > 0 RETURN label
```

The recovered matrix, its hash and the exact provenance of every probe are in `evidence.md`
and `evidence/`.

## Causal owner (complete for the recorded rejection)

One heuristic, called from one site, produces the false rejection. The CASE and subquery
owners are not involved.

1. `pkg/cypher/match_semantic_validation.go:623-627` — `semanticScopeWalk`,
   `case pipelineClauseDelete`, calls `validateDeleteClauseScope(mutationScope(), clause.text)`
   before `deleteTargetTypeError` and `validateStaticClauseTypes`, so it is the first DELETE
   check any statement reaches.
2. `pkg/cypher/set_semantic_validation.go:50-64` — `validateDeleteClauseScope`. The
   trailing-word check at line 53 accepts the text (`CASE`, `WHEN`, `THEN`, `ELSE`, `END` are
   operator words to `isExpressionOperatorWord`, and the `COUNT { … }` braces are tracked).
   Line 60 then asks the target's *leading identifier* to be bound.
3. `pkg/cypher/executor_mutations_delete_guard.go:156-175` —
   `deleteExpressionRootIdentifier` returns the leading identifier unless it is a function
   call or `null`/`true`/`false`. For this target it returns `CASE`, which is a keyword, not a
   variable, so `scope.contains("CASE")` is false and `deleteUndefinedVariableError`
   (`set_semantic_validation.go:136-142`) is raised.

Two owners already answer "which variables does this expression read" correctly, so the
failing check is a divergence from the canonical owner, not a missing capability:

- `pkg/cypher/pipeline_mutation.go:23-41` (`pipelineApplyDelete`) scans DELETE targets with
  `expressionFreeVariables` and reports `deleteUndefinedVariableError(name)` for an unbound
  reference — the runtime step whose behaviour the static check says it mirrors.
- `pkg/cypher/set_semantic_validation.go:228-392` (`expressionFreeVariables`) skips CASE
  keywords (`setExpressionKeyword`, line 417) and whole subquery bodies (lines 337-345), so it
  returns `n` — bound — for this target.
- `pkg/cypher/match_semantic_validation.go:785-810` (`undefinedExpressionVariable`) is the
  same pattern for WHERE/UNWIND: a simple identifier/property chain fast path, otherwise
  `expressionFreeVariables`.

Owners after the failing check statically appear to accept the same statement (they were never
reached in the recorded run, so this is analysis, not evidence): `trailingWordAfterOperand`
(`set_semantic_validation.go:67-110`), `deleteTargetTypeError`
(`match_semantic_validation.go:1646-1661`), and `staticDeleteTargetTypeError`
(`executor_mutations_delete_guard.go:351-370`) through `staticOperatorChecker.check` →
`checkCase` (`static_operator_types.go:600`) with `COUNT { … }` typed as Integer by
`staticSubqueryExpressionOperand` (`static_operator_types.go:967-989`).

Neither parser's grammar rejects the shape: `pkg/cypher/query_body_manifest_test.go:71` already
carries `MATCH (n) DELETE CASE WHEN COUNT { CALL { @X@ } } > 0 THEN n END` as the witness for
the `deleteSt` carrier.

## Original proposed minimal correction (accepted with preserved error argument)

In `validateDeleteClauseScope`, replace the leading-identifier question with the canonical
reference scan that the runtime DELETE step already uses:

```go
if missing := firstUndefinedSetExpressionVariable(ctx, expression, scope); missing != "" {
    return deleteUndefinedVariableError(expression)
}
```

`firstUndefinedSetExpressionVariable` (`set_semantic_validation.go:213-221`) is the
`*semanticBindingScope` view of `expressionFreeVariables`, so no new scanner is introduced.
`deleteExpressionRootIdentifier` then has no caller and becomes a deletion candidate, together
with its comment reference at `set_semantic_validation.go:47-49`.

Why this shape:

- It keeps exactly one owner of "variables a DELETE target reads" — the canonical scanner the
  runtime step uses — instead of teaching the heuristic about CASE and every other keyword.
- It preserves the current error owner and detail (`deleteUndefinedVariableError`:
  `Neo.ClientError.Statement.SyntaxError`, `UndefinedVariable`) for the statements that must
  still be rejected.
- It removes the false rejection without weakening any reference requirement: the same
  statement without a bound `n` still fails, and `DELETE null`, `DELETE 1 + 1` and
  `DELETE node:Other` keep their existing behaviour (`null` is a literal keyword;
  arithmetic and label targets carry no free variable and are judged later by the type and
  label owners).

Rejected alternatives: adding CASE (or a keyword list) to the excluded words keeps two owners
of one decision and re-opens the same hole for the next keyword/function-shaped target;
skipping the scope check for "complex" targets defers a compile-time rejection to RUN and
loses the EXPLAIN/PROFILE verdict; dropping the static check entirely contradicts the existing
DELETE negative tests (`pkg/cypher/delete_test.go:141-175`).

Known risks the parent must settle before this is treated as architecture:

- The scan is stricter than the heuristic where a leading root is bound but a nested reference
  is not (e.g. `DELETE n.list[x]`): today that passes admission, Neo4j rejects it, and no test
  in this checkout pins the permissive result. That is a behaviour change inside the same
  causal unit, and it is the one place where "minimal" and "reference-correct" could differ.
- Two message shapes exist for an unbound DELETE target today:
  `pkg/cypher/delete_test.go:148` pins the `UndefinedVariable` detail only, while
  `pkg/cypher/subquery_update_scope_test.go:26-31` asserts the text
  "Variable \`m\` not defined" for `SET`/`REMOVE`/`DETACH DELETE m`. The owner of that second
  text on the top-level `DETACH DELETE` route was not established here (see `evidence.md`,
  "Open verification items"), so the correction must be verified against both tests before the
  message contract is considered unchanged.

## Reuse plan

- Expression-scope decision: `expressionFreeVariables` via
  `firstUndefinedSetExpressionVariable` (`semanticBindingScope`) — the owner the SET/REMOVE and
  runtime DELETE paths use; no second scanner, no keyword list.
- Error: `deleteUndefinedVariableError`, unchanged.
- Fixture: the ticket's own matrix (recovered verbatim in `evidence/`) rather than a new probe
  list; whether the eventual regression lives in that recovered file, extends
  `staticTypeNeo4jVerdicts` (`pkg/cypher/static_types_neo4j_verdicts_test.go:14`, which today has
  no DELETE entry), or joins an existing DELETE test is left to the parent.
- Helpers already present for the recovered file: `forEachParser`
  (`quantified_group_predicates_test.go:211`, default + ANTLR), `newTestMemoryEngine`
  (`test_helpers_test.go:14`), `staticConstructorTypesOracle`/`Population`/`Params`
  (`static_constructor_types_test.go:23,39,27`), `staticTypeNeo4jVerdicts`
  (`static_types_neo4j_verdicts_test.go:14`, holds both NOR-81 controls at lines 17-18).

## Out of scope

- NOR-79, NOR-81, NOR-82 cells and their owners (`static_constructor_types_test.go`, the
  NOR-81/NOR-82 static type entries) — other worktrees own them.
- The procedure-filter probes: matching controls only; no correction is claimed or recorded.
- Plan contents, performance claims, and unrelated broad runtime behavior. Non-empty
  DELETE CASE/COUNT semantics were subsequently included by the parent and verified below.
- Unrecovered populations: NOR-79's "120 generated parameter cases" and NOR-82's "64 inherited
  H2 false accepts" were not supplied inline and have no canonical population in the
  repository; they are therefore not covered and no full-family claim is made.
- Commits, pushes, PRs and external ticket writes remain parent-owned.


## Accepted bounded implementation plan

Parent approved reuse of `firstUndefinedSetExpressionVariable` through the current
context, preserving `deleteUndefinedVariableError(expression)` for the existing
scope-check message. Change the sole semantic-scope call to supply `ctx`; remove
the now-unused root-identifier heuristic and its sole-purpose `cyphertext` import.
No production change is applied before the fail-first run.

The public missing-variable message is owned earlier: `Execute` calls
`admitStatement`, whose binding compilation reports `createUndefinedVariableError`
through `boundCompilation.bindSymbol`/`failUndefined`; its deferred type validation
does not run after a binding error. Valid CASE bindings proceed into the semantic
scope pass, where the existing heuristic incorrectly treats CASE as a variable.
These owners remain distinct error boundaries; their diagnostics will not be
rewritten or unified by this fix.

The focused test uses exactly the three recovered NOR-80 queries (36 cells),
plus non-empty CASE/COUNT deletion controls (selected node, unchosen node,
selected null/else, correlation and CALL-bearing COUNT; both parsers and tx modes)
and missing-reference controls (both parsers, tx modes, RUN/EXPLAIN/PROFILE,
exact upstream message and preserved storage). Non-empty execution is now an
explicit verification requirement, extending the investigation-only scope above.
