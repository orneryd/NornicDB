# Keep USE separate from an importing WITH in a native CALL body (NOR-85)

Status: investigation and extraction only. No production change is proposed here as
done work: this document records the verified causal owner and the minimal convergence
for the parent to accept and implement.

## Issue and required result

- Issue: **NOR-85** (`NOR-85 · Keep USE separate from an importing WITH in a native CALL
  body`), Linear sequence 152, priority medium, release tracking card for N151-49.
- Required product result (fixture `pkg/cypher/nor85_importing_with_use_test.go`): on a
  real `multidb.DatabaseManager` session whose database is `neo4j`,
  `WITH 1 AS v CALL { WITH v USE neo4j RETURN v AS x } RETURN x` returns columns
  `["x"]` and rows `[[int64(1)]]`, agreeing with `StatementResultColumns` header facts and
  `EXPLAIN`, plus the seven sibling required behaviors (self-contained importing WITH,
  multi-atom import list, routing boundary, quoted graph, quoted variable named `USE`,
  dynamic reference contract, unknown graph).
- Classification: intentional **NornicDB extension** already implemented by the Fabric
  route (`pkg/fabric/planner.go` `parseLeadingWithUse`, `SingleGraphStatement`). No new
  Neo4j syntax claim, no new grammar policy, no second parser.

## Verified causal chain (HEAD `18b36a85d10e775b9d5d3500ecf3482682754a8d`)

1. Routing: `WITH …` enters `executeRequiredPipeline` (`pkg/cypher/executor_query_routing.go:342`,
   error at `:371-375`). A standalone `CALL { … } RETURN x` reaches the same route through
   `firstTopLevelCallSubquery` (`pkg/cypher/executor_query_routing.go:161-163`).
2. The pipeline reaches the CALL-subquery operator, which detects a USE-bearing body with
   `body.branches[0].useStart >= 0` (`pkg/cypher/pipeline_call_operator.go:77`). That is
   true for `WITH v USE neo4j …` because the shared grammar admits USE there: `subqueryBody`
   allows `(readingStatement | updatingStatement | withSt)*`
   (`pkg/cypher/antlr/CypherParser.g4:285-286`) and `readingStatement` itself admits
   `useClause` (`CypherParser.g4:364`). Parse/header/EXPLAIN therefore succeed.
3. The operator's only admission is `parseUseClause(ctx, text(), true, AcceptedSource)`
   (`pipeline_call_operator.go:81`) → `fabric.ParseUseClause`
   (`pkg/fabric/use_clause.go:87`, `ReadUseClause` at `:96`), which is strictly **leading**:
   `if !startsWithWordFold(trimmed, "USE")` returns `hasUse=false` (`use_clause.go:120`).
4. With `hasUse=false` the body is not rewritten: `bodyText`, `importBody` and `useBody`
   keep the original text (`pipeline_call_operator.go:82-104`, used at `:177-179`).
5. `run()` then plans the *original* body text; `parsePipelineClauseBranch` declines every
   branch with `useStart >= 0` (`pkg/cypher/pipeline_executor.go:559`), so
   `pipelineClausesForBody` yields `ok=false` and the operator returns a decline
   (`pipeline_call_operator.go:181-183`).
6. The decline returns `pipelineDecline` (`pipeline_executor.go:1300-1302`), the dispatch
   outcome is not terminal, and `executeRequiredPipeline` raises
   `Neo.ClientError.Statement.SyntaxError: query could not be planned as a clause pipeline`
   (`executor_query_routing.go:375`).

That chain reproduces the ticket's asymmetry exactly: header facts and EXPLAIN never enter
this operator, and the composite route is planned by `pkg/fabric` instead. **Causal owner:
the native CALL-body USE admission in `pkg/cypher/pipeline_call_operator.go`, which
implements only one of the two admitted USE positions while the canonical owner in
`pkg/fabric` implements both.**

## Canonical owner and reuse plan

Owner of where a CALL body's USE may sit, and of the rewritten body: `pkg/fabric`.

- `parseLeadingUse` + `parseLeadingWithUse` (`pkg/fabric/planner.go:760-792`), using
  `findLeadingWithClauseEnd` / `nextTopLevelClause` (`pkg/fabric/executor.go:1936-1982`),
  which skip quoted text and comments through `cyphertext.SkipToken`.
- Existing two-step sequences: leading USE first, then importing WITH+USE —
  `planCallBranch` (`planner.go:727-731`), `callBlockContainsFabricUse` (`planner.go:1064-1073`),
  `singleGraphStatement` in a subquery (`planner.go:438-441`).
- Behavior already pinned by `pkg/fabric/planner_use_coverage_test.go:105-147`
  (`TestPlan_DynamicUseInImportingCallSubquery`: body rewritten to
  `WITH g MATCH (n) RETURN count(n) AS c`, dynamic USE resolved from the imported row) and
  `pkg/fabric/planner_test.go:1670-1690`.

Minimal convergence (one owner, no new parser, no grammar change):

1. `pkg/fabric/planner.go`: add one exported composition
   `ParseAcceptedSubqueryUseClause(ctx context.Context, body string, isName ClauseKeywordNameReader)
   (UseClause, string, bool, error)` — explicitly consuming accepted source,
   `parseLeadingUse(…, inSubquery=true)`, then, only when `!hasUse` and no error,
   `parseLeadingWithUse`. It is the single decision point for a CALL body's admitted USE
   positions.
2. `pkg/fabric/planner.go`: replace the three identical inline sequences (`:438-441`,
   `:727-731`, `:1064-1073`) with that one call. Behavior is identical; the duplicate
   decisions disappear.
3. `pkg/cypher/executor_use.go`: add `parseAcceptedSubqueryUseClause(ctx context.Context, body string)` beside
   `parseUseClause`, passing accepted body text and the
   executor's existing reader `clauseKeywordUsedAsName` (installed at
   `pkg/cypher/executor_fabric.go:528`); map `*fabric.UseSyntaxError` with the same
   `localizedStatusError` `parseUseClause` uses, factored into one shared helper so the
   mapping is not duplicated.
4. `pkg/cypher/pipeline_call_operator.go:81`: call that wrapper instead of
   `parseUseClause`. The rest of the operator is unchanged: `dynamicUseError` (`:89`),
   `useDatabase` (`:92`), the `bodyText`/`importBody`/`useBody` reassignment (`:95-99`) and
   `pipelineClausesFor(runCtx, text())` (`:179`).
5. Untouched: `parseLeadingUseClause` and every top-level statement USE call site
   (`executor_use.go:16-21`, `executor_entry.go:514/703/721`,
   `executor_internal.go:140`, `executor_fabric.go:235/456`), and
   `parsePipelineClauseBranch`'s `useStart >= 0` decline rule.

### Why each required behavior is preserved

- **Quoted `USE` variables and quoted graph names**: the canonical scanner treats backtick
  and quoted text as opaque tokens (`cyphertext.SkipToken` → `IsQuote`), and the
  clause-keyword boundary consults `clauseKeywordUsedAsName`.
- **Import boundary**: the boundary is the end of the importing WITH clause
  (`findLeadingWithClauseEnd`), so `WITH v, 2 AS w` and a later `RETURN v + w AS x` are not
  touched; the rewrite keeps the WITH and removes only USE (`planner.go:786-791`), which is
  the body `importBody`/`unscopedCallImports` read for imports.
- **Selected graph**: the rewritten remainder runs on the executor that `useDatabase`
  returns, the same mechanism the leading-USE controls already use, so the `other` marker
  case routes to `other`.
- **Dynamic reference / unknown graph**: `dynamicUseError` and `useDatabase` run before
  planning, so `Dynamic graph lookup not allowed here`
  (`Neo.ClientError.Statement.SyntaxError`) and `Graph not found: nosuch`
  (`Neo.ClientError.Database.DatabaseNotFound`) replace the planning error exactly as they
  already do in the leading position.
- **Leading USE and no-USE bodies**: unchanged paths.
- **USE elsewhere in a body** (e.g. `CALL { MATCH (n) USE other … }`): both readers decline,
  so such a body keeps declining exactly as today. The change admits no new position.

## Non-goals

No grammar change, no second parser or lexer, no new or changed error text/status code, no
change to composite/Fabric routing, no remote constituents, no new Neo4j parity claim, no
refactoring outside the converging call sites.

## Limits

The initial investigation was read-only. Implementation and fresh verification evidence
are recorded separately in evidence.md and tasks.md.
`openspec/config.yaml` names `docs/plans/cypher-convergence-plan.md`, which does not exist
at this HEAD, so no convergence-plan step ID can be cited; the package comments listed
above are the current owner documentation. Parent owns architecture acceptance.
