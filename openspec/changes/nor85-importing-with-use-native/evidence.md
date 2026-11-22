# NOR-85 evidence

Checkout: `/private/tmp/n152-152-importing-use`, HEAD
`18b36a85d10e775b9d5d3500ecf3482682754a8d` (`fix(linkpredict): stabilize unchanged-graph
top-k ties (N152-47) (#1912)`), clean tree before this delegation.

## 1. Fixture recovery (exact)

- Source: `/tmp/n152-152-ticket.json` (the approved external file), field
  `description_html`, first `<pre><code>` block, HTML-unescaped.
- Result: 13711 bytes, 294 lines, ending with the `TestNOR85ImportingWithUSECompositeRoute`
  closing brace.
- sha256 `9195b583bca79eead8d4ddcf2bb932bcae5b1223d19175c902fcc12da9061626`, byte-identical
  to the sha256 the ticket itself prints for `pkg/cypher/nor85_importing_with_use_test.go`.
  Recovery method: extraction script wrote the decoded block to the worktree path;
  `workspace_read` then reported the same sha256.
- Helper dependencies the fixture uses all exist at this HEAD (static check only, not
  compiled here): `forEachParser` (`pkg/cypher/quantified_group_predicates_test.go:211`,
  runs the default and ANTLR parser via `config.SetParserType`),
  `newCompositeNativeManager` (`pkg/cypher/composite_native_fixture_test.go:12`),
  `testDatabaseManagerAdapter` (`pkg/cypher/composite_commands_test.go:16`),
  `requireStatusCode` (`pkg/cypher/use_clause_grammar_test.go:126`),
  `StatementResultColumns` (`pkg/cypher/result_columns.go:63`), `RawSource`
  (`pkg/cypher/source_stage.go:10`).
- The original audit's fixture was reported unavailable; this is the ticket's inline copy,
  which the ticket's own sha256 authenticates.

## 2. Failing baseline (from the ticket, not re-run here)

Recorded in the ticket body (macOS combined focused run RED 1.122s; Buttocks Docker Linux
RED 0.260s, exit 1, not OOM), command:
`GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher
-run '^TestNOR85' -count=1 -timeout=3m -v`.

- `nornic` and `antlr`: 8 failing required-behavior subtests each (`reproduction`,
  `self_contained_importing_with`, `multi_atom_importing_with`,
  `boundary_routes_the_remainder`, `quoted_database_name`, `quoted_variable_named_use`,
  `dynamic_string_reference_keeps_its_contract`, `unknown_database`).
- 5 controls per parser pass (`control_without_use`, `control_leading_use`,
  `control_leading_use_dynamic_reference`, `control_leading_use_unknown_database`,
  `control_missing_variable`); all 6 composite-route subtests per parser pass.
- Representative messages: `Neo.ClientError.Statement.SyntaxError: query could not be
  planned as a clause pipeline`; the dynamic-reference subtest shows
  `"…query could not be planned as a clause pipeline" does not contain "Dynamic graph lookup
  not allowed here"`; the unknown-database subtest shows expected
  `Neo.ClientError.Database.DatabaseNotFound`, actual
  `Neo.ClientError.Statement.SyntaxError`.

## 3. Source evidence for the causal owner (read, not run)

- `pkg/cypher/executor_query_routing.go:161-163` — a top-level CALL subquery routes to
  `executeRequiredPipeline`; `:342` — a `WITH` statement does the same; `:371-375` — the
  decline is converted to `query could not be planned as a clause pipeline`.
- `pkg/cypher/pipeline_call_operator.go:77` — the body's `useStart >= 0` opens the USE
  branch; `:81` — `parseUseClause(ctx, text(), true, cyphertext.AcceptedSource)`;
  `:82-104` — rewrite only when `hasUse`; `:177-179` — plan the body text, or the rewritten
  text when `useBody`; `:181-183` — decline when `ok` is false.
- `pkg/fabric/use_clause.go:87`/`:96`/`:120` — `ParseUseClause`/`ReadUseClause` return
  `hasUse=false` unless the text starts with `USE`; the leading-only contract is documented
  in the function comment.
- `pkg/cypher/pipeline_executor.go:559` — `parsePipelineClauseBranch` returns a declined
  split for `branch.useStart >= 0`; `:1295-1302` — a CALL-subquery decline becomes
  `pipelineDecline`; `pkg/cypher/pipeline_dispatch.go:37` — `terminal()` is false for
  `pipelineDispatchNotApplicable`.
- `pkg/cypher/antlr/CypherParser.g4:285-286` and `:364` — the grammar admits the USE clause
  after a `withSt` in `subqueryBody` through `readingStatement`, which is why the statement
  parses and only execution fails.
- Canonical owner: `pkg/fabric/planner.go:760-792` (`parseLeadingWithUse`),
  `pkg/fabric/executor.go:1936-1982` (`findLeadingWithClauseEnd`, `nextTopLevelClause`),
  call sites `planner.go:438-441`, `:727-731`, `:1064-1073`, pinned by
  `pkg/fabric/planner_use_coverage_test.go:105-147` and `pkg/fabric/planner_test.go:1670-1690`.
- Header path accepts the USE clause in either position:
  `pkg/cypher/result_columns_compile.go:207-216`.
- Reader the native executor already installs for clause-keyword names:
  `pkg/cypher/executor_fabric.go:528`.
- No test in `pkg/` pins the current message `query could not be planned as a clause
  pipeline`, so the correction does not contradict a checked-in expectation.

## 4. Graph availability (honest status)

The supplied retrieval receipt is `status: incomplete`: `/nornicdb/search` timed out twice,
`/db/nornicdbcode/tx/commit` timed out, `code_count: 0`, `code_baseline: []`,
`docs_generation: 5587e249c0764474b3cc825192604b26`, head `18b36a8…`. No graph-derived claim
is made anywhere in this change; every owner/reuse statement above comes from reading the
current checkout. No reingestion was requested or attempted.

## 5. What was not executed

No production edits, no `go build`/`go vet`/`go test`/benchmark, no `make` target, no mesh
or network client run, no commits, stashes, pushes or PRs, no Linear edits, no delegation.
All commands in `tasks.md` are unexecuted, and the fixture is untracked in the worktree.
The parent is running CPU-sensitive checks; the only writes here are the fixture test file
and this change directory. The ticket's historical "no production change" audit scope is
not the current operator decision; this delegation is investigation-only either way.

## Current implementation and fresh baseline

Parent approved convergence onto the existing importing-WITH reader. The new
`fabric.ParseAcceptedSubqueryUseClause` explicitly consumes accepted source and
composes `parseLeadingUse` with `parseLeadingWithUse`; it does not introduce a
parser or normalize the body again. All three Fabric inline compositions now
use this entry. The native CALL pipeline uses it only for a detected body USE,
keeping imports in the remaining body. Top-level parsing stays unchanged.
`executor_use.go` retains one shared `UseSyntaxError` translation.

Manual duplicate review: inspected the native pipeline consumer, all three Fabric
composition sites, `parseLeadingWithUse`, and `ParseUseClause` at base
18b36a85d10e775b9d5d3500ecf3482682754a8d. The overlap is graph selection after
importing WITH; native previously called only the leading reader. Reuse now
converges the selection contract; planner-specific wrapping and executor-specific
status mapping remain with their existing owners. Advisory checker/graph results
are not claimed here; parent owns that integration check.

Fresh fail-first command before production edits:
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^TestNOR85ImportingWithUSE' -count=1 -timeout=4m -v`
exited 1, package 1.125s; log `/tmp/n152-152-release-red.log`.
All 16 required native importing-WITH leaves failed (8 per parser); all 10 native
control leaves and all 12 composite leaves passed. This is fresh local evidence,
separate from the historical ticket run above. Production edits followed this run.

Green and race results are recorded below. No full-suite,
coverage, performance, or canonical verification claim is made.

The recovered original test is retained byte-exact. Its header says the shared
grammar admits only leading USE, but current `readingStatement` includes
`useClause`, so that explanatory claim is incomplete. The defect is native routing,
not grammar admission. No Neo4j reference run or new parity claim is included.

## Focused current-source green

`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/fabric ./pkg/cypher -run 'TestParseAcceptedSubqueryUseClause|TestParseLeadingWithUse|TestPlan_.*Use|TestUse|TestSubqueryUseClauseGrammar|TestDynamicGraphLookupOutsideComposite|TestCallSubquery|TestNOR85' -count=1 -timeout=5m -v`
passed: Fabric 0.830s, Cypher 5.256s. Log `/tmp/n152-152-green.log`.
The original fixture passes all 38 leaves across both parsers (26 native,
12 composite), including exact rows/headers, selected graph, quoted imports,
dynamic-reference and unknown-graph statuses, and missing-variable controls.
The 8 new shared-entry cases pass, including cancellation and typed syntax errors.
Existing source-stage and leading USE tests in this focused selection also pass.
This selection is not the full package suites.

## Focused current-source race and handoff

`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -race -p=1 -tags=noui,nolocalllm ./pkg/fabric ./pkg/cypher -run 'TestParseAcceptedSubqueryUseClause|TestPlan_DynamicUseSubqueryPositions|TestNOR85ImportingWithUSE|TestSubqueryUseClauseGrammar|TestDynamicGraphLookupOutsideComposite' -count=1 -timeout=5m -v`
passed: Fabric 1.937s, Cypher 3.645s. Log `/tmp/n152-152-race.log`.
Production/test source was unchanged between green and race. All changed Go files
were formatted; `git diff --check` passed. No additional build or dedicated vet
was launched after race, preserving the parent's scheduled profiling window.

Parent owns canonical `make verify-list` / `make verify`, advisory duplicate
checker results, broader qualification, staging and PR publication. Those checks
are not represented as passed by these focused runs. No benchmark or performance
claim is made for this correctness convergence.

## Parent review checks

Parent independently reviewed the native entry, shared composition, all three Fabric
consumers and original fixture. Dedicated bounded `go vet -p=1 -tags=noui,nolocalllm
./pkg/fabric ./pkg/cypher` passed. `make verify-list BASE=origin/release/v1.5.2`
completed; `make verify BASE=origin/release/v1.5.2 ONLY=localization` passed. This
scoped invocation does not qualify the full verifier: other checks were skipped.
The staged advisory duplicate checker ran and reported a code-graph transaction
timeout; indexed discovery remains incomplete, not clean. Logs are preserved
under `/tmp/n152-152-{vet,verify-list,verify,duplicate}.log`.

The release base subsequently advanced to 6f52ed1187fd9061edf5eb59d15d06fe59ebe938
with CDC docs and replication changes. This branch's tested base remains
18b36a85d10e775b9d5d3500ecf3482682754a8d; combined release qualification is pending.
