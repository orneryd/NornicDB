# fix145: finish #1385 and #1408 for 1.4.5 (Claude Opus 5.5 implementation agent, 18 Oct 2026)

Task: rebase both onto origin/develop, fix open findings, tests (both parsers), gofmt/vet, A/B vs develop (<=5%), mark ready, update bodies. No merge (Codex review first, arranged by the orchestrator).
Base worktree for red checks and A/B: `wt-145-devbase` (detached origin/develop e13815677). Logs: `scratch145/`.

## #1385 (wt-1317-s8, fix/1317-call-import-reader)
- Old head 35630a6dc (local backup branch `backup/1385-35630a6dc`).
- Rebased onto develop e13815677. Conflicts: executor_subqueries.go (develop moved parseIntModifier / parseOrderBy* to result_modifiers.go; PR version kept minus those), gen_sweep.py (kept both families).
- Also removed: `withImportClauseKeywords` (dead after the deletion; the one test that read it keeps the list locally); stale comments naming deleted helpers.
- WIP commits replaced by: 1e67a8031 fix + deletion + tests; d7bbf6ac7 sweep family.
- Jobs: (pending)
- Build fix: subquery_helper_coverage_test.go deleted (nothing left in it), unused "strings" import in subquery_test.go (first full run j20261018005804-9112-0f25 failed to build). vet on the fixed tree: i20261018005839-56548-d5af exit 0.
- Head now c5ef3fb76 (fix + deletion + tests) / 81fd459e3 (sweep family).
- Red: TestUnscopedCallImportsIgnoreNestedSubqueryNames on develop e13815677: i20261018005802-51876-4c26, 3 nested cases FAIL (got 0, want 1), 2 controls pass.
- Neo4j 5.26.30 confirms the expected values: j20261018005951-64360-5d15.
- Full pkg/cypher both parsers on 81fd459e3: running (log scratch145/1385-full.log).

## #1408 (wt-gdssim, fix/f-gds-sim-route)
- Old head a35fab31c (local backup branch `backup/1408-a35fab31c`).
- Rebased with `--onto origin/develop d2f2dc77e`: dropped the 4 #1386 commits (git cherry: patch-equivalent in develop). Remaining 4 replayed cleanly: bc9e5beed, c4d12e596, 69ddb0eab, 8bd3c9d98.
- P2 (r4301112032, remote key "\x00remote\x00"+elementId collides with a NUL-containing stored id): fixed in 9c69e6438. gdsNodeRef keeps the raw id; key() is injective ("\x00R"+elementId for remote, "\x00L"+id for a local id starting with NUL, else the id); gdsShownKey reverses it. Storage/projection callers use ref.id.
- Red: TestGDSNodeKeysKeepLocalAndRemoteApart on the pre-fix head 8bd3c9d98: i20261018010157-92464-e018 FAIL (2 rows, shown id "x") as Codex described.
- Focused (205 tests from the gds*/similarity/linkprediction/louvain/fastrp/procedure_registry test files) both parsers on 9c69e6438: running (scratch145/1408-focused.log; regex in scratch145/1408-run.txt).

## Progress 01:10Z
- #1385 pushed 81fd459e3 (force-with-lease over 35630a6dc). gofmt clean i20261018010737-73024-f858.
- #1385 full pkg/cypher both parsers j20261018005943-58248-9608: everything passes except TestEvaluateWhereForContext_Branches (line 48, "(a)-[:KNOWS]->(c)" with c unbound returns true) on both parsers. Same failure on develop e13815677: i20261018010932-105508-7a41. Develop baseline, not from this PR; not tracked in any open PR (reported to orchestrator).
- #1408: a test read ref.key as a field (gds_aggregation_values_test.go:169), fixed to ref.id in b8e580744; gofmt of the PR's own lines in 091b82e5c. Pushed 091b82e5c (force-with-lease over a35fab31c).
- #1408 focused both parsers on 091b82e5c: i20261018010747-58028-fa1f PASS. vet i20261018010650-94928-28b4 exit 0; gofmt i20261018010727-106284-a046 clean.
- A/B queued: #1408 (scratch145/1408-ab.log), #1385 (scratch145/1385-ab.log), base wt-145-devbase.

## 01:25Z
- T3 restart killed the local A/B clients; the jobs stayed queued on the box: #1408 a20261018010914-90456-ae36, #1385 a20261018011038-97756-129f (bench lane: 16 A/B queued at 01:20Z). Re-attached via `tnrun.sh result` → scratch145/*-ab-result.log.
- PR body drafts: scratch145/1385-body.md, scratch145/1408-body.md (A/B section pending).

## 04:15Z
- #1408 A/B a20261018010914-90456-ae36 (10 rounds, base develop e13815677, head 091b82e5c) done. Table: scratch145/1408-ab-table.txt, p-values (Mann-Whitney, own awk: scratch145/mwu.sh) scratch145/1408-ab-p.txt. B/op and allocs/op identical on every row. ns/op: all within noise except NodeSimilarityKernel/COSINE/explain=true +7.7% p=0.041 (head spread 97%) and GDSLinkPrediction/commonNeighbors +11.7% p=0.096 (spread 26%/45%). The kernel and its benchmark are untouched by the PR. 20-round confirmation queued (scratch145/1408-ab2-result.log).
- PR bodies updated with all results so far (A/B pending); #1385 title widened. Replied on the #1408 P2 thread (r4305582150). A first reply with a wrong commit hash (r4305581745) was posted by mistake and deleted within ~1 min.
- Develop TestEvaluateWhereForContext_Branches failure: already traced in sig/handoff/reviews/sol-1458-1459-1460-r1.md to #1459's develop original (stale internal assertion).

## 05:05Z
- #1385 A/B a20261018011038-97756-129f (10 rounds, head 81fd459e3): B/op, allocs/op unchanged. ns/op: only CallSubqueryClausePlan/count_return significant (+3.97%, p=0.049, below 5%; this query has no CALL). Medians >5% without significance: CallSubqueryClausePlan scoped_traversal_pushdown +9.98% (p=0.11), traversal_pushdown +5.62% (p=0.13), limit_body +5.56% (p=0.07), MapPropertySubscript map_property +7.51% (p=0.11). None of these reaches the changed code (scoped CALL / importing WITH skip the new mask). Tables: scratch145/1385-ab-table.txt, -p.txt.
- No existing benchmark ran the new path, so added BenchmarkUnscopedCallImplicitImport (plain / nested_exists) in 6ac2637a1 (pushed). A/B base for it: wt-145-devbase-bench = local branch tmp/145-base-callimport-bench (develop + the bench file only, never pushed).
- #1408 confirmation a20261018041406-98140-f190 has a bad -bench pattern (slash inside parens: Go doesn't split it) and will match nothing; `admin term-stale` doesn't know A/B ids. Resubmit with the split pattern after it clears.
- #1385 confirmation (20 rounds, new bench + the 4 drifting rows + count_return): scratch145/1385-ab2-result.log.

## 06:35Z
- #1385 confirmation a20261018045718-87272-de63 (20 rounds, base wt-145-devbase-bench 6757809a6, head 6ac2637a1): all rows within 5% (max +3.04%); map_property +2.55% p=0.04; new path UnscopedCallImplicitImport plain −1.46%, nested_exists +2.19% (p=0.08). B/op, allocs/op identical. Tables scratch145/1385-ab2-table.txt / -p.txt.
- Observation (pre-existing, same on develop): unscoped implicit-import CALL over 400 rows ≈4.7 s/op (nested EXISTS ≈43 s/op) vs ≈16 ms for the scoped form. Perf family material (#911/#1230), not this PR.
- gofmt new bench file i20261018063158-6072-7082 clean.
- #1385 body finalized, marked READY at head 6ac2637a1; mergeable. Milestone is v1.4.4 (no v1.4.5 milestone exists; left for the orchestrator).
- #1408 corrected 20-round confirmation submitted (scratch145/1408-ab3-result.log).

## 07:15Z
- Codex bot on #1408 (091b82e5c): completed, no findings.
- Codex bot on #1385 (6ac2637a1): P1 r4306022915: masking whole nested bodies dropped free references (`CALL { RETURN COLLECT { RETURN n.id } AS xs }`). Fixed in 2b5378942: unscopedBodyReadsVariable (executor_subqueries.go, next to branchDeclaresVariable) decides per nested subquery, recursively; pipeline_call_operator uses it. Tests: +3 cases. Red on 6ac2637a1 i20261018071242-53132-9225 (exactly the 2 read cases fail). Neo4j doubly-nested case j20261018065151-95280-b92f = 1/1. Full pkg/cypher both parsers j20261018070128-100304-e627: only the develop-baseline failure. gofmt i20261018070135-57564-ff85. Pushed; replied r4306135418. (Auto-sharding failed "shard 2 regex too long" on i20261018065955-28356-e18b; reran with --shards 1.)
- A/B of the new path on 2b5378942 queued: a20261018071207-57108-7b3f. #1408 confirmation still queued: a20261018063153-28128-51fb (dud a20261018041406-98140-f190 matches no benchmark).
- Pairwise merge-tree vs open develop PRs (07:25Z): #1385 and #1408 both conflict with #1421/#1423 (those already conflict with develop). #1385 also conflicts with #1382 (edits resolveCorrelatedImportValue, which #1385 deletes as dead: drop that hunk) and #1391 (edits parseCallSubquery next to deleted helpers: adjacency only). Resolution falls to whichever lands second.

## 10:30Z
- #1408 20-round confirmation a20261018063153-28128-51fb: commonNeighbors −0.80% (p=0.83), kernel COSINE explain=true +2.15% (p=0.42), explain=false −0.69%. No regression. Body updated. #1408 DONE (ready, head 091b82e5c, bot review clean).
- #1385: waiting only for a20261018071207-57108-7b3f (new path on 2b5378942).

## FINAL STATE (11:05Z, stopped by orchestrator)
- #1385: READY, head 2b5378942 (origin/fix/1317-call-import-reader), mergeable. Bot P1 fixed + replied. Full pkg/cypher both parsers j20261018070128-100304-e627 (only develop-baseline TestEvaluateWhereForContext_Branches). A/B runs 1+2 no >5% regression.
  PENDING: A/B a20261018071207-57108-7b3f (new path on 2b5378942, running on bench lane at 10:52Z). Fetch: `bash sig/tnrun.sh result a20261018071207-57108-7b3f --raw`; tables via `bash scratch145/abtable.sh sig/tnrun-results/ab-a20261018071207-57108-7b3f` and `scratch145/mwu.sh <dir> ns/op`; replace the "Run 3" line in the PR body (scratch145/1385-body-v2.md).
- #1408: DONE. READY, head 091b82e5c (origin/fix/f-gds-sim-route), mergeable, bot review clean, A/B + 20-round confirmation no regression.
- Open items for orchestrator: no v1.4.5 milestone (both on v1.4.4); #1385 conflicts with #1382/#1391 (adjacency to deleted code); develop baseline TestEvaluateWhereForContext_Branches (traced in sol-1458-1459-1460-r1.md); pre-existing perf: unscoped implicit-import CALL ~4.7 s/op vs ~16 ms scoped.
- Local leftovers: worktrees wt-145-devbase, wt-145-devbase-bench (local branch tmp/145-base-callimport-bench, never pushed), backup branches backup/1385-35630a6dc, backup/1408-a35fab31c. Remove once both PRs merge.
- 11:10Z: a20261018071207-57108-7b3f finished (no result dir written; tables built from the client log, scratch145/ab-7b3f/, scratch145/1385-ab3-table.txt / -p.txt): plain +4.63% p=0.36, nested_exists +5.72% p=0.08 (not significant; allocs/B identical; foreign CPU up to ~16%). Attached to the #1385 body. A 20-round re-run of nested_exists would settle the >5% median; not started (orchestrator: no new work).

# Rule 73 family analysis (18 Oct, 12:30Z): no production code changed

Trigger: #1385 (bot P1, then Sol r1 F1) and #1408 (bot P2, then Sol r1 F2), two failed correction attempts each. Review: sig/handoff/reviews/sol-1385-1408-r1.md.

- **Probe worktrees** (untracked test-only probes, kept as future regression tests): wt-145-r73-1385 (= #1385 head 2b5378942), wt-145-r73-1408 (= #1408 head 091b82e5c), wt-145-devbase (develop e13815677). Probe data and logs: scratch145/r73/.
- **Graphify** (local shared container, own roots):
  - repos C:/Users/mm0ns/Desktop/codex/builds/repos/nornic-wt-145-r73-{1385,1408}[-src];
  - graphs /graphs/nornic-wt-145-r73-{1385,1408}-src (code-only, pkg/cypher + pkg/fabric non-test, 6796 / 6851 nodes);
  - query script <repo>-src/zz_query.py; outputs scratch145/r73/graphify-{1385,1408}.txt.
  - A first graphify run on TrueNAS (j20261018114058-95032-184c) was the wrong place and is superseded. I stopped my own whole-repo local extract, which ran without --no-cluster.
  - Graphify's call graph misses calls made through dispatch tables and method values (e.g. semanticScopeWalk→callSubqueryReturnTypes, p.nodeIDValue). Every member below is also confirmed by grep.

## Family A (#1385): which outer variables a CALL body or subquery body reads, and which it declares

### A.1 Inventory (commands)
- **grep:** `git grep -n -E "^func (\([^)]*\) )?(isIdentifierReferenced|unscopedCallImports|branchDeclaresVariable|unscopedBranchLocals|unscopedBodyReadsVariable|maskSubqueryBodies|maskNestedScopeBodies|expressionFreeVariables|semanticScopeWalk|localBindingAt|localReader|expressionLocalBindings|firstVariableOccurrence|referencedIn|pipelineReadsImportedBinding|extractWithImports|containsStandaloneIdentifier)\b" -- pkg/ ':!*_test.go'`, then the call sites of each name.
- **graphify seeds:** the names above plus branchLeadingWithImport, leadingWithUndefinedVariable, semanticFreeReferences(In), callSubqueryImports, callSubqueryReturnTypes, readCallSubqueryScope, newLocalReader, bindLeadingImportColumns, rewriteLeadingWithImports, stripLeadingWithImportsForFabricRecord, importColumnsFromFragment, validateSubqueryPatternBindings, pipelineApplyCallSubquery(WithMetadata).
  - For each: callers, reachability from Execute/executeInternal/Fabric entry points, and a name scan for /import|freevar|freeref|referenc|declares|locals|scopewalk/ (duplicate candidates).
  - The name scan found readers grep had missed: referencesVariable / pipelineVariableReferences / referencesVariableFor, referencesTraversalVariable, referencesUnwindBinding, pipeline_imports.go, carryStaticSubqueryImports, cypher5ImportGrouping, and Fabric's isSimpleWithImportClause / sanitizeOtherWhereForImportColumn / mergeImportColumns.
- **ANTLR (pkg/cypher/antlr):** no scope or import decision of its own; its symbol list is used only by DELETE (ast_builder.go:576). Both parsers share every member below.

Readers that answer "does text T read variable X" (the split owner). [R] = an earlier review looked at it; [new] = nobody had.

| # | Reader | Where | Method | Consumers |
|---|---|---|---|---|
| A1 | isIdentifierReferenced [R] | call.go:1380 | whole-word scan: counts property keys, map keys, strings, comprehension locals | runtime imports via A3; semantic_validation.go:267 leading-WITH outer set [new]; pipeline_executor.go:1107 shortcut gate [new]; call.go:344, query_info.go:505 (other contracts) |
| A2 | branchDeclaresVariable + firstVariableOccurrence [R] | executor_subqueries.go:359, :422 | first occurrence in a clause list; a UNION query is treated as one branch | unscopedBranchLocals (runtime); A3; static callSubqueryReturnTypes static_type_scope.go:503 [new] |
| A3 | unscopedBodyReadsVariable (this PR) [R] | executor_subqueries.go:387 | A1 on masked text + A2 per nested subquery, recursive | pipeline_call_operator.go:105 |
| A4 | maskSubqueryBodies (develop's runtime path) [R] | match_semantic_validation.go:883 | blanks EXISTS/COUNT/COLLECT bodies | A3; 6 validation callers (other contracts) |
| A5 | expressionFreeVariables + localReader/localBindingAt [R, named as owner] | set_semantic_validation.go:311; expression_validation.go:209, :621 | expression-level free variables; handles keys, strings and comprehension/quantifier/reduce locals; does NOT walk clauses or UNION branches | branchLeadingWithImport executor_subqueries.go:323; leadingWithUndefinedVariable semantic_validation.go:340; 6 validation callers |
| A6 | semanticScopeWalk [R, named as owner] | match_semantic_validation.go:123 | clause walk with bindings per clause, per UNION branch (:937), nested subqueries via validateSubqueryPatternBindings (:917); used only for validation and static types | :80, :947; callSubqueryReturnTypes static_type_scope.go:508 |
| A7 | semanticFreeReferences(In) [new] | return_order_semantic_validation.go:404, :412 | free references of RETURN/ORDER BY | aggregation/order validation (other contract, same question) |
| A8 | referencesVariable / pipelineVariableReferences / referencesVariableFor [new] | pipeline_value_helpers.go:19; pipeline_clause_plan.go:185-227 | sentinel-replace scan outside strings and property positions | call.go:1536, pipeline_match_product.go:38, pipeline_traversal_match.go:152, quantified_path_pattern.go:215, shortest_path_match.go:122, pipeline_executor.go:1737/1746 (pushdown; other contract) |
| A9 | referencesTraversalVariable, referencesUnwindBinding [new] | traversal.go:789; unwind_multi_match_create.go:543 | ad-hoc scans | traversal/unwind fast paths (other contract) |
| A10 | Fabric extractWithImports + isWithImportStopKeyword [R] | fabric/planner.go:1016, :1053 | tokenizes the leading WITH; accepts `AS` and `WHERE` | planMultiGraph :616/:622 → bindLeadingImportColumns :735 |
| A11 | Fabric isSimpleWithImportClause, rewriteLeadingWithImports, importColumnsFromFragment, mergeImportColumns [new] | fabric/executor.go:3175, :3131, :3012, :3090 | own leading-WITH parsing and import rewrite | rewriteFragmentWith(Runtime)Imports :3032/:3061 |
| A12 | Fabric containsStandaloneIdentifier in sanitizeOtherWhereForImportColumn [new] | fabric/executor.go:2033, :1835 | standalone-word scan of the import column | batched-lookup fast paths :609/:841/:1034/:1271 (executeApply → tryExecuteApplyBatchedLookupRowsIter) |
| A13 | stripLeadingWithImportsForFabricRecord [R] | cypher/executor_fabric.go:690 | strips a leading WITH when record bindings exist | ExecuteQueryWithRecord :657 |

Decision points that consume those answers:
- runtime implicit import (pipeline_call_operator.go:85-110) and runtime branch locals (:119-135);
- runtime import-scope carry (pipeline_imports.go; withPipelineImports :195; carryPipelineRowImports pipeline_projection.go:218, pipeline_row_stream.go:100);
- static CALL types (static_type_scope.go:444-525; carryStaticSubqueryImports match_semantic_validation.go:356);
- leading-WITH validation (semantic_validation.go:238-300);
- expression-subquery import grouping (cypher5ImportGrouping subquery_value_expression.go:335);
- the shortcut gate (pipeline_executor.go:560/1101);
- Fabric record bindings (every outer column; executor_fabric.go:657/690).

Routes covered: auto-commit, explicit transaction, CALL IN TRANSACTIONS, Fabric (composite); both parsers.

### A.2 Self-attack (TrueNAS, both parsers)
**Oracle.** Neo4j 5.26.30 per case: `CALL ()` for a body that doesn't read n, `CALL (n)` for one that does. A wrong read/declare label would make Neo4j reject the `CALL ()` form; none did.
- Jobs: j20261018113100-73256-fd91; j20261018113121-69456-6ad0 (stopped at my invalid R14); j20261018113152-80464-5c0f; j20261018113222-39940-61be; j20261018121711-90528-7379.
- Cases: scratch145/r73/cases.tsv (51); answers: expected.tsv.

**NornicDB probe TestZZR73CallImportsProbe:** 51 cases × {unscoped, scoped CALL ()/(n), IN TRANSACTIONS} × {auto-commit, explicit tx}. Head i20261018121744-54952-bc20; develop i20261018121744-10908-922b. Every scoped form passes on both. All failures are unscoped and identical across routes and parsers.
- **Fail on head AND develop:**
  - D05 property key `m.n`, D06 string `'n'`, D07 map key `{n:1}`: A1 counts them as reads, so the nested EXISTS binds the outer n (0 vs 1).
  - D08: a pattern-comprehension local `[(m)-->(n) | n]` inside a nested COLLECT (0 vs 1).
  - R12 [new]: a nested `CALL { WITH n … }` inside an unscoped body is rejected with "Variable `n` not defined". semantic_validation.go:267 decides "outer" by grepping the text before the CALL, and the nested CALL's text starts at offset 0.
  - R16 [new]: a comprehension local shadowing the imported n is accepted unscoped (6/7) but rejected scoped, as Neo4j does. static_type_scope.go returns nil, nil on any unscoped walk error.
- **Fail on head only (regressed by the PR):** R05 and R20, nested UNION declared in one branch and read in another (10 instead of n.id; R20 errors "could not evaluate expression: n.id"). A2 treats a UNION as one branch.
- **Fail on develop only (fixed by the PR):** D01-D03, D12, D19, D22-D24, S01, S02, S04: nested declarations (including UNION and backticked names) and a scalar outer n.
- **Pass on both:** backticked reads R21-R23 (the shortcut gate and pushdown reader don't miss them); list/reduce/quantifier locals; QPP; labels and types named n; comments.

**Fabric probe TestZZR73FabricImportsProbe** (composite, 12 forms): head i20261018115217-25256-0177, develop i20261018115217-90328-0e1d; identical results.
- F04 [new]: an unscoped Fabric body's nested `EXISTS { (m)-->(n) }` binds the outer n (0 vs 1). Fabric passes every outer column as record bindings, an all-names import decision of its own.
- F06 `WITH k AS kk` and F08 `WITH k WHERE k > 1`: these importing WITHs are accepted. Neo4j rejects both, and the local route rejects the F06 form on its first branch.
- F01-F03, F05, F07, F09-F12 return the right values.

**Side finding (outside this family, to file):** on the composite route, `WITH k, c ORDER BY k RETURN collect(...)` after a Fabric CALL returns one row per input row instead of aggregating globally. Same on head and develop.

### A.3 Cause
No single owner answers "which outer variables does this body read, per branch, honouring clause and expression scope".
- Eight text readers (A1, A2, A4, A5, A7-A12) each answer part of it.
- The one walker that knows clause scope, UNION branches and nested subqueries (A6, semanticScopeWalk) feeds only validation and static types. The runtime, Fabric and the shortcut gate never consult it.
- So each decider works it out on its own:
  - runtime imports (A1/A3) and runtime branch locals (A2);
  - static CALL scope (all outer names minus A2, with errors swallowed for unscoped bodies);
  - leading-WITH validation (A1 on the preceding text);
  - Fabric (every column, plus its own WITH tokenizer).
- Every earlier fix patched one reader:
  - develop's masking (A4) dropped free reads inside nested subqueries;
  - the PR's recursive A2+A1 helper dropped reads in nested UNION branches and kept A1's property/string/map-key and comprehension-local errors.
- The other deciders never moved, so they drift: R12, R16, F04, F06, F08.

### A.4 Plan (for your check)
**Owner.** Extend semanticScopeWalk (A6) so that, for every body it walks and every UNION branch, it records:
- the free references that resolve to the enclosing scope ("reads");
- the names each branch declares before reading them.

It uses A5 (expressionFreeVariables + localReader) on every expression it already visits, and recurses into EXISTS/COUNT/COLLECT and nested CALL bodies through its existing subquery recursion. It is exposed as one function, `callBodyScope(body, outer) → {reads, branchLocals, importingWith, err}`, cached per body text in the statement clause plan (one walk per distinct body, not per row).

**Consumers switched to the owner** (their own readers removed):
- Runtime implicit imports and branch locals (pipeline_call_operator.go). Delete unscopedBodyReadsVariable, unscopedBranchLocals, branchDeclaresVariable and firstVariableOccurrence, and drop A1 from this path.
- Static CALL types (static_type_scope.go callSubqueryImports/callSubqueryReturnTypes). Visible names = reads, so imports are exact and the unscoped walk's errors are no longer swallowed. The gh648 implicit-import extension stays, now defined by "reads".
- Leading-WITH validation (semantic_validation.go:238-300). The outer set comes from the enclosing scope (fixes R12), and Neo4j's importing-WITH form rules live in one place.
- Shortcut gate pipelineReadsImportedBinding: reads instead of A1.
- cypher5ImportGrouping and pipeline_imports consume the same import set unchanged.

**Fabric.** pkg/fabric can't import pkg/cypher (cypher imports fabric), so the owner is injected: the cypher executor passes an ImportReader when it builds the Fabric planner. It replaces:
- extractWithImports / isWithImportStopKeyword (A10);
- the leading-WITH parsing in isSimpleWithImportClause, rewriteLeadingWithImports and importColumnsFromFragment (A11);
- the import-column scan in sanitizeOtherWhereForImportColumn (A12);
- stripLeadingWithImportsForFabricRecord (A13).

Fabric record bindings become the owner's imports instead of every outer column (fixes F04), and importing-WITH validation matches the local route (F06/F08).

**Not converged in this PR.** A1's uses at call.go:344 and query_info.go:505, and A7, A8, A9, answer the same question but for a different contract (clause-level pushdown and caching, not imports). The backtick probes show no wrong results from them. Listed for #1055 with file:line.

**Verification.**
- TestZZR73CallImportsProbe and TestZZR73FabricImportsProbe become regression tests, with the Neo4j-oracle table.
- New cases for every switched consumer: static type errors, leading-WITH forms (nested too), the shortcut gate on and off with identical results, Fabric record imports.
- Full ./pkg/cypher and ./pkg/fabric on both parsers; re-run the Neo4j oracle.
- Performance: the import decision becomes one cached walk per body text instead of per-call masking. Paired A/B on BenchmarkUnscopedCallImplicitImport (nested_exists was +5.72%), CallSubqueryClausePlan, StatementRouting and a Fabric CALL benchmark. Any remaining cost is stated in the PR.

**Release copy.** After develop, port to release/v1.4.4 (24d08776c) by hand. Release has A1, A2, unscopedCallImports, callSubqueryReturnTypes and extractWithImports. It still uses parseIntModifier (pipeline_projection.go:1117, pipeline_traversal_match.go:257), so the dead-code deletion is recomputed against release's callers.

## Family B (#1408): node identity encoding and rendering across GDS and similarity outputs

### B.1 Inventory (commands)
- **graphify** on /graphs/nornic-wt-145-r73-1408-src. Seeds: readGDSNode, readGDSNodeValue, gdsNodeRef.key/find/isNode, gdsShownKey, gdsShownBefore, renderGDSNodeColumns, renderGDSRowKeys, renderNodeIDs, nodeIDValue, gdsStreamHandler, gdsProjectionNode, gdsNodeByID, gdsNodeFilterID, similarityRowsScores, readSimilarityIncidence, tieKey, nodeSimilarity, callNornicSimilarityStream, similarityVectors. Name scan: /nodeid|shown|render|identity|gdsnode|elementid|nodekey|asnode/.
- **grep:** `git grep -n -E '"(nodeId|node1|node2|shared|sourceNodeId|targetNodeId|communityId|nodeIds)"' -- 'pkg/cypher/*.go' ':!*_test.go'` for every row producer; `git grep -n -E "func [a-zA-Z]*[Ee]lementI[Dd][a-zA-Z]*\("` for the canonical identity owner.

Members:
- **Readers:** readGDSNode (gds_node_identity.go:64) ← gdsProjectionNode gds_cypher_projection.go:290; fnGDSNodeProperty gds_functions.go:100; gdsNodeByID gds_functions.go:403 (asNode/asNodes); gdsNodeFilterID gds_node_similarity.go:278; readSimilarityRowsConfig gds_node_similarity_rows.go:79/86; readSimilarityIncidence :186/:207; linkPredictionConfigFromArguments linkprediction.go:537/550. readGDSNodeValue :205 ← gdsNodePair gds_functions.go:237 (alpha link prediction).
- **Encoding:** gdsNodeRef.key() with gdsRemoteKey "\x00R" and gdsLocalKey "\x00L" (this PR) [R]; develop's "\x00remote\x00" [R].
- **Decoding and ordering:** gdsShownKey :119 [R]; gdsShownBefore :128 ← similarityRowsScores tieBefore :371 and group sort :389.
- **Renderers (post-hoc, in place):**
  - renderGDSNodeColumns :250 ← renderNodeIDs :237 ← gdsStreamHandler :215, registered for every projection stream (procedure_registry_builtin.go:909) and using GraphProjection.nodeIDValue (gds_projection_state.go:42);
  - renderGDSRowKeys :243 ← callNornicSimilarityStream gds_node_similarity_rows.go:62 [R].
- **Row producers:** fastrp.go:103; gds_louvain.go:229; gds_node_similarity.go:99 (shared maps built at :545); linkprediction.go:588/:607; gds_node_similarity_rows.go:61/:383-410 (the grouped node1 list is shared across rows [R]; shared maps are per row).
- **Identity-domain boundary:** storage.NodeElementID (storage/element_id.go:10), via fn entityElementID (fn/builtins_core.go:101), uses the executing database, not the node's own.

### B.2 Self-attack (TrueNAS, default parser; GDS code doesn't depend on the parser)
TestZZR73GDSIdentityProbe, with ids {"\x00Rx", "\x00Lx", "\x00L\x00Rx", "\x00remote\x00x", "\x00", "p"} across:
- B1 scalar node1; B2 grouped node1 (3 targets); B3 grouped two sources; B4 shared nodeId (3 rows); B5 node2;
- B6 grouped local + remote "x"; B7 the same statement twice (result cache); B8 tieBreakProperty;
- B9 rows built in Cypher, called once per UNWIND row;
- B10 projection streams (fastRP, louvain, nodeSimilarity, filtered, all 6 linkPrediction); B11 aggregation projection;
- B12 composite with two local constituents sharing raw id "same".

Jobs: head i20261018122224-106200-4f29 (168 pass / 44 fail); develop i20261018122224-65760-b71a (190 / 22).
- **Head only** [R F2, plus shapes nobody had reported]:
  - every grouped node1 list for ids starting "\x00R"/"\x00L" is decoded again on each row (B2, B3, B8, B9) and shows as "x";
  - B6: local "\x00Rx" + remote "x" → ["x","x"];
  - the result cache then serves the corrupted rows (B7 is consistent, but wrong).
- **Develop only (the original P2):** "\x00remote\x00x" merges with remote "x" and shows as "x" in every shape B1-B9.
- **Both:** B12. The two constituent nodes have the same elementId "4:r73c:same" and arrive as plain *storage.Node with no database, so similarity counts them as one (0 pairs).
- **Pass on both:** all projection streams B10/B11 (they render once through nodeIDValue, and their only conversion, INTEGER ids, can't apply twice); scalar node1 and node2; shared nodeId.

### B.3 Cause
- The rows procedure represents identity as a string key that mixes two domains (stored ids and remote elementIds).
- It recovers the output value by decoding that string after the rows are built, in place, over row values that may share containers (the grouped node1 list is one slice for all rows).
- Develop's prefix encoding wasn't injective (P2). The PR made it injective but kept "decode the string later, in place", which isn't idempotent, so any value reached twice is corrupted (F2 and B6/B8/B9).
- The projection streams use the same post-hoc in-place renderer. They're only safe because their single conversion happens to be idempotent.
- B12 is the same contract at the next boundary: a node value has no database provenance, so no GDS code can tell those two nodes apart.

### B.4 Plan
**Owner.** Identity travels as the gdsNodeRef (id as given + kind) from input to output. Each output value is produced once, when the row is built:
- from the ref, via `ref.output()`: remote → its elementId; a node → its stored id; an INTEGER → INTEGER; an id string → itself;
- or from the projection's nodeIDValue, for projection streams.

Map keys stay internal: an injective key, never decoded. Delete gdsShownKey, renderGDSRowKeys, the in-place renderGDSNodeColumns/renderNodeIDs, and the gdsStreamHandler post-processing. The producers (fastrp.go, gds_louvain.go, gds_node_similarity.go including shared maps, linkprediction.go, gds_node_similarity_rows.go) build their node-id cells through the owner. Ordering (gdsShownBefore) compares ref outputs.

**B12** is outside this family's boundary: it needs the entity-provenance owner (storage.Node carries no database, and elementId uses the executing database). File it as its own bug with the probe; no similarity-only workaround.

**Verification.** TestZZR73GDSIdentityProbe becomes the regression test over every procedure × shape × id kind, alongside the existing gds* tests. A/B on SimilarityRows, NodeSimilarity, FastRP, LinkPrediction and GDSProjectAggregation.

**Release copy.** release/v1.4.4 has none of the rows procedure, gds_node_identity.go or the stream renderer, so F2 doesn't exist there. The release copy carries only the parts of #1408 whose procedures exist on release, decided after the develop fix; no new procedures.

## Open questions for the orchestrator
- Family A boundary: include Fabric (A10-A13) in this PR as planned? It changes composite import behaviour (F04, F06, F08). Or split it out?
- A1's uses at call.go:344 and query_info.go:505, and A7-A9, stay out (different contract) and are listed for #1055.

# Gate approved (18 Oct). Family A owner: API note (for the #1459 agent)

Both PRs were rebased onto develop 16923aef7 before this (#1385 now be07f6f87 locally, #1408 e06ff7a9f locally; neither pushed yet).

**One static owner of "what a query body reads from the scope around it, and what it declares".** It lives in pkg/cypher next to semanticScopeWalk and is produced BY semanticScopeWalk (a recorder carried in subqueryScope), so the validator and every consumer see one answer.

```go
// bodyScope is what one query body (a CALL body, or the body of an
// EXISTS / COUNT / COLLECT subquery) reads from the scope around it and
// declares itself. Static: names only, never values.
type bodyScope struct {
	Reads    []string      // enclosing-scope names the body reads, any depth, first-read order
	Branches []branchScope // one per top-level UNION branch (one entry without UNION)
}
type branchScope struct {
	Reads     []string // names this branch reads from the enclosing scope
	Declares  []string // names this branch binds before any read of them (a new variable there, even if the enclosing scope has one)
	Importing []string // an unscoped CALL branch's importing WITH list; nil when it has none
}
// visible: the enclosing names the body can see; nil = an unscoped
// CALL { } body, whose visible names are exactly the ones it reads.
func (e *StorageExecutor) bodyScopeOf(body string, visible []string, cypher25 bool) (bodyScope, error)
```

Rules (one implementation; each rule is today's validator rule):
- **Read:** a name in an expression position (WHERE, projections, property-map values, function/list/map arguments, ORDER BY, SKIP/LIMIT, SET/REMOVE/DELETE operands, procedure arguments, UNWIND source, pattern predicates `WHERE (n)-->()`) that the body hasn't bound before that point, and that isn't a local of the expression (comprehension / reduce / quantifier iterator, read through localReader).
- **Pattern-variable position** (MATCH / OPTIONAL MATCH / CREATE / MERGE patterns, EXISTS/COUNT/COLLECT patterns, pattern comprehensions): a read when the name is visible at that point, a declaration otherwise. For visible == nil (unscoped CALL), a name is visible only once something reads it, so a name used only in pattern positions is a new variable (Neo4j's answer for `CALL () { … }`).
- **Horizons:**
  - A nested EXISTS/COUNT/COLLECT body sees the enclosing body's scope at its position (the body's own earlier bindings, and a WITH/RETURN's projected names for its WHERE/ORDER BY). Its reads that aren't enclosing-body locals propagate to Reads.
  - A nested `CALL (a, b) { }` reads exactly a, b. A nested `CALL (*) { }` reads every name it uses that's visible. A nested unscoped `CALL { }` reads what its own body reads (same rule, recursively).
  - Each UNION branch is walked on its own (a declaration in one branch never hides a read in another).
  - A WITH in a body doesn't end visibility of enclosing names (the existing import-carry extension).
- **Presence vs value:** the owner says whether a name is read; whether its value is null or the name is absent from a row is the runtime frame's business.
- **Cache key:** (body text, cypher25, visible as a sorted list, or "*implicit*" for nil). Cached in the statement clause plan (one walk per distinct body per statement form), not per row.
- **Consumers (#1385):** runtime CALL imports and branch locals, static CALL types, leading-WITH validation, the shortcut gate, Fabric (through an injected ImportReader). **#1459:** the runtime frame for EXISTS/COUNT/COLLECT/comprehension bodies consumes bodyScopeOf(body, clauseVisible) instead of walking scope itself.
- Landing order: the owner and its tests go first (own commit on #1385, kept small), then the consumer switches.

## Side-finding issues filed (18 Oct ~12:40Z), each label bug, milestone v1.4.4
- #1478 composite: nodes from two constituents with the same stored id get the same elementId (4:comp:same, equality true). develop 16923aef7 i20261018123129-30556-0036, release 24d08776c i20261018123129-58308-e2ec.
- #1479 composite CALL: WITH … ORDER BY then aggregating RETURN gives one row per input row ([2],[1] instead of [1,2]); plain collect/count after the CALL are right. Same jobs.
- #1480 (found while confirming) composite CALL body that is a UNION of USE branches fails "could not evaluate expression: n UNION ALL". Same jobs.
Repro probe: wt-145-iss-{dev,rel}/pkg/cypher/zz_issue_composite_probe_test.go (untracked).

## 1.5.0 requirement (recorded 18 Oct; dropped from 1.4.4 scope by the orchestrator)
The #1468 consumer needs a form-tagged entry into the same owner for pattern comprehensions: bodyScopeOf(form, body, visible, cypher25) with form = query | patternComprehension; for `[p=(a)-->(b) WHERE … | proj]` the named path p and the pattern's unbound variables are local Declares even when an outer p exists; Reads holds only real outer reads; the cache key includes the form. Not in #1385 (1.4.4). The PR body carries a one-line note.

## Family B implementation (18 Oct, 12:46Z)
- #1408 head adf89b06f (pushed, force-with-lease over 091b82e5c; rebased on develop 16923aef7): gdsNodeOutput (projection streams: FastRP, Louvain, node similarity incl. shared, every link prediction) and gdsRowNodes (similarity rows) produce each id once at row build; post-hoc renderers and ExecuteResult.gdsProjection removed.
- TestGDSNodeIdentityThroughEveryOutput (from the probe, minus B12 → #1478). Red: probe on pre-fix head i20261018122224-106200-4f29.
- vet i20261018124458-58180-343b clean; gofmt i20261018124343-29968-b02c clean; focused 206 tests both parsers i20261018124625-61524-330f PASS.
- Full pkg/cypher both parsers running (scratch145/1408-full-r73.log); A/B a20261018124821-105868-b3be queued (--no-wait).
