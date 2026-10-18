# One reader for the inside of a relationship pattern: Rule 73 gate, step 1

Owner: Claude Opus 5.5 implementation sub-agent, 18 Oct 2026. Parent thread `075cf8e3-9632-45e4-a74b-ecbe4e778f84`.
Code inspected: develop `16923aef7` (#1407 merged), release/v1.4.4 `24d08776c` (#1472, the release copy of #1407, is still open). The earlier Codex branch `fix/relationship-readers` (head `47000a40`, draft PR #1473) was tested as a third build.
Step 1 only. No product code changed, nothing pushed. The original Codex handoff is kept unchanged at the end of this file and in `rel-inv/fix-rel-readers-codex-original.md`.

## 1. How the inventory was built (repeatable)

All extractor outputs are in `sig/handoff/rel-inv/`.

| Pass | Command (from `wt-rel-inv/pkg/cypher`, non-test files) | Output |
|---|---|---|
| E1 literal arrows | `grep -n -E '"[^"]*(-\[\|\]-\|<-\[)[^"]*"\|`[^`]*(-\[\|\]-)[^`]*`'` | e1-literal-arrows.txt (69) |
| E2 byte-level bracket scans | `grep -n -E "== '\['\|== '\]'\|'\[' *&&\|…\|IndexByte\([^)]*'\['\|Index\([^)]*\"\]\""` | e2-byte-brackets.txt (113) |
| E3 regexes containing `\[` | `grep -n -E 'regexp\.(Must)?Compile' \| grep -F '\['` | e3-regex.txt (4) |
| E4 functions named *Rel* taking a string | `grep -n -E '^func (\([^)]*\) )?[A-Za-z]*(Rel\|rel)[A-Za-z]*\([^)]*string'` | e4-relfuncs.txt (137) |
| E5 canonical reader callers | `grep -n -E 'relationshipDeclarationOf\|parseRelationshipDeclaration'` | e5-decl-callers.txt |
| E6 per-function body scan | `extract-funcs.sh`: a function body that finds a bracket AND splits on `: \| * { ..` AND mentions rel/edge/pattern | e6-funcs.txt (95) |
| Union | hits mapped to their enclosing function (`enclosing.sh`) | union.txt (284 functions) |
| Graphify (local shared container) | `docker exec graphify graphify update /repos/nornic-wt-rel-inv --no-cluster` with `GRAPHIFY_OUT=/graphs/nornic-wt-rel-inv/graphify-out` (code-only; 53,807 nodes, 68,715 call edges) | graph root `/graphs/nornic-wt-rel-inv` |
| Call-graph script | `relinv2.py` (in the graph root): (a) reachability from `StorageExecutor.Execute` and from the 59 pkg/cypher functions other packages call; (b) every caller of the bracket and declaration primitives that the grep union missed; (c) the shortest caller chain for each reader | g2-cands.txt, g3-prims.txt, g4-prims.txt, g5-chains.txt |

**Note on reachability.** Graphify's own call edges miss many receiver method calls: `Execute` shows only 74 callees. `augment.py` adds name-resolved edges, giving 350,929. For each graphify function node it takes the text from the node's line to the next function in the file. That layer over-approximates where names collide. For example, `element()` "reached" through `normalizeParameterValue` is a name collision; its real caller is `patternElements`, checked by reading the code. Every reachability claim below was checked against the code, not taken from the graph alone.

Classification: four read-only agents classified every union function, and a fifth classified the 45 graphify-only callers. I then spot-checked the readers that drive the failures (section 3). R = reads the inside itself. D = reads it through `relationshipDeclarationOf`/`Head`. B = only finds the brackets or arrows. N = not a relationship-inside reader.

**What graphify added** that the grep passes missed:
- `pipeline_write_stretch.go:179 addStretchPattern`: the write-stretch reader the task names.
- The label-rewriter reader `label_expression_rewrite.go:694 element`.
- `relationship_quantifier_rewrite.go:103 quantifiedRelationship`.
- `quantified_path_pattern.go:713 quantifiedGroupError`.
- `traversal.go:1220 parseChainedTraversalPattern`.
- `create_semantic_validation.go:330 createPropertyMapBody`.
- `dynamic_tokens.go:513 staticPatternTokenError`.
- `semantic_validation.go:490 containsIllegalProjectedPatternExpression`.
- The D consumers of `parseTraversalPattern` and of `parseMergeRelationshipShape`.

## 2. The complete list

### 2a. R: reads the inside itself, reachable from a statement (production)

| # | Place | Reads | How it splits / what it gets wrong | Reached from (chain) |
|---|---|---|---|---|
| 1 | clauses.go:659 `parseUnwindMergeRelationshipClause` (task's clauses.go:684) | var, type, props | `strings.Index "["`/`"]"`, first `{`, last `}`, first `:`; not quote-aware. Only gates the plan (identifier checks); rows run through `executeUnwindRowsPipeline` | Execute → executePipeline → executeUnwindBatchOperator → executeUnwindMergeChainBatch → cachedUnwindMergeChainPlan → parseUnwindMergeChainPattern |
| 2 | clauses.go:1155 `executeUnwindFixedChainLinkBatch` (closure parseMergeClause, l.1265-1310) | type | `strings.Index "["`/`"]"`; gate only | Execute → executePipeline → executeUnwindBatchOperator |
| 3 | unwind_relationship_merge_batch.go:184 `parseRelationshipBatchMergeClause` | var, type, props | `Index "]->"`, `LastIndex "-["`, first `{`; not quote-aware; keeps backticks in the type; gate (isSimpleIdentifier) | executeUnwindBatchOperator → executeUnwindRelationshipMergeBatch |
| 4 | unwind_multi_match_create.go:630 `parseSimpleCreateEdge` | type, props | same as #3 | executeUnwindBatchOperator → executeUnwindMultiMatchCreateBatch |
| 5 | call.go:924/981 `parseCallTailRelationshipPattern` / `…Token` | var, one type, one-key map | own identifier and map scan; quote-aware; strict gate | executeCall → executeProcedureCall → executeCallTail → tryExecuteCallTailRelationshipMatchProjection |
| 6 | pipeline_write_stretch.go:179 `addStretchPattern` (task's :183) | props keys, length, types | backtick-aware `{` and `:` only; raw `*`; does not handle inline `WHERE` (`[r:T WHERE …]` records type "T WHERE …") | Execute → executePipeline → runPipelineClauses → runPipelineClauseRows → pipelineWriteStretchConflicts → analyzeStretchClause |
| 7 | query_info.go:553 `appendPatternPropertyExpressions` (task's :556) | type as a label chain, props | backtick-aware `{`; a `$({…})` type or `*` breaks it | MergeUniqueConflictIsRetrySafe → mergeStatementSetItems |
| 8 | label_expression_rewrite.go:694 `element`, :781 `samePatternMapReads`, :964 `relationshipElement` | var, types, length, props, inline WHERE | quote-aware, **not parenthesis-aware**: a `*` or `{` inside `$(…)` is read as a length or the property map | Execute → desugarLabelExpressions → statement → pattern → patternElements |
| 9 | label_expression_rewrite.go:868 `declaredPatternVariables` | var | first symbolic name after `(`/`[`; does not skip `$(…)` | patternWithWhere |
| 10 | relationship_quantifier_rewrite.go:103 `quantifiedRelationship` | WHERE, `{`, `*` | `indexOutsideQuotes`; not parenthesis-aware | patternElements |
| 11 | path_selector.go:508 `hasUnboundedRepetition`, :565 `hasVariableLengthRelationship`; quantified_path_pattern.go:713 `quantifiedGroupError` | length | quote-aware, not parenthesis-aware (`$(t*2)` reads as a variable length) | statement → patternWithWhere → pathPrefixes; patternElements → quantifiedGroup |
| 12 | relationship_variable_repeat.go:36 / :112 / :148 | var, length | quote-aware, not parenthesis-aware | desugarLabelExpressions → patternWithWhere |
| 13 | label_expression_rewrite.go:165 / :195 / :229 prefilters (`mayReadPatternInPropertyMap`, `mayUsePatternPredicate`, `mayUseLabelExpressions`) | presence only | prefilters that "never answer false" (over-approximation only) | desugarLabelExpressions |
| 14 | create_semantic_validation.go:341 `createRelationshipPropertyMapBodies` + :330 `createPropertyMapBody` | props | raw `Index "["`, backtick-only `{`, `LastIndex "}"`; a `{` inside `$(…)` is taken as the map | Execute → validateSemanticScopes → validateCreateSemanticScopes → validateCreateClauseBindings |
| 15 | static_operator_types.go:1162 `forEachClauseOperatorExpression` (visitPatternValues) | prop values | skips `'`/`"` but not backticks or `$(…)` | validateBoundParameterExpressions → validateStaticOperatorParameters → validateStaticOperatorTypes |
| 16 | subquery_value_expression.go:888 `patternMapsAreConstantText` | props | a `{` inside `$(…)` is taken as a map; declines safely | rowSubqueryValue → boundChainCount → parseBoundChainShape |
| 17 | dynamic_tokens.go:442 `resolveRowDynamicTokens`, :513 `staticPatternTokenError` | dynamic type only | quote/paren-aware | pipelinePlanCreateRow; validateMatchSemanticScopes |
| 18 | optional_match_traversal.go:113 `extractRelationshipVariables` | var | shared bracket finder, `scanSymbolicName` | validateCreateSemanticScopes and five pipeline appliers |
| 19 | merge_semantic_validation.go:267 `mergePatternUsesParameterPredicate` | `$param` map | own quote/brace scan; skips `$(…)` | validateMergeClause, validateMatchClauseBindings |
| 20 | executor_subqueries.go:1058 `isPatternVariableAt` | var | `LastIndexByte '['` | branchDeclaresVariable |
| 21 | quoted_variables.go:299 `classifyQuotedToken` | var (backticked) | quoted-token scanner | canonicalizeQuotedVariables |
| 22 | explain.go:556 `containsVariableLengthPathPattern` | length (EXPLAIN plan text only) | outer `[` scan not quote-aware; misses `[r:T*1..3]` | Analyze → analyzeQuery → analyzeMatchClause |
| 23 | expression_failure.go:753 `relationshipPatternMayFail` | presence of `{`/WHERE | coarse, errs towards "may fail" | rowOperandFactsOf |
| 24 | shortest_path_match.go:166 `shortestPathPatternError` | props (error message only) | raw first `[`, raw `{` | evaluateShortestPathValue / parseShortestPathMatch |
| 25 | DDL readers: schema.go:831 `parseCreateIndexForPattern`, :1716 `parseRelationshipForDirection`, :1825 `parsePolicyForClause`, :1913 `parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL`, :2422 `parseFulltextRelationshipTypes`; schema_contracts.go:20; schema_lookup_index.go:23 (regex); knowledgepolicy_ddl.go:566 `parseEdgeTarget` | var, type | `strings.Index(inside, ":")` after a quote-aware bracket match | executeSchemaCommand / ParseKnowledgePolicyDDL |

### 2b. B: bracket or arrow scanners that misread the inside (production)

| Place | Defect | Reached from |
|---|---|---|
| label_expression_rewrite.go:1414 `opensSubquery` (panic at :1440) | counts `)` inside a string while walking back; the slice index reaches -1. Item (b) | Execute → mayUseQueryStructure; also expression(), subqueries() |
| traversal.go:1206 `isChainedPattern` | raw reverse scan: a `]-` inside a backticked type counts as a second hop | Execute → executePipeline → … → parseTraversalPattern |
| traversal.go:1220 `parseChainedTraversalPattern` | its own state machine: handles `'`/`"` but not backticks or `$(…)` | parseTraversalPattern |
| query_patterns.go:540 `extractNodeVariables` | skips `'`/`"` but not backtick names, so a `'` inside a backticked relationship type swallows the rest of the pattern | validateCreateSemanticScopes, pipeline appliers, MATCH binding scope |
| merge_context.go:195 `extractVariableNamesFromPattern` | toggles on `[`/`]` with no quote handling | validateSetSemanticScopes → pipelinePatternBindingNames |
| semantic_validation.go:490 `containsIllegalProjectedPatternExpression` | raw `[` scan, not quote-aware | validatePatternExpressionPlacement |
| create.go:753 `splitNodePatterns` | a `(` inside a relationship map (`[:R {k: f(x)}]`) becomes a fake node `(x)` | validateCreateClauseBindings |
| executor.go:1266 `queryDeletesNodes` | `strings.Contains "]->("` over the whole query, string literals included | Execute |
| query_patterns.go:159 `countRelationshipPatterns` | only `'`/`"` aware | DetectQueryPattern (ExecuteOptimized) |

### 2c. D: already on the canonical reader

create.go:137/845/968; create_pipeline_template.go:152; create_semantic_validation.go:203/234; path_selector_search.go:187; traversal.go:188 `parseRelationshipPattern` (and the 13 consumers of `parseTraversalPattern` graphify found); merge_relationship_pattern.go:52/97; merge_actions.go:234; pipeline_pattern_template.go:150; match_semantic_validation.go:1331/1375; executor_match_vector_cosine_fastpath.go:484/608/1471.

**The canonical reader itself lacks inline `WHERE`.** For `[r:$(…) WHERE r.w = 17]`, `relationshipDeclarationOf` returns typeText `$(…) WHERE r.w = 17`. Probe: i20261018113626-44524-e328.

### 2d. No production caller (dead or test-only)

ast_builder.go:29 regex (`GetAST` is test-only); call.go:1310; clauses.go:2006 `parseOptionalRelPattern`; clauses.go:2074 (no callers at all); executor_mutations.go:854 / 945 / 1099 / 1130 / 1161; rel_existence_pattern.go:5; optional_match_traversal.go:138 `parseOptionalClauseEndpoints` (the task's :160); query_patterns.go:588 `extractRelationshipType` (the task's :599); create_pipeline_helpers.go:80; executor_mutations_relationship_delete.go:11; create.go:1423.

## 3. Results on TrueNAS against Neo4j 5.26.30

**Matrix.** `rel-inv/matrix/` holds `template.txt` × `forms.tsv` + `extra.cypher`, generated by `gen.sh`. That gives 311 statements, each preceded by `RETURN n AS stmt`. The harness `zz_relinv_matrix_test.go` (temporary, never committed) runs the same file auto-commit (A) and one explicit transaction per statement (X), printing cypher-shell's format; `norm.sh`/`cmp.sh`/`show.sh` compare them.

Forms (each in CREATE, MATCH, MATCH with map, MATCH with inline WHERE, OPTIONAL MATCH ×2, ranges `*1..2` with and without a map, OPTIONAL `*2`, MERGE ×2 with ON CREATE/ON MATCH, full-pattern MERGE ×2, UNWIND…MERGE chain, write stretches CREATE→MATCH / SET→MATCH / MERGE→MATCH, EXPLAIN, PROFILE, EXPLAIN MERGE, PROFILE OPTIONAL range):
- plain `:DZ`
- braces `$({k:'DZ'}.k)`
- star `$(['X','DZ'][i * 1])`
- slice `$({ts:['DZ','X']}.ts[0..1][0])`
- quotes `$('D):Z|Q{*..')`, `$("D]:Z|{*..")`
- escapes `$('D]\'Z')`, `$("D\"Z]-[")`
- backtick `` `D):Z|Q{*..` ``
- CASE `$(CASE WHEN i = 1 THEN 'DZ' ELSE 'X' END)` and a CASE returning lists

Extras:
- (c) mixed-type ranges: 14 statements.
- (d) nested map access in node and relationship maps: 18 statements.
- Quoted values in property maps and inline WHERE: 11 statements.
- CASE as dynamic labels and types: 4 statements.

**Jobs.**
- Neo4j: j20261018112728-78836-0305 (all 311 statements succeed; log neo-3.log).
- NornicDB, both parsers, both modes:
  - develop i20261018112730-77408-f3c6;
  - release i20261018112730-100300-4e98;
  - #1473 head i20261018112730-65112-cd3b.
- Both parsers and both transaction modes give identical results on every build. Every difference below holds on default and ANTLR, auto-commit and explicit.

| Form / group | develop 16923aef | release 24d08776 | #1473 (47000a40) |
|---|---|---|---|
| plain, quotes in values, CASE labels/types | = Neo4j | = Neo4j | = Neo4j |
| braces, CASE | PROFILE errors `unknown function: __nornic_haslabels` | same | = Neo4j |
| star | inline WHERE and stretch-MERGE reject "pattern predicates are not supported for variable-length relationships"; PROFILE as above | **all 24 statements fail** ("variable-length relationships cannot be created"): #1407 not on release | inline WHERE + stretch-MERGE still fail |
| slice | CREATE TypeError "Expected node label to be a string…", then everything after it differs | same | = Neo4j |
| single/double quotes with delimiters (b) | **Go panic** `slice bounds out of range [:-1]` in 21 of 24 statements | same panic | = Neo4j |
| escaped quotes `\'` / `\"` | MATCH / OPTIONAL MATCH / ranges: "Variable `b` not defined"; MERGE / stretches: "invalid traversal pattern"; one PROFILE matched a literal type `:$('D]\\'Z')` | same | **still fails** (12 escdq + 4 escsq statements) |
| backtick `` `D):Z|Q{*..` `` | = Neo4j | MATCH/OPTIONAL/stretches: "expected Relationship but was List" (the `*` read as a length) | = Neo4j |
| CASE returning lists | every statement: "Type mismatch: expected String or List<String> but was List<T> or List<String>" | same | inline WHERE + stretch-MERGE still fail |
| (c) mixed-type range | 8 differ (see below) | same | = Neo4j |
| (d) nested map access | 13 differ: CREATE writes null; MERGE errors; **MATCH and OPTIONAL MATCH return both 17 and 999** (the filter is silently dropped: wrong data) | same | = Neo4j |
| UNWIND…MERGE chains | = Neo4j wherever the form itself works | same | = Neo4j |

**Item (c): what Neo4j 5.26.30 does.**
- For a dynamic type on a `*` range, Neo4j ignores the dynamic type completely: `[r:$('NOPE')*1..2]` returns both paths, `$any(['NOPE'])` too, and so do the values 7 and null.
- Property maps still filter.
- A single-hop `$('NOPE')` and a static `:NOPE*1..2` return nothing.
- NornicDB filters every hop by the type.

**DDL check.**
- Neo4j: j20261018113743-96652-3cef (neo-ddl.log). NornicDB: i20261018113746-96080-3fd3.
- Index, uniqueness constraint and fulltext DDL with backticked types containing `]`, `:`, `-[` match Neo4j.
- The follow-up `MATCH ()-[r:`U]-[:z` {k:1}]->()` returns 0 rows on NornicDB, 1 on Neo4j. That is `isChainedPattern` counting the `]-` inside the name.

**Root-cause probes**, i20261018113626-44524-e328 (both parsers):
- A static backticked type that contains a quote fails on its own:
  - `MATCH (a:QB)-[r:`D]'Z`]->(b:QB) RETURN b.id` → "Variable `b` not defined" (`extractNodeVariables`);
  - MERGE of `` `D"Z]-[` `` → "invalid traversal pattern" (`isChainedPattern` / `parseChainedTraversalPattern`).
- The escaped dynamic forms reach the same code: desugaring folds the constant `$('D]\'Z')` into the static name `` `D]'Z` ``.
- `star` + inline WHERE: `relationshipElement` (via samePatternMapReads/`*`) reads the `*` inside `$(…)` as a length. Separately, `relationshipDeclarationOf` puts the WHERE text into the type.

## 4. Why the readers diverged

1. **Each clause and fast path got its own reader before a shared one existed.** UNWIND batch plans, the CALL-tail plan, write-stretch analysis, the MERGE-retry planner, the label-expression rewriter, the semantic validators and the traversal parser each cut the inside with `strings.Index` on `] : { * |`.
2. **Each one handles quoting differently.** Some skip `'`/`"` but not backticks (`extractNodeVariables`, `visitPatternValues`, `parseChainedTraversalPattern`). Some skip quotes but not parentheses (the rewriter, path selector and variable-repeat readers, `createPropertyMapBody`). Some skip nothing (`isChainedPattern`, `opensSubquery`, `queryDeletesNodes`, `extractVariableNamesFromPattern`).
3. **Dynamic types (5.26 syntax) put arbitrary expressions in the type position**: strings with escapes, maps, slices, `*`, `..`, CASE. Every reader that assumed "type = identifier" mis-splits there.
4. **#1407 converged only the three clause readers** (CREATE, MERGE, MATCH validation and traversal). The rest kept their own splitting, and the canonical reader never learned inline `WHERE`.
5. **The quote panic (b)** is a separate scanner in the query-structure path, `opensSubquery`. It walks back over `)` without knowing about strings.
6. **(c) and (d) are not text readers.**
   - (c) is the traversal filter (traversal.go:2109): NornicDB applies the dynamic type to every hop, and Neo4j does not apply it on ranges at all.
   - (d) is the subscript receiver reader `rowSubscriptReceiverStart`: it drops the map/list/call primary before `.outer.xs[i]`.
   - CASE is the static dynamic-type admission check, `dynamic_tokens.go:341`.

## 5. Plan for step 2 (after go-ahead)

One integrated change. #1473's commits (`52f14b25..47000a40`) are reused where they are already right: the (d) subscript reader, CASE admission, the PROFILE source re-entry and the `opensSubquery` quote fix. The rest is new.

1. **Owner.** `relationshipDeclarationOf`/`Head` reads the whole inside, inline `WHERE` included (a new `predicate` field). Boundaries come from one masked text: quotes and backticks with escapes, plus parenthesised expressions. One element scanner, built on `scanRelationshipBracket` (relationship_brackets.go), walks a pattern's nodes and relationships for every caller and skips quotes, backticks, maps and `$(…)` the same way.
2. **Route every R entry in 2a through it:** #1–#4 (UNWIND plans keep their identifier gates, read from the declaration); #5; #6; #7; #8–#12 (the rewriter's element, map-read, length and variable logic read the declaration instead of rescanning); #14–#16; #18–#20; #22–#24. #13 stays a prefilter, and #17 already uses the quote/paren-aware matcher.
3. **Scanners in 2b skip relationship insides through the element scanner:** `isChainedPattern`, `parseChainedTraversalPattern`, `extractNodeVariables`, `extractVariableNamesFromPattern`, `containsIllegalProjectedPatternExpression`, `splitNodePatterns`, `queryDeletesNodes`, `countRelationshipPatterns`, and `opensSubquery` (b).
4. **DDL readers (#25):** a quote-aware colon from the same owner. DDL accepts only static names, so this is the same reader with no new behaviour.
5. **2d dead readers:** delete them with their test-only callers. Routing code that never runs would keep a second implementation alive.
6. **(c):** apply Neo4j 5.26.30's rule once, in the label-expression rewrite before the traversal routes split (as #1473 did). A dynamic type on a `*` range does not filter; single-hop dynamic types, static types and property maps still do. Divergence documented in code and the PR, and the TCK checked first (it has no dynamic types).
7. **Tests:** the matrix becomes committed table tests with Neo4j's recorded results.
   - Every form × clause, both transaction modes, both parsers, plus the escape, DDL and backtick-quote cases.
   - Red first on develop, green on the branch.
   - Then full `pkg/cypher` on both parsers.
   - A paired A/B (`tnrun ab --no-wait`, 10 rounds) on StatementRouting, MatchSemanticValidationCached, MergeUniqueKey and the UNWIND MERGE batch benchmarks, optimised until any remaining cost is stated plainly.
8. **PRs:** develop PR (label bug, milestone v1.4.4, "Part of #1055"). The release/v1.4.4 copy covers at least (b) and (c); it needs #1472's reader first, so it is based after #1472 or carries it. Both linked to the parent thread. Not merged. #1473 is superseded by this change and will be closed with a pointer once the new PR exists.

## 6. Open point for the user

Item (c). Neo4j 5.26.30 ignores a dynamic relationship type on variable-length patterns, even `$('NOPE')`, 7 or null; it looks like a Neo4j defect. Following Neo4j (the standing rule) means `MATCH ()-[:$('NOPE')*1..2]->()` returns paths of any type. The newer Neo4j 2026.09 cannot be checked through the lab tooling (only neo-ref and gds are available).

---

## 7. Step 2 (approved 18 Oct): implementation

- Develop PR #1485 (draft), branch `fix/relationship-pattern-reader`, head `69b9a43ae`: #1473's four commits plus `8b1756e73` (convergence, PROFILE rows) and `69b9a43ae` (edge contracts, gofmt). Release PR #1486 (draft), branch `fix/relationship-pattern-reader-v1.4.4` on #1472, head `53cdbf8b2`: same commits plus release-only `e35015dd6` (a `[` after a map literal subscripts the map). Both linked to this thread and to the parent thread. #1473 closed as superseded.
- Oracle test `TestRelationshipPatternReaderMatchesNeo4j`: 415 statements, `testdata/relationship_pattern_oracle.json` from Neo4j j20261018122655-75088-eaef (generator: `rel-inv/matrix/gen.sh`, `norm.sh`, `mkoracle.awk`).
  - Red: develop i20261018123110-13160-01e6; #1473 head i20261018123114-28052-80c2 (69 statements differ); release #1472 head i20261018132002-62416-81b1 (26 panics).
  - Green: develop i20261018132504-88952-65f8; release i20261018131806-69468-cb61. Zero differences in the family on both heads, both parsers, both transaction modes.
- Root causes found during step 2 beyond the inventory: PROFILE data rows were rewritten by the client-text restore (keyword_scan.go `restore`), and release lacked develop's map-receiver subscript rule.
- Full pkg/cypher both parsers: develop `rel-inv/s2/full2.log`, release `rel-inv/s2/rel-full2.log` (results in the PR bodies). A/B a20261018132748-28960-3638 (the earlier a20261018131514-72572-18b2 against 8b1756e7 is held in ab-priority and superseded by it).
- Review round 1: Codex Sol xhigh on Codex Admin, `reviews/sol-1485-r1.md`.

# Earlier Codex run (kept as written)

# Relationship reader and expression follow-up

Implementation branch: `fix/relationship-readers`, worktree `wt-rel-readers`.
Head: `47000a4027cc91bf7bd5d284fd7f30bf8dbd67d5`.
Base: develop `16923aef778025fab3490f6a57549763b536f3d5`, after #1407 merged. The branch was initially based on `5fcc6f84`, then rebased with only this follow-up's commits.
Parent thread: `075cf8e3-9632-45e4-a74b-ecbe4e778f84`.
PR: [#1473](https://github.com/orneryd/NornicDB/pull/1473), draft while A/B is pending; base develop, bug, milestone v1.4.4. Linked to this implementation thread and parent thread. Do not merge.

## Findings per reader

- `clauses.go`, UNWIND/MERGE chain: the static fast path declines dynamic expressions both before and after. Probes cover maps, slices, multiplication, quotes, braces, stars and ranges; repeated MERGE retains one relationship and runs both actions. It now takes the complete bracket with `findMatchingDelimiter` and reads its declaration through `relationshipDeclarationOf`, retaining the conservative static-plan admission. No runtime wrong answer was established in the old fast path.
- `optional_match_traversal.go`, endpoint reader: direct probes reproduced truncated dynamic and quoted types. This helper currently has no production callers; runtime OPTIONAL MATCH probes pass. It now takes its variable and type text from the shared declaration.
- `pipeline_write_stretch.go`, dependency tokens: direct probes reproduced the expression's `ts` map key being recorded instead of the actual relationship property `w`. Dynamic types already implied any relationship, so no runtime wrong result was established. The reader now records the shared declaration's actual properties and types. Clause visibility probes cover dynamic writes followed by reads.
- `query_info.go` and `query_patterns.go`, plan/info readers: direct probes reproduced truncated types and missing property expressions for quoted type names containing delimiters. Both consume the declaration; the neighboring relationship-variable reader also consumes it. Dynamic expressions do not become alleged static MERGE keys. EXPLAIN/PROFILE probes cover dynamic types with maps/slices/arithmetic and real ranges.
- PROFILE's recursive execution revalidated generated internal predicates as user text. It now retains the source before expression rewrites and executes that source through the regular entry, preserving validation, caching behavior and implicit write transactions. A failed profiled write leaves zero nodes.

## Expression and range corrections

- Nested-map inline properties: `rowSubscriptReceiverStart` now includes the map/call/list primary before a property chain, so `{outer:{xs:[11,17]}}.outer.xs[i]` resolves to 17. The text and compiled row evaluators already consume this single reader. Tests cover both entity kinds, CREATE/MERGE/MATCH/OPTIONAL MATCH, decoys, repeated MERGE, autocommit and explicit transactions.
- CASE dynamic types: dynamic-token admission now uses the existing function-signature owner's union handling. Both possible list result types are admissible; single-hop runtime validation remains. Tests also cover dynamic labels and property keys, and retain rejection of incompatible static types.
- Quoted-string panic: `opensSubquery` formerly counted `)` inside a string and sliced at -1. Both it and the map/subquery brace classifier now consume the shared quote-aware `matchingRowDelimiterStart`. The declaration type splitter also skips string contents. Red/green tests cover single quotes, double quotes and a map-expression type with `D):Z|Q{*..`, in both transaction modes and parsers.
- Dynamic ranges: Neo4j 5.26.30 ignores dynamic type values on legacy `*` ranges, including the first hop and unresolved row-dependent values; static types and single-hop dynamic types still filter, and property maps still filter every hop. This is normalized once by the relationship label-expression rewriter before traversal paths diverge. A retained internal predicate enables the existing static token check while returning true without evaluating a row-dependent type expression. The TCK corpus predates dynamic type syntax; its static map/property behavior was checked before adopting the Neo4j behavior.

## Verification

- Initial red expressions/readers, both parsers: `i20261018104419-72244-2349` (baseline `5fcc6f84` plus tests, commit `21fed52a`). Earlier diagnostic red: `i20261018103915-106128-3b60`.
- Added panic and range red, both parsers: `i20261018104923-86304-63bb`.
- Focused green, both parsers: `i20261018105352-53232-77e1` at `384ba8bc` (before final formatting and shared brace-reader convergence). Final-head full suite below includes all these tests.
- Neo4j references: `j20261018104546-61396-fce8` (nested maps, CASE, dynamic writes, OPTIONAL MATCH, EXPLAIN/PROFILE, write visibility); `j20261018104920-75080-d734` (panic query and range modes; final incompatible-static-type query intentionally errors); `j20261018105027-86104-dff5` (range values of mixed string/integer/null, deliberately not evaluated by Neo4j).
- Develop baseline known failure: `i20261018105449-83188-b545`, both parsers, `TestEvaluateWhereForContext_Branches` line 48. Baseline `e67ed1a0` is develop `16923aef` plus the exact head's test fixtures only; product code is unchanged. The existing #1384 regression is tracked separately and fixed by #1468.
- Final full `pkg/cypher`, both parsers, unsharded: `j20261018105726-63240-2b68`, complete: only `TestEvaluateWhereForContext_Branches` fails on each parser, identically to the develop baseline. All new tests pass. Default 273.8 s, ANTLR 366.3 s test time. Command: `bash sig/tnrun.sh go wt-rel-readers --parser both --shards 1 -- ./pkg/cypher -count=1 -timeout 90m`.
- Vet `i20261018105449-103892-799b` and real gofmt check `i20261018105449-72756-b896` passed before the final one-call convergence; final-head vet `i20261018105905-86280-5292` and gofmt `i20261018105905-75084-a032` also pass.
- Paired A/B: `a20261018105736-99416-70a9`, 10 rounds, dedicated lane, submitted `--no-wait`; queued. Baseline `wt-rel-readers-benchbase@e67ed1a0`, head `47000a40`. Both binaries contain identical test fixtures. Benchmarks: StatementRouting, MatchSemanticValidationCached, MergeUniqueKey, 1 s rounds. No other jobs held or moved.
- Superseded A/B jobs `a20261018104740-106124-9a2e` and `a20261018105429-89712-512b` were cancelled while still queued, with exact ID/base/head/label guards, via remote Go control jobs `j20261018105220-86424-ab59` and `j20261018105726-86924-833e`. Their terminal records state cancelled, exit 130. Earlier full-suite jobs were automatically superseded when this worktree advanced; they are not full-suite evidence.

Logs, reference queries and preparation notes are `sig/handoff/rel-readers-*`.
Confirmed compatibility findings are recorded in [#907](https://github.com/orneryd/NornicDB/issues/907#issuecomment-6248996254). Part of #1055.

All execution uses the TrueNAS clients; no local Go, Python, Node, Docker, builds, tests, formatting or benchmarks were run. Local operations are git, gh, file edits and tnrun clients.
