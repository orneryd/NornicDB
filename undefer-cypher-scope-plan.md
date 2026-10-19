# U4 cypher-scope undefer plan and running handoff

Date: 2026-10-19. Binding brief: brief-undefer-common.md, read completely before work.
Release reproduction base: b4bee5d5e4c58edf66c765066cb6ebeac9bdbdf8.
Dedicated test-only worktree: wt-undefer-cypher-scope-red; branch wip-1.4.4-cypher-scope.
No implementation changes made before this plan. All execution uses TrueNAS clients.

## Scope and acceptance

Bring confirmed release bugs back to 1.4.4. #1468/#1459 integrated graph-expression correction remains 1.5.0; do not copy unfinished frame scaffolding or revive closed #1459. #1385's static import owner remains in scope. Conditional fixes #1335/#1345/#1356 travel with their parent owners. Never report a source inference as a reproduced failure. For every release-green item stop its port and record evidence. Do not replay #1351's accidental revert. Preserve release identity, declaration, relationship-seek and remote-path owners; exclude develop-only GDS and memory-accountant code.

## Reproduction matrix (populate results before production ports)

Every row runs default and ANTLR on the pinned release base. Public Execute and explicit-transaction paths first; Bolt/HTTP focused probes when normalization or route contracts matter. Compare disputed values/status codes with Neo4j 5.26.30 through tnrun cypher neo-ref, one file per isolated case when an error would terminate a batch.

| Item | Release probe population | Conditional / measurement |
|---|---|---|
| #1330 | procedure_yield_subquery_scope_test; empty/populated YIELD WHERE EXISTS/COUNT/COLLECT/comprehension; grouped identity | existing develop regression tests, no production overlay |
| #1331 | scoped_call_distinct_test; projected DISTINCT vs hidden persistent imports, ordering and horizons | #1335 native/wrapped path identity follows parent |
| #1335 | public path equivalence population plus owner-presence inspection | new index absent from release; carry if #1331 ports |
| #1337 | call_star_scope_test; CALL (*) explicit/implicit imports, rebinding, empty aggregate types | carry #1345/#1356 final join facts |
| #1345/#1356 | aggregate_import_noninteger_test; invalid indexes with agreed nonInteger/predicateDeferred facts | new join absent; public outcomes still checked |
| #1347 | qualified comment/plugin lexical tests; spaced/dotted keyword properties and comments | reconcile newer release declarations |
| #1354 | CASE alternatives in issue_1226_runtime_regression_test, empty/populated and transaction paths | compare static rejection codes, not message text |
| #1357 | aggregate_subquery_comment_boundary and form-feed tests; normalized/original forms, embedded/Bolt/HTTP | release #1323 already covers some cases |
| #1359 | subquery_evaluation_order, null slice, row-subscript, logical-version residuals | port only release-red residuals beyond #1240 |
| #1361/#1341 | correlated CALL with 1 vs many outer rows, clause-plan benchmark public Execute | baseline vs head paired ABBA, ten rounds |
| #1380 | remaining logical/seek/modifier correlated clause benchmark population | same paired A/B owner batch as #1361 |
| #1384 | pattern owner/operand probes plus saved release family cases | distinguish absent develop consolidation regression from existing family; keep frame-dependent correction parked |
| #1388 | aggregate map arguments and prepared-scope preservation; public Execute | ten-round paired A/B with compiled map benchmarks |
| #1395 | correlated EXISTS/COUNT/quantified paths, bound endpoints, equality/read-count probes | ten-round paired A/B; preserve relationship-seek owner |
| #1385 | 51 call_imports bodies across unscoped/scoped/IN TRANSACTIONS, auto/explicit; nested locals, UNION YIELD | static owner + Fabric import consumers; fix known body-union/branch-read regression, no graph frame |

## Owner groups and dependency order

Provisional boundaries, refine using graph + source before each change:
1. Static procedure bindings (#1330), projection/equivalence (#1331/#1335), CALL static/runtime type imports (#1337/#1345/#1354/#1356). These have actual dependency edges; do not split a required owner or ship intermediate regressions.
2. Lexical token/gap/subquery classifier owner (#1347/#1357); retain release #1472/#1485 declarations.
3. Runtime row operation owner (#1359 release-red residuals) after relevant static/lexical corrections.
4. Statement clause-plan owner (#1361/#1380), followed by aggregate map compilation (#1388), then correlated access owner (#1395). Performance results need measured paired A/Bs, not historical numbers.
5. Static free-read/declaration owner (#1385), including runtime/static/importing-WITH/Fabric consumers, adapted to current release planner. Develop correction also required because parked head has a known UNION YIELD regression and incomplete Fabric integration.
6. #1384 disposition after current-release repro: no blind pick of the regressing consolidation; do not cross into explicitly parked integrated runtime frame.

Use cherry-pick -x unchanged wherever possible, document every conflict/adaptation. Several release PRs by owner, each independently reviewable; when genuine code dependency prevents independence combine that causal owner, rather than duplicate or manufacture feature dependencies. All release PRs DRAFT, base release/v1.4.4, suffix (v1.4.4), bug label and v1.4.4 milestone. Link every PR to parent T3 thread. No merges or ready-marking.

## Verification and preservation

Commit finalized tests, establish red on pinned baseline, then final implementation green plus matching changed-package -run tests, both parsers. Only contract-dependent package tests. One batched result collector per batch; no duplicate job watchers. No local workloads, new exact-head ingests, broad suites or unrelated queues. Use existing codeintel baseline graphs + rg. Tests/outputs remain outside repository except durable fixtures/tests. Update this handoff and push wip-1.4.4-cypher-scope at meaningful checkpoints (explicit brief overrides old no-WIP-push rule). Update parity mapping only after concrete PRs exist; run release-parity and keep CLEAN.

## Initial code-intelligence evidence

2026-10-19: symbols/callers/families semanticScopeWalk on release/v1.4.4 returned graph commit 24d08776cdb9cb624da1073007da437306518426, analysis_revision bb1cdb34ec9314523383d9c143cdbe2ae4ac479bb2818f8464e218f2a88ce303, server 5c3c10d69147d18824acca036920e08cee3f42ca. It is an older baseline, not exact b4bee5d5e. Validate changed source with git diff/rg. Direct callers: validateMatchSemanticScopesUncached, validateSubqueryPatternBindings, callSubqueryReturnTypes; recursive shared walker. Families first page is truncated; complete pagination required before claiming full closure. Saved central results c1792411728521-3bcccf4b, c1792411744225-111302f5, c1792411744449-c3b87a12.

## Progress

- Read common brief, audit in-scope sections, parked #1468/#1391 and #1385 notes, working/testing rules, release AGENTS and develop Rule73/standing design principles.
- Dedicated pinned-release reproduction worktree created. No production edits yet.
- Reproduction jobs: none submitted yet. PRs: none. No item disposition established yet.

### Checkpoint 1 (12:17 UTC)

Test-only WIP pushed at 1fe65f7d1 (last own pushed SHA); no production changes. Initial imported test population had compile-only dependencies on newer internal APIs; removed those internal assertions and retained public-path tests. Compile-only attempts i20261019121212-70960-5782 / i20261019121216-70960-1537 and i20261019121404-54644-d0fb / i20261019121408-54644-6b5b are not red semantic evidence. Corrected matrix running: i20261019121538-612-9234 default, i20261019121543-612-57ce ANTLR. ONE collector process, log undefer-cypher-scope-red3.log.

Neo4j 5.26.30: j20261019121417-5396-b82c success for YIELD subquery, scoped DISTINCT, CALL(*), dotted boolean keyword, commented COUNT, null upper slice and valid mixed CASE. j20261019121450-5940-ec83 rejects CASE list alternatives on empty input; j20261019121522-65736-ce16 rejects Float-or-Duration aggregate-import index on empty input. These confirm expected semantics, not NornicDB outcomes.

Codeintel complete reverse closure semanticScopeWalk: 447 files / 3666 functions, truncated=false, same pinned graph metadata as above. Direct queryGapEnd, callSubqueryImports, projectTypeScopeIn caller inventories saved as undefer-cypher-scope-*-callers.json, all untruncated. Static scope walker/types files unchanged between graph base 24d08776 and requested release b4bee5d5. Graph's broad recursive reverse closure includes clients; causal owner boundary remains walker + its type/import consumers, not all reachable packages.

### Checkpoint 2 (12:28 UTC)

All initial release matrix jobs terminal. Default i20261019121538-612-9234 and ANTLR i20261019121543-612-57ce show matching family failures. Null/scalar-slice population passes both and is NOT ported. Aggregate map result values also pass; its issue is repeated work and remains a performance investigation. Node-lookup and optional-null semi-join comparison controls pass; bound pattern consumer test fails, so do not attribute all #1395 family controls to one optimization.

Draft release PRs opened and linked to this child T3 thread:
- #1546, fix/1.4.4-cypher-yield-scope @2df988a6505ade97fc9c04b976bfb17e85d4276b: #1330 three unchanged -x picks. Focused green default i20261019122009-70928-9c11, ANTLR i20261019122015-70928-f934. Dups retry c1792412693213-f21254c2 COMPLETE, detector_exit=0, analysis_complete=true, no truncated inventory (first try got busy/retry-after30). WIP backup wip-1.4.4-cypher-scope-yield pushed at same head.
- #1547, fix/1.4.4-cypher-lexical-scope @8a5567f79b0b6f627b8ba229d8da582f9273cdfe: #1347/#1357 eleven unchanged -x picks. Focused green Cypher i20261019122050-37468-fae0 / i20261019122054-37468-892f; Bolt i20261019122050-37468-a907 / i20261019122058-37468-0d57; HTTP i20261019122051-37468-0a69 / i20261019122054-37468-b4f2. WIP backup wip-1.4.4-cypher-scope-lexical pushed at same head.
Parent orchestrator 075cf8e3-9632-45e4-a74b-ecbe4e778f84 notified; independent review requested while remaining implementation continues. No merges/ready marking. Paired costs pending; PR bodies say so.

Projection/type owner local branch fix/1.4.4-cypher-projection-scope @97a9dc29a includes #1546 prerequisite plus #1331/#1335/#1337/#1345/#1354/#1356. Testing now: i20261019122614-54824-c346 default, i20261019122618-54824-e3ea ANTLR. Adaptations: changelog keeps release mapping note, omits develop-only test-hygiene note; #548e07 keeps already-ported #1345 exclusions beside union flag; #1356 intentionally empty -x provenance commit because final state already present (temporary #1351 revert omitted). No production behavior from a future graph-expression frame copied. PR must avoid duplicating #1546 once parent merges it.

Runtime host restarted during a tool call; checked actual worktree state before resuming. Initial collector had downloaded default red only; remaining red and first green jobs collected ONCE together into undefer-cypher-scope-collect4.log. All those jobs terminal. No lost/resubmitted remote work.

Fresh Neo4j import matrix j20261019121834-37108-6733: every non-error case from 51-body fixture ran successfully; raw results undefer-cypher-scope-neo-import-matrix.log. Error cases retain pinned fixtures and separate focused error controls.
