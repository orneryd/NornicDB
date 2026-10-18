# #1384 runtime owner: overlap and release prerequisites

18 October 2026. This is a pre-implementation mapping against the approved Rule 73 plan, the completed analysis at the end of `fix145-1385-1408.md`, its `scratch145/r73` graph/probe results, the release-reconciliation rows 21–34, and the source diffs for all fourteen groups. It records planned ownership, not delivered release parity.

## Ownership ruling

The CALL family A in #1385 and the static half of #1468/#1459 are the same causal unit. `semanticScopeWalk` is the **single static owner** of free outer reads and per-branch declarations, using the existing expression/local-scope readers and recursing through UNION, nested expressions and CALL. #1385 supplies and caches that metadata and injects it into Fabric. This task will not add a second walk, a CALL-only reader, or a new import-selection policy. Its compiled graph-expression plan records the original form and consumes the supplied free/local/import metadata.

This task owns the **runtime frame**: name presence versus null value, current locals versus persistent imports, horizon retention, the current transaction/view/error context, strategy admission after frame resolution, producer requirements from the compiled contract, and expansion/accounting. Static metadata identifies imports and branch locals; runtime resolves their values. CALL's genuine boundary selects a frame using that metadata. EXISTS/COUNT/COLLECT retain their expression imports through their whole body.

The overlap is evidenced by the CALL analysis A5/A6 and its decision-point inventory: `semanticScopeWalk`, `expressionFreeVariables`, runtime/static import carry, expression grouping and Fabric record bindings. Its D08 comprehension-local/inner-COLLECT failure, R05/R20 nested UNION failure and F04 all-column Fabric import reproduce the exact binding/declaration distinction that this task's S02/S03/B01/B02 and routed controls exercise. R12/R16 show why a runtime merge alone cannot substitute for static metadata. #1408's typed identity/output-rendering family is a different concern and is not absorbed.

The user has explicitly assigned these two halves and will forward #1385's owner API note. Runtime work can proceed against an interface returning immutable body scope metadata; static integration waits for that API. The implementation will not guess its public API or reimplement its analysis.

## Release mapping, rows 21–34

"Prerequisite" below identifies the service/contract needed before final release integration. It does **not** demand wholesale cherry-picks or create a second scope carrier. In particular, final static/type-fact services must be coordinated with #1385's release API; portions involving static horizon retention cannot be independently patched into a competing frame. #1345/#1354/#1356 are one final-state type-fact port, as the reconciliation already recommends. Dependency lists for those three express one combined unit, not a cyclic sequence of ports.

| Row / source PR | Disposition for the release runtime frame | Concrete boundary |
| --- | --- | --- |
| 21 / #1330 | **Static prerequisite, supplied by #1385** | Procedure YIELD outputs must be in the subquery scope; grouped identity projections and outer-name redeclaration rules come from the static owner. Native regression suite accompanies its release delivery. |
| 22 / #1331 | **Partly subsumed** | This frame carries hidden imports outside DISTINCT/grouping keys and preserves them for ORDER BY. General value-equivalence-index and APOC/collection changes remain an independent port, not a frame prerequisite. Release already has `cypherEquivalenceKey`; reuse that canonical policy. |
| 23 / #1335 | **Independent; no frame prerequisite** | Fixes native/map-wrapped path hashing in #1331's new index. Release has no index. Port with that independent index change; do not introduce another path identity policy in the frame. |
| 24 / #1337 | **Static prerequisite plus runtime subsumption** | #1385 supplies exact CALL */()/named imports and static retention. This task consumes the same metadata, carries runtime values and replaces release's `pipelineRowOutput.carried` / rewritten WITH policy. |
| 25 / #1345 | **Type-fact prerequisite, one port with #1354/#1356** | Agreed `nonInteger` and deferred-predicate facts must survive the canonical type join. The frame transports the result; it does not invent joins. |
| 26 / #1347 | **Lexical prerequisite** | Shared comment/gap/qualified-name and dotted-keyword readers feed original-form capture and body analysis. Coordinate with #1472's declaration reader; no frame-local scanner. |
| 27 / #1354 | **Type-fact prerequisite, combined final state** | CASE type alternatives and function compatibility stay in the type owner; the shared static API/frame carries them. Include the final exclusion/deferred facts rather than the transient reverted state. |
| 28 / #1356 | **Type-fact prerequisite, combined final state** | Final restoration of #1345 facts after #1354. Do not replay #1351's temporary regression or create another import-only join. |
| 29 / #1357 | **Subsumed in compiled graph-form recognition** | Canonical subquery keyword/body boundaries include comments/form feed; aggregate/map-projection/WHERE classifiers consume that recognition so inner aggregates stay inside the body. Retain its oracle/regression family. |
| 30 / #1359 | **Native scalar-evaluation/error prerequisite** | Logical folds, operand isolation and deferred-operation errors remain in their existing canonical owners. Graph-expression adapters use them; no local replacement for error policy. Its graph/subquery scope entries must use #1385 metadata and this runtime frame. |
| 31 / #1361 | **Partly subsumed** | One compiled/cached graph body and statement clause-plan contract, shared with the static API. DELETE guards and unrelated index-seek/traversal planning remain separate parity work. |
| 32 / #1380 | **Partly subsumed** | Graph-body UNION/grouping/reference/result-tail facts come from the shared body contract. Unrelated scan optimisations and result-modifier relocation remain separate. Release's live `parseIntModifier`/ordering helpers must not be deleted through a wholesale file copy. |
| 33 / #1388 | **Partly subsumed** | Prepared and unprepared graph-expression inputs resolve through the same frame, preserving already-bound values without per-adapter copies. Generic aggregate map compilation and GDS-only adapters remain separate. |
| 34 / #1395 | **Native matching strategies subsumed** | EXISTS/COUNT hop/chain/node strategies, sparse-end choice, endpoint/equality reads and zero-hop matching sit beneath one scope/strategy entry. Fold the native kernels and tests; this does not port #1381's separate memory framework or GDS features. |

**Scheduling request:** deliver the #1385 static API on release with #1330/#1337 semantics; schedule the shared lexical #1347 port, one final-state type-fact port for #1345/#1354/#1356, and the native scalar/error owner from #1359. The runtime can be developed on develop while these services are prepared. The partial rows must not be marked wholly covered by #1459: their independent remainders still need their own ledger disposition/port. General #1331 index/#1335 work can proceed independently; it is not required to build another equivalence implementation here.

## Ledger and evidence

Each of the fourteen existing ledger entries now has `patternPredicateIntegration` containing the planned role, scope, dependencies and proposed consumer PR #1459. **`status` is `planned-not-implemented`; existing `releasePR`, `class`, patch matches and shipped-coverage evidence are untouched.** This deliberately does not make the parity checker green or claim that an open PR already contains a source group. Upgrade a mapping only after the final source and tests establish its coverage; leave partial remainders explicit.

Machine-readable mapping: `fix-1384-overlap-parity-mapping.json`. Source snapshot and changed-file evidence: `fix-1384-overlap-parity-source.json`, `fix-1384-overlap-parity-stats.txt`. Commands used: `git diff <first-group-sha>^ <last-group-sha> --stat -- pkg/cypher`, followed by the actual production diffs for scope/import/type/boundary/prepared-row owners. Exact merged SHAs and code excerpts remain in the reconciliation ledger. The source inspections compare the unchanged gate heads `fc375efa4` / `4bc185858` and the pinned reconciliation snapshot `16923aef7` / `24d08776c`.

No production edits, ports, tests or benchmark submissions were made to establish this mapping. Earlier gate jobs are terminal. Local graphify remains the code-analysis service; execution remains on TrueNAS.
