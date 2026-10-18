# #1412 parked for 1.5.0 (2026-10-18)

## PR, heads, WIP
- PR #1412 (develop), draft, milestone v1.5.0, issue #1343 (v1.5.0). PR head `fix/fmem-retained-payloads` = **7f42f0ffa** (pushed; per-finding r1 fixes, superseded by the Rule 73 course change, kept as the PR head).
- WIP branch **`wip-1.5.0-1412-entity-holders`** = **83c92a72f** (origin). `wip/…` names are impossible on this remote (a branch named `wip` exists), so the dash form, as #1391.
  - 8dad290cf: entityHolder first pass (does **not** compile; see half-done).
  - 83c92a72f: Rule 73 material under `wip/rule73-1412/` (build tag `rule73probe`): ANALYSIS.md, inventory extractor, graphify reachability script, self-attack matrix, inventory and probe outputs.
- Local worktrees kept: `wt-fmem-retained-payloads` (on the WIP branch), `wt-fmem1412-test` (detached 7f42f0ffa, with the two probe files untracked in pkg/cypher), `wt-fmem1412-base` (develop e13815677), graphify copy `C:/Users/mm0ns/Desktop/codex/builds/repos/nornic-wt-fmem1412` and graph root `/graphs/nornic-wt-fmem1412` (remove both when the PR merges).

## Rule 73 gate status
- Rule 73 active for F-MEM since review r1 (sol-1412-r1.md, CHANGES, 4 findings) of course change 1. Gate (scratch1012/rule73-gate.txt): analysis written, **not yet checked by the orchestrator; no code may follow until it is.**
- Analysis: `sig/handoff/fix145-1412.md` sections "Rule 73 family analysis" (inventory, cause, plan) and "Self-attack results" (below, copied there), same text in `wip/rule73-1412/ANALYSIS.md` on the WIP branch. Inventory: `sig/handoff/fix145-1412-inventory.tsv` (15,132 lines), reachability `scratch1343/graphify-reach.tsv`.
- Proposed plan (awaiting approval): one `entityHolder` per structure that keeps entities (charged on start, follows what it holds, dropped once), one sizing function, a go/types-derived guard with a checked-in holder inventory, and the self-attack matrix as the regression test. Scope question open: transaction-lifetime state (`txNodeLookupCache`) cannot be owned by a statement because the per-transaction limit resets per statement.

## Done / half-done / next
- Done: rebase onto develop e13815677; course change 1 (held* reads, name-derived guard) on 36629ecc7/f9e63a474; r1 point fixes f5f13bad8/7f42f0ffa (superseded); mechanical inventory (typed extractor + graphify union); self-attack on 7f42f0ffa (default parser complete for 28/30 members, ANTLR part 1 complete, part 2 see jobs); cause and plan written.
- Half-done (WIP 8dad290cf, does not compile):
  - `pkg/cypher/transaction_memory_retention.go`: entityHolder type (set/grow/drop/sibling/giveTo/follow/failed, held* read methods, heldNodeList on holders, observeEntityHolder test hook) replaces the old held* methods on memoryRetention (reserveCandidates/holdAppended/heldMergedIDs removed).
  - 38 files mechanically converted (`*memoryRetention`→`*entityHolder`, `trackedRetention`→`holdEntities`, `release`→`drop` for holder variables); procedures holding nodes + relationships at once got a second holder (call.go callDbPropertyKeys, apoc_load_export.go x2, call_query_statistics.go, call_schema_visualization.go).
  - Not yet converted / still to model per structure: reserveCandidates/holdAppended/reserveSlots/reserveCacheKey call sites (count_store.go:188, match_index_order_groups.go:114/129, executor_mutations_relationship_delete.go:138, executor_mutations.go:164, match_index_seek.go:2143/2155/2165, pipeline_row_stream.go:366/370/434, pipeline_executor.go:1775, relationship_merge_identity_cache.go:84/96, unwind_relationship_merge_batch.go:430/507), subquery_value_expression.go and pipeline_match_product.go (mixed holder + retention, untouched), seeks' ID sets (sibling holders), caches via giveTo, fast aggregations, path search sizing, the guard test rewrite, test-file signature updates.
- Next, in order: (1) orchestrator checks the gate analysis; (2) implement the plan on the WIP branch (finish the holder conversion per structure; cover the unreviewed members: path search state and its sizing, TraversalContext.nodeCache/usedEdges, deleted-entity sets, APOC expansion sets, GDS/link-prediction sets, QPP state, statement property views, degree count / existence checks, derived structures in fast aggregations and algorithms); (3) replace the name-based guard with the type-based one + checked-in inventory; (4) self-attack matrix as regression test on both parsers -race, full ./pkg/cypher both parsers, paired A/B (no 5% gate; state remaining cost); (5) move the PR head to the result, update the body, request the Codex review.

## Self-attack results (head 7f42f0ffa; R73 lines in the job outputs)
Classification note: an over-limit run whose error text is "… db.memory.transaction.max threshold reached" is the limit's error (over=ok); the first run's code check missed it.
| Member | default i20261018111618-21544-1677 (-race) | ANTLR part 1 i20261018121658-15188-132e (-race) |
|---|---|---|
| pipeline label scan candidates | over ok, near ok, zero ok | same |
| index seek (pattern props / WHERE eq / IN / explicit tx) | ok ok ok | same |
| label scan in explicit transaction | ok ok ok | same |
| MERGE label-scan candidates | ok ok ok | same |
| MERGE relationship identity cache | ok ok ok | same |
| relationship-type seeds | ok ok ok | same |
| deleted-entity sets via DETACH DELETE | over ok **only through the rows' charge**; the ID sets themselves are uncharged (not isolated) | same |
| undirected type count | over ok; near FAIL = probe estimate low (seen-ID set is live beside the list), not a double count | same |
| fast single-hop aggregation | over ok; near FAIL: route materialises 30,000 reserved paths (live), inconclusive | same |
| DELETE … LIMIT 10 | no error over: streams (the hot path is dead code); probe premise wrong | same |
| one-hop traversal count from a hub | **no error over 960 KB**: undetermined (streams, or nodeCache/usedEdges uncharged) | same |
| path search adjacency cache + frontier | **no error over 960 KB: under-charged** (own accountant, 8 B/relationship) | same |
| existence check on a hub | over ok, near ok | over ok, near ok (part 2) |
| degree count `COUNT { (h)-[:FAN]->() }` | **no error over 960 KB: 30,000-relationship list uncharged** (reviewed one-node exemption) | same (part 2) |
| pattern comprehension / QPP / APOC path expansion over a hub | over ok (rows/own retention), near n/a | same (part 2) |
| OPTIONAL MATCH temporal loader, no match | no error: streams, keeps nothing (premise wrong) | same (part 2) |
| db.labels / relationshipTypes / propertyKeys / schema.nodeTypeProperties / stats.retrieve | ok ok ok | same (part 2) |
| fulltext queryNodes, APOC wcc input | over ok, near n/a | same (part 2) |
| batch prefetch (statement level) | not reached (job hit the 1 h cap at member 29) | **not completed**: part 2 stopped with term-stale at 55 min; must be rerun (smaller row count) |
Zero (pools empty) held on every run. Members not probed: statement property views (needs a decay-enabled engine), txNodeLookupCache (transaction scope), GDS/link prediction sets, dead-code functions (listed in the analysis).

## Jobs
- Inventory: i20261018110812-102936-090b (stack overflow in the extractor, fixed), i20261018111055-53348-21ec, i20261018120144-79724-6d4a.
- Self-attack: default i20261018111618-21544-1677 (exit 143 at the 1 h cap, 28/30 members reported); ANTLR part 1 i20261018121658-15188-132e (exit 0); ANTLR part 2 i20261018121700-104052-707f: stopped at park with `admin term-stale` (exit 143 after 3281 s), 13/15 members reported (output kept: wip/rule73-1412/attack-antlr-part2-i20261018121700-104052-707f.txt); rerun the batch-prefetch member.
- Earlier: red i20261018012743-88360-a1b0, i20261018102342-103364-379c; green i20261018031408-67880-b21a, i20261018102818-18704-3877; full pkg/cypher j20261018032616-46588-*, j20261018104310-81164-3a4e; vet i20261018030220-72116-1a5e, i20261018102824-100740-6eb2; gofmt i20261018030303-79712-09ee, i20261018103004-87812-b26b; develop baseline TestEvaluateWhereForContext_Branches i20261018032900-57596-1e52.
- A/B: a20261018030338-41564-3550 (develop vs 36629ecc, done: explicit_tx/property_lookup +7.1% p=0.001, rest ~); a20261018094745-79160-790a (cancelled by the orchestrator); a20261018103016-81136-78c2 (develop e13815677 vs 7f42f0ffa, 10 rounds, completed at park): **no row significant** (all p ≥ 0.089); explicit_tx/property_lookup −4.3% (p=1.0), so the earlier +7.1% did not repeat; lane noise high (foreign CPU up to 18%). Allocation bytes to look at: MergeUniqueKey/merge-unconstrained +64.8% B/op, unwind-merge-100 +12.3% B/op (allocs ±1%). Table: wip/rule73-1412/ab-a20261018103016-81136-78c2.txt.
- Cancelled at park: none were still queued (`admin cancel-ids` reported both A/B ids "notQueued": one running, one already cancelled).

## Open questions / dependencies
- Gate approval (inventory complete? cause real?) before any code.
- Scope: transaction-lifetime entity state (`txNodeLookupCache`) is outside the per-statement limit by construction; keep out of scope or change the limit's meaning?
- Dead code reachable from no statement (DELETE/rel-delete hot paths, ExecuteOptimized family, evaluateRelationshipPatternInWhere, ApplyIndexHint, seedNodesFromOuterMatch, evaluateMatchClauseNodes, tryFastRevenueByProduct): leave untouched or propose removal to the maintainer.
- Conflicts with #1391 (call_compat, match_index_seek, pipeline_executor, pipeline_row_stream) and #1385 (executor_subqueries + seed-node tests): both also parked for 1.5.0; whichever lands second rebases.
- #1412 on release lines: #1368's accountant exists only on develop (user decision: 1.5.0).
