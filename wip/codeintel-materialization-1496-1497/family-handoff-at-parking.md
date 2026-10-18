# #1412 parked for 1.5.0 (2026-10-18)

## PR, heads, WIP
- PR #1412 (develop), draft, milestone v1.5.0, issue #1343 (v1.5.0). PR head `fix/fmem-retained-payloads` = **7f42f0ffa** (pushed; per-finding r1 fixes, superseded by the Rule 73 course change, kept as the PR head).
- WIP branch **`wip-1.5.0-1412-entity-holders`** = **b4874f8c2** (origin). `wip/…` names are impossible on this remote (a branch named `wip` exists), so the dash form, as #1391.
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

## Code-intel labelled count and relationship identity scan (2026-10-18, final orchestrator decision)

Bugs 1 and 2 from `sig/handoff/codeintel-three-bugs-scope-20261018.txt` both belong to this family. The final user/orchestrator decision supersedes the earlier standalone 1.4.4 count plan: both wait for 1.5.0 because they share MATCH's materialisation barrier and the tool has a correct, usable unlabelled-count workaround. Only the orchestrator may reopen the release decision if the tool agent reports that the workaround blocks usable operation. No standalone 1.4.4 count or general MATCH-streaming implementation was written. The family's existing Rule 73 gate status remains unchanged.

Exact query: `MATCH ()-[r]->() RETURN startNode(r).id AS a,endNode(r).id AS b,type(r) AS t,r.site AS site,r.props_hash AS hash`. The pinned RC 39e3f7647 (54k indexed nodes, 4096-byte bodies, 1.3M CALLS edges, 5 GiB cap / GOMEMLIMIT=4GiB) returns no rows before a 95.920167 s Bolt failure, supervisor exit 137 and cgroup oom_kill increment. Receipt: `sig/codeintel/compare-repros-39e3f7647e034ebe7bf23b067cd161a915700b98.json`; harness `sig/codeintel/compare-repros.py`. v1.4.3 also reproduces; its older full-graph 900 s timeout/~12 GiB observation is a different workload, not a measured isolated OOM kill.

Source: `pipelineApplyInitialTraversalMatch` calls `executeMatchWithRelationshipsWithPathSeeded`, `findPaths`/sequential/parallel traversal retain paths and entity references, and the adapter builds binding maps before Bolt's `pipelineStreamReturn` projects scalar rows. Fetch size therefore cannot bound retained upstream MATCH inputs. Endpoint `.id` projection also loads ordinary node properties. Develop 16923aef7 improves endpoint memoization/redundant edge reads but keeps this materialization barrier. Develop's large runtime/peak has not been remeasured.

Explicit acceptance: successfully stream ALL scalar identities from the exact 54k/1.3M/4096-byte query at the 5 GiB cap; verify rows/values and no OOM; memory must scale with fetch window plus explicitly bounded working caches, not relationship count or node bodies. A public-Cypher test on both parsers must measure retained heap/peak or allocations as graph size grows, stopping at the first fetch to expose upstream retention. Graceful memory-limit rejection alone does not satisfy successful bounded-memory streaming. Preserve transaction and namespace visibility and normal cancellation. Test outline saved in `sig/handoff/codeintel-identity-working-set-test.go.txt`; it is not a compiled test or a pass claim. No new job for this parked bug is authorized. The initial combined red probes were cancelled while queued when scope narrowed; no identity workload ran in this implementation turn.

Tracking issue for the identity scan: https://github.com/orneryd/NornicDB/issues/1497 (author mm0nst3r, bug, v1.5.0).

### Labelled-count evidence and joint parking record

Bug 1 tracking: https://github.com/orneryd/NornicDB/issues/1496 (author mm0nst3r, label bug, milestone v1.5.0). Bug 2 is #1497 with the same author/label/milestone. No implementation PR was created for either; family draft #1412 stays at 7f42f0ffa and its existing entity-holder WIP branch remains separate.

Count query: `MATCH (:CodeIntel)-[r]->(:CodeIntel) RETURN count(r) AS n`. On exact RC 39e3f7647, the isolated 54k-node / 1.3M-edge / 4096-byte-body fixture at a 5 GiB cap / GOMEMLIMIT=4GiB OOM-kills the labelled query after 76.585732 s (exit 137, cgroup oom_kill increment); the unlabelled count returns the correct 1,300,000 in 0.006458 s. v1.4.3 reproduces (78.026297 s labelled OOM; 0.002727 s unlabelled). These measurements are the supplied receipts, not a new workload run. A count-store counter cannot represent arbitrary endpoint-label intersections, so declining that shortcut is correct. The fallback accumulates the same traversal paths and binding maps as bug 2 before aggregation. Source presence on develop is established; its separate large runtime/peak is not measured.

Explicit count acceptance for the integrated 1.5.0 correction: execute the exact full-shape labelled query successfully with 1,300,000 under the isolated 5 GiB cap; add public-Cypher memory/peak or allocation assertions on both parsers while increasing edges at fixed node/body size, so scalar aggregation cannot retain a PathResult/binding-map list proportional to the input. Keep distinct endpoint labels, direction/self-loop cardinality, namespace and transaction visibility correct, plus the existing unlabelled counter behavior. A total unlabelled count is the tool's current correct workaround, not a general answer for arbitrary label intersections. Collect paired A/B only when the family is resumed. Bug 2's separate successful bounded-memory streaming acceptance above remains required; graceful over-budget failure alone does not meet it.

Pushed probe/evidence snapshot: `6a0dd4aa83d2f65f55ef76daebc5f82e17169d24` on **`wip-1.5.0-1412-codeintel-materialization`**, based on develop 16923aef7. Archive: `wip/codeintel-materialization-1496-1497/` (README.txt, public count/semantics/benchmark probe, identity working-set outline, original reproduction notes, scope/paused history, remote harness text, and RC receipt with free-form server-log text omitted). Probe source is stored with `.go.txt` / `.py.txt` suffixes and is neither compiled nor accepted. Provisional 32 MiB count-allocation and 8 MiB identity-retention ceilings have not run and must be calibrated on resumption; no red/green claim is made. The initial design and hook-order notes are historical, not implementation approval.

All own jobs were cancelled WHILE QUEUED, with no runs/results: mixed probes `i20261018154359-92312-e2a4`, `i20261018154400-92312-2e33`, `i20261018154417-47712-cc42`, `i20261018154420-47712-9190`; count-only probes `i20261018154547-79344-7995`, `i20261018154548-79344-1e01`, `i20261018155021-101336-e063`, `i20261018155023-101336-4da6`. Cancellation returned all under cancelled and none under notQueued. Own waiting clients were stopped after verification. No owned test, A/B or verify job remains. Resubmit only after explicit 1.5.0 resumption, adapting the probes to the integrated implementation and test-economy rules.

Done: source analysis, supplied-runtime evidence, both bug issues, joint acceptance criteria, preserved probes and WIP snapshot. Half-done production code: NONE. Next: the orchestrator resumes F-MEM under its existing Rule 73 gate/owner, integrates both cases, then obtains public red/green and paired A/B evidence. Bug 3 is assigned to the separate `fix-nornic-rel-index-seek-20261018` implementer and is not part of this parked work. No open implementation permission/dependency here beyond that explicit family resumption. Keep local `wt-ci-stream-dev`, `wt-ci-stream-144`, `wt-ci-scope-dev`, and `wt-ci-scope-rc`; the identical release probe copy is retained. Nothing useful or any running job is at risk.
