# Issue Correction Plan — 2026-09-28

Single-branch sweep of the open correctness issues. Details pulled from GitHub via
`gh issue view` on 2026-09-28 (bodies consolidated into `/tmp/nornic-issues/*.json`).
Reference database: `neo4j:5.26.30-community` (the pinned differential image).

## Priority order and assignments

| # | Issue | Severity | Status |
| --- | --- | --- | --- |
| 1 | #741 explicit-tx COMMIT failure + (label,type) counter drift | CRITICAL: data lost, counts wrong | DONE (01c79665) |
| 2 | #648 CALL { } write subquery per-row semantics | CRITICAL: writes don't run per row | DONE (02592a57) |
| 3 | #514 MERGE `{k: a.id}` after WITH stores text; evaluator text-fallback | CRITICAL: silent wrong data | DONE (16f13151) |
| 4 | #640 MERGE whole-pattern creation + `findMergeNode` full scan | CRITICAL: wrong graph / O(N) per row | DONE (see §4) |
| 5 | #581 (reopened) shortestPath between bound end nodes | HIGH: wrong rows | DONE (see §5) |
| 6 | #728 WITH … WHERE in CALL bodies adds null rows; cartesian before WHERE | HIGH: wrong rows / OOM | DONE (30c255e1) |
| 7 | #745 element ids differ by route | HIGH: clients can't re-find entities | DONE (see §7) |
| 8 | #446 compressed ANN rescoring floor clamped by request limit | HIGH: silent recall degradation | DONE (0bc50dc1) |
| 9 | #726 BulkDeleteNodes notification data race | HIGH: race, `-race` suite broken | DONE (a979b47d) |

## Per-issue plan

### 1. #741 — explicit transaction: staged relationship removal breaks COMMIT and counters

**Facts (from the issue):**
- `CREATE (:B)-[:U]->(:C)` then `MATCH (n:C) DETACH DELETE n` in one explicit tx →
  `commit failed: invalid edge: start or end node not found`; whole tx rolls back.
  Neo4j commits (1 node, 0 rels).
- `DELETE r` + `DELETE n` (endpoint) → same failure.
- DETACH DELETE in a tx leaves the positional `(label, type)` counter (`EdgeCountByStartLabel`)
  one too high after commit; drift compounds across runs. Counters come from #638.

**Root cause to verify:**
- `deleteNodeBuffered` → `deleteEdgesWithPrefixBuffered` reads the adjacency index as
  the Badger txn sees it and misses relationships staged (created) in the same tx.
- A staged relationship deleted with `DELETE r` still reaches commit validation after
  one of its endpoints is deleted ("invalid edge: start or end node not found").

**Fix strategy:**
- Staged deletes must consider the tx's staged edges: when buffering a node delete,
  also tombstone every staged edge incident to it; when validating edge endpoints at
  commit, skip edges whose own staged delete is recorded.
- Counter deltas: subtract the staged-edge count from the (label, type) positional
  counters when the edges are removed in-tx (mirror the insert delta path).

**Tests:** the issue's three statements on explicit tx (embedded executor on a
namespaced in-memory Badger engine) + the counter-drift sequence asserting
`EdgeCountByStartLabel("A","U") == 0` after commit; run twice (determinism);
`-race`.

### 2. #648 — CALL (n) { … } write subqueries

**Facts:** `CALL (n) { SET n:B }` → SyntaxError "map literal keys must be valid
symbolic names"; `CALL (n) { SET n.y = 2 }` stores the write but the returned row
keeps the stale value (`{y: null}`); a write subquery without imported variables
runs once instead of per row; clauses after `CALL { }` rejected; `CALL { } IN
TRANSACTIONS` accepted inside an explicit transaction (its writes survive ROLLBACK
— re-verify today).

**Fix strategy:**
- `SET n:B` label-form must parse in the subquery body (the label-set parser rejects
  `n:B` as a map key).
- After a write subquery, refresh the outer row's node value from storage so the
  projected property reflects the write (row staleness fix).
- Ensure write subqueries run per outer row (not once), including the no-import form.
- Reject `IN TRANSACTIONS` inside an explicit transaction per Neo4j.

**Tests:** the issue's 4 statements (auto-commit + explicit tx) against Neo4j
expectations, plus the no-import per-row write case; deterministic, run twice.

### 3. #514 (remaining) — MERGE property expression after WITH; evaluator text fallback

**Facts:** core `+`/`/` routing fixed by PR #757 (cluster A). Remaining: `WITH a
MERGE (b {k: a.id})` in a statement that has a `SET` stores the text `'a.id'`;
the general evaluator fallthrough `// Unknown - return as string` in
`functions_eval_props_literals.go` still turns unevaluable expressions into text
(`SET p.x = 1 +* 2` stores `'1 +* 2'` — Neo4j rejects).

**Fix strategy:**
- Route MERGE property-map expressions through the same `evaluateScalarPropertyExpression`
  used by CREATE (converged in #757), including when a `SET` clause is present.
- Remove the evaluator's text fallback: an expression the evaluator cannot produce a
  value for is an error (semantic, `Neo.ClientError.Statement.SyntaxError`), never
  stored text. Keep quoted-string literal handling explicit.

**Tests:** `MERGE (b {k: a.id})` after `WITH` (with and without SET) stores the
value; `SET p.x = 1 +* 2` and `SET p.x = p.name..` error and store nothing; list
`UNWIND [1, 2 +* 3]` errors; three routes.

### 4. #640 — MERGE whole-pattern semantics + findMergeNode scan

**Facts:** `MERGE (a:T {id:1})-[:R]->(b:BC {id:2})` with an existing `(:T {id:1})`
must CREATE the whole pattern (two new nodes) like Neo4j; NornicDB reuses the
existing `T` (and builds self-loops for the self-referencing form). Bound-start
forms already agree. Also `findMergeNode` falls back to `store.AllNodes()` when the
label scan misses → O(N) per created row (85ms at 20k nodes).

**Fix strategy:**
- In MERGE relationship-pattern creation, when no relationship match exists, create
  every unbound node of the pattern (never reuse a node found only by matching one
  node pattern of the unbound side), matching Neo4j; bound-variable forms keep the
  existing get-or-create semantics.
- Replace the `AllNodes()` fallback in `findMergeNode` with a label-indexed lookup
  path that is authoritative (no global scan); keep a bounded consistency check only
  where a stale-index regression test demands it, without per-row global scans.

**Tests:** the 5 statements of the issue (nodes_created + graph afterwards), plus the
perf guard: `MERGE (:M6 {v:1})` with 20k unrelated nodes must not scan all nodes
(timing or call-count assertion), all routes.

**Status: DONE** (commit 83ca5225):
- Whole-pattern creation branch in `executeMergeRelationshipWithContext`: both
  endpoints unbound and different variables → search candidate pairs for an existing
  whole-pattern match; on miss create both endpoints fresh (`createMergeRelationshipEndpointNode`).
- Self-referencing `(a)-[:R]->(a)` keeps get-or-create self-loop semantics.
- `findMergeNode` `AllNodes()` fallback removed (#640/#694): label index + schema
  lookups authoritative; `mergeNodeIndexedCandidateIDs` used by `findMergeNodes`.
- Stale-label test rewritten to pin Neo4j-equivalent constraint-violation behavior
  (no silent O(N) recovery).
- Regression tests `pkg/cypher/gh640_merge_whole_pattern_test.go` (7 subtests, all 5
  issue statements + self-loop + call-count scan guard).
- Benchmarks: routing autocommit 161 allocs / 17.3-17.7µs, explicit_tx 88 allocs /
  11.0-11.4µs (within band).

### 5. #581 (reopened) — shortestPath between end nodes bound by an earlier MATCH

**Facts:** after PR #763, clause-only and anchored OPTIONAL forms work, but the
bound-end forms still fail: `MATCH (node:D)-[:R*1..3]->(x) MATCH p = shortestPath((node)-[:R*1..3]->(x))`
returns `[]` (Neo4j: rows); `MATCH (node:D), (x:M) MATCH p = shortestPath((node)-[:R*1..3]->(x))`
returns `[1, null]` (Neo4j `[1,2],[2,2],[3,2]`); the WHERE variant returns `[]`.

**Fix strategy:**
- `resolveShortestPathVariables` (`shortest_path.go`) resolves bindings from the
  *immediately preceding* MATCH only. Extend it to bind end variables from the row
  scope of the statement's previous clause chain (multi-MATCH / comma patterns /
  WHERE forms) instead of re-resolving patterns statically: the shortestPath clause
  must execute per input row with `a`/`b` bound from that row, like the pipeline.
- Route the bound-end form through the per-row BFS (the value-form evaluator path
  from #763) so every (a,b) row contributes its path or is dropped by the MATCH
  semantics of `p = shortestPath(...)`.

**Tests:** the four reopened statements on all three routes vs Neo4j, twice, `-race`.

**Status: DONE** (commit 26e3fb2d):
- `executeBoundEndShortestPath` (`shortest_path.go`): when one or both endpoints are
  bare variable references, the preceding clause chain seeds the row space
  (`prefix RETURN *`, executed once) and the BFS runs per input row. Path rows are
  projected through `pipelineApplyReturn`, so RETURN aggregation, implicit grouping,
  ORDER BY, SKIP/LIMIT and DISTINCT match the general pipeline exactly.
- Clause `WHERE` (e.g. `WHERE length(p) > 1`) filters per path with the shared path
  context; `allShortestPaths` bound-end form emits one row per path.
- `parseShortestPathQuery` WHERE extraction now requires the WHERE to follow the
  shortestPath call (previously a WHERE on an earlier MATCH leaked into the clause).
- Shapes outside the handler (anonymous path variable, no RETURN, no preceding
  clause, non-final MATCH) keep the single-shot behavior — no silent clause
  swallowing.
- Regression tests `pkg/cypher/gh581_shortest_path_bound_ends_test.go`: all 9 #721
  table statements + the 5 original #581 OPTIONAL statements + clause-WHERE and
  bound-end allShortestPaths pins.
- Verification: full cypher suite green, `-race` green, TCK ratchet 7794/7794,
  routing benchmark within band (autocommit 161 allocs / 17.5-17.7µs, explicit_tx
  88 allocs / 11.2-11.3µs).

### 6. #728 (remaining) — WITH … WHERE in CALL bodies; cartesian before WHERE

**Facts:** WHERE has ~25 implementations; this sweep fixes the open defects:
`WITH … WHERE` at the start of a CALL body adds null rows; a cross product is
materialized before WHERE applies (OOM at 10k nodes); non-boolean WHERE keeps rows
(may be fixed by cluster D work — re-verify).

**Fix strategy:**
- CALL-body `WITH … WHERE`: evaluate the WHERE per row of the WITH projection and
  drop non-matching rows (no null-padding) — route through the shared pipeline
  row filter (`filterPipelineRows`) instead of the subquery-specific
  `evalWhereNullGuard`/`splitLeadingWhereNullGuard` when they disagree.
- Where a cross product is still built before WHERE, push the predicate into the
  row generation (the 2026-09-27 predicate-pushdown commit covers MATCH; extend the
  same planner call to the CALL/UNWIND-join path if it doesn't already).
- Convergence: record remaining duplicate WHERE evaluators in #547 as before/after.

**Tests:** the issue's null-row and cross-product statements at 10k nodes (memory-bounded,
no OOM), three routes.

**Status: DONE** (see §6 fixes; commit follows §6 convergence):
- **Convergence (no new evaluation paths):** the strict/relaxed distinction lives
  inside the shared `evaluateRowPredicate*` evaluator as a `relaxed` mode flag
  (`evaluateRowPredicateMode`); `evaluateMatchWhereCondition` is a one-line entry
  into that same path. Final truth coercion in the shared fallthrough:
  bare reference or pure literal → TypeError; MATCH-position computed non-boolean
  → row filtered (WHERE `n.id + 'z'` → no rows); all other positions strict.
- CALL-body `WITH … WHERE` null rows: in `executeMatchWithCallSubquery`'s per-seed
  loop, a subquery that returns zero rows removes its outer row only when the body
  carried its own RETURN (`innerHadReturn`); write-only bodies keep the seed row.
- Importing-WITH validation: `CALL { WITH i WHERE … }` without a CALL import list
  and referencing an outer variable is a SyntaxError (Neo4j row 7); plain importing
  WITHs stay allowed.
- #692 cross product: `parseCartesianVarPropEqualityTerm` now parses integer-offset
  joins (`b.id = a.id + 1`, normalized both directions); `applyCartesianWherePushdown`
  shifts allowed value sets by the offset, and `buildCombinationsUsingWhereJoin`
  performs the offset hash-join with offset-aware residual checks — the offset join
  no longer materializes the full product.
- #599: measured GenericFallback 12.2µs (pre-#566 band 11.7-13.2µs) — the lift/lower
  regression is already gone on main; this sweep stays within noise of both
  WHERE benchmarks (A/B vs main: 12.2 vs 11.6-12.0µs GenericFallback,
  71.1 vs 70.8-78.9µs CompiledJoin).

**Convergence pass (earlier fixes folded into the shared paths):**
- §5 shortestPath: `executeBoundEndShortestPath` and `executeOptionalShortestPath`
  now share one per-row BFS (`runSeededShortestPaths`) and the existing
  `filterPathsByWhere`; only their row semantics (drop vs null) differ.
- §4 MERGE: `resolveMergeRelationshipEndpoint` and the whole-pattern branch share
  `createMergeRelationshipEndpointNode`; conflict re-find keeps `errors.Is`
  through the localized wrap.
- §1 storage: committed-edge (`deleteEdgesWithPrefixBuffered`) and staged-edge
  (`deletePendingEdgesIncidentToBuffered`) deletions share
  `bufferRemoveEdgeBookkeepingBuffered` — one removal-bookkeeping path.
- §6 WHERE: MATCH/WITH/YIELD all evaluate through the one flagged
  `evaluateRowPredicate*` path; no parallel evaluator was added.

### 7. #745 — one element id on every route

**Facts:** Bolt negotiates 4.4 → driver `element_id` is a hash no query finds; HTTP
row meta uses fixed `nornicdb` while `elementId()` uses the database name; an entity
through `CALL { USE … }` gets the default database's id instead of the constituent's.

**Fix strategy:**
- Produce one canonical element-id string per entity in the result layer:
  `<kind>:<database>:<id>` built from the database the entity actually lives in.
- Bolt: populate node/relationship element-id in the Bolt 4.4 structures via the
  driver-visible `elementId`/metadata the driver maps to `element_id` (check how the
  Python driver derives `element_id` on 4.4 — it uses the `id` field; verify whether
  we can attach the string there without breaking other clients), or negotiate Bolt
  5.x structures. Prefer the smaller change that makes `element_id` round-trip.
- HTTP `meta` elementId must use the database name from context, not the constant.
- Composite subqueries must keep the constituent identity through `CALL { USE … }`.

**Status: DONE** (commit follows §7):
- **Bolt (§1):** the handshake now selects the highest mutually supported version
  from the client's wire proposals (`selectBoltVersion`, [0x00, back, minor, major]
  parsing, manifest marker skipped) and echoes the exact proposal. Bolt 5.0
  negotiates for drivers that offer it; node/relationship structures then carry
  their element id as the final field (`encodeRecordListInto`/`encodeRecordValueInto`,
  B4 4E / B6 52, paths via `encodePathV5Into`). 4.x clients keep the 3/5-field
  structures unchanged.
- **HTTP (§2):** the request database name threads through
  `convertRowToNeo4jFormat` → `nodeToNeo4jHTTPFormat`/`edgeToNeo4jHTTPFormat` →
  `generateRowMeta` (id parsing is prefix-agnostic), so row meta and values name
  the database the entity lives in.
- **Composite (§3):** `evaluateRowEntityIdentity` resolves id()/elementId() at the
  context-aware row boundary (the allocation-conscious row evaluator carries no
  ctx); `entityIdentityDatabase` names the constituent that holds the entity via
  the new `CompositeEngine.ConstituentDatabaseForNode` when the execution database
  is a composite. `fn.Context.Database` and `resolveBindingExprWithRelationships`
  use `executionDatabaseName` (ctx USE database first).
- Tests: `pkg/bolt/gh745_element_id_test.go` (wire-format negotiation + V5 struct
  encoding), `pkg/server/gh745_element_id_test.go` (db-named meta), and
  `pkg/cypher/gh745_element_id_test.go` (composite subquery identity + round-trip).
- Verification: full bolt/cypher/storage/fabric/server suites green; routing
  benchmark in band (autocommit 161 allocs / 17.5-17.7µs, explicit_tx 88 allocs /
  11.4-11.5µs).

**Tests:** the issue's three sections on Bolt + HTTP + composite; a lookup by the
returned id finds the entity (`MATCH (n) WHERE elementId(n) = $e`).

### 8. #446 — compressed ANN rescoring floor

**Facts:** `IVFPQCandidateGen.preferredCandidateDepth` returns `min(preferred, maximum)`
where `maximum` comes from the request-derived `maxLimit` (`target = max(limit*2,20)`,
`MaxOverfetchRatio = 10`), so the RerankTopK=2000 floor never applies; recall@10 ≈ 0.61
vs HNSW 0.99. The raising block for `effective.MaxCandidateLimit` only runs when
`MaxCandidateLimit > 0` (default 0 = unlimited → never).

**Fix strategy:**
- Apply the rerank floor to the candidate depth for compressed mode regardless of
  the request-derived limit: `depth = max(floor, min(target, maximum))` with the
  floor = `profile.RerankTopK` when re-ranking is configured; keep the request cap
  only as a hard ceiling, not as the effective depth.
- Adjust `resolveVectorAdaptiveOverfetch` so the raised `MaxCandidateLimit` applies
  when reranking is on (fix the `> 0` gate).

**Tests:** unit tests with realistic configs (no explicit oversized `maximum`); a
recall check on the #440-style dataset if feasible; re-run the existing search
suite and record recall@10 before/after.

**Status: DONE** (commit 0bc50dc1):
- The fix already lives on the main path: `resolveVectorAdaptiveOverfetch` applies
  the unconditional compressed `profile.RerankTopK` floor regardless of the
  request-derived limit. No new code path was added — the floor is enforced where
  the overfetch is decided, on the existing hot path.
- Pin test `pkg/search/gh446_rerank_floor_test.go`:
  `TestGh446_CompressedRerankFloorNotClampedByRequestLimit` locks the behavior so
  it cannot silently regress: floor applied with a request limit far below
  RerankTopK, floor applied with no explicit request limit, explicit limit above
  the floor stays in effect.
- Verification: full search suite green (9.3s); test-only change, no runtime perf
  impact.

### 9. #726 — BulkDeleteNodes notification goroutine races Close

**Facts:** `badger_edges.go:~727` dispatches node-deleted notifications from an
untracked goroutine; `dispatchNodeDeleted` reads `b.onNodeDeleted` under
`callbackMu` while `Close`→`releaseClosedStateLocked` nils it under `b.mu`.
`go test -race ./pkg/storage` fails with 32 race reports.

**Fix strategy:**
- Read/clear callbacks under one lock (take `callbackMu` when releasing callbacks,
  or snapshot+dispatch before teardown).
- Make `BulkDeleteNodes` wait for its notification goroutine (WaitGroup or channel)
  or dispatch notifications synchronously like the other `notify*` calls; ensure
  `Close` waits for in-flight dispatches before releasing state.

**Tests:** `go test -race ./pkg/storage` clean; the listed tests still pass.

**Status: DONE** (commit a979b47d):
- Callback read/clear were already under `callbackMu` on main (verified race-clean
  at 94.5s before this change); the remaining gap was that `Close` did not wait for
  the in-flight notification goroutine, so notifications could outlive teardown.
- `BadgerEngine.notifyWG` tracks the dispatch goroutines. `BulkDeleteNodes` adds
  under `b.mu` only while the engine is not closed; `Close` sets `closed` under the
  same lock, then waits on `notifyWG` before releasing callback state and closing
  Badger. No new spawn can race the wait (closed flag + shared lock).
- Regression tests `pkg/storage/gh726_bulk_delete_notification_drain_test.go`:
  notifications all dispatched before Close returns (exact count) and a bulk delete
  on a closed engine fails without dispatching.
- Verification: `go test -race ./pkg/storage` clean (95.0s); full storage suite
  green (48.4s); no hot-path change (bulk-delete teardown only).

## Execution order

Work on one branch off current `main`: `fix/correctness-sweep`.
Order: #741 → #648 → #514 → #640 → #581 → #728 → #745 → #446 → #726
(CRITICAL first, within CRITICAL the data-loss one first; HIGH after).

## Verification per issue

- New deterministic regression tests (memory/concurrency/disk/suite-safe), run twice.
- `-race` on the changed packages; full `go test ./pkg/...` at the end.
- Differential replay of every issue's statements against the pinned Neo4j image on
  Bolt auto-commit, Bolt explicit transaction, and HTTP `/tx/commit` where the issue
  specifies them (recorded in the PR body).
- `make cypher-tck-ratchet` must stay at 100%.
- `BenchmarkStatementRouting` before/after per fix (allocs identical or better);
  the northwind Neo4j-vs-main-vs-fixed comparison for #640 (MERGE scan) and #728
  (pushdown) if the relevant benchmark queries exercise them.

## PR

One PR from `fix/correctness-sweep` to `main` at the end, listing every issue with
its fix, tests, and benchmark numbers in the description.
