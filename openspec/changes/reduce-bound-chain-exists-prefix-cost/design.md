# Design: bound-chain EXISTS predicate decides its decidable prefix

Status: proposal. Reviewed source is worktree `/private/tmp/n152-90-retrieval`
at HEAD `18b36a85d10e775b9d5d3500ecf3482682754a8d`. No production code was changed
and nothing was built, tested or benchmarked for this change.

## One owner per decision

| Decision | Existing owner | This change |
|---|---|---|
| Which conjunct may be decided on a path prefix | `chainPrefixDecides` (`pkg/cypher/traversal.go:1875`) | reuse unchanged |
| A prefix's own pattern and bindings | `chainPrefixMatch` (`:1941`), `buildPathContext`, `bindParameterRow`, `chainPrefixPrune.keep` (`:1958`) | reuse unchanged |
| Evaluating the prefix predicate | `prepareRowPredicate(...).holds` (`pkg/cypher/row_predicate_program.go:99`) → `evaluateRowPredicate*` | reuse unchanged |
| Applying a prefix loss | `chainPrefixPrune.keep` (`:1958`), called from `traverseChainedGraph` (`:1750`, `:1811`) | reuse unchanged |
| Admitting a body as a bound chain | `boundChainCount` shape parse and `boundChainReadPlan` (`pkg/cypher/subquery_value_expression.go:577`, `:592`) | supply the body `WHERE` the owner already applies |
| Per-candidate verdict in one search | `filterVerdicts` (`pkg/search/filter_verdict.go:24`), created per `Service.Search` (`pkg/search/search.go:5202`) | unchanged |
| Complete paths' final `WHERE` | `boundChainCount` row merge (`subquery_value_expression.go:674`) + `evaluateRowPredicate` | unchanged |

No second admission rule, row builder, evaluator or traversal is introduced.

## Contract per boundary

| Boundary | Contract after this change |
|---|---|
| `boundChainCount` body admission | same accepts and same declines: pattern list, row-dependent property map, dynamic pattern syntax, shortest path, repeated elements, no bound start node, non-entity row binding |
| Chain seed and row-bound end node | unchanged, including the existing per-row copy that carries `boundEndNodeID` |
| Chain expansion | a candidate whose prefix fails a decidable conjunct stops earlier; a surviving candidate expands the same segments in the same DFS order |
| Complete paths handed to the row loop | a subset of today's paths, exactly the ones whose complete `WHERE` holds (see soundness below) |
| `WHERE` on complete paths | unchanged and still applied once per surviving path |
| `EXISTS` (`first=true`) | same first accepted path, therefore the same boolean |
| `COUNT` | same count |
| Correlation with row-bound nodes/edges | unchanged: the correlation loop still compares each path binding with the row's own binding |
| Retained plan immutability | the cached `TraversalMatch` is never mutated; a row that needs different traversal state copies it, as the bound-end-node case already does |
| Cancellation | same statement context; retained frames and sibling expansions keep their existing probes (`traversalCancelled`, `pkg/cypher/traversal_cancel_state.go:19`); pruning only removes expansions |
| Memory accounting | only retained paths are charged (`findPaths`, `pkg/cypher/traversal.go:2158`); pruning charges fewer paths, never more |
| Cancellation as expression failure | a cancelled or memory-failed traversal keeps its existing latch and partial-result behavior; the prefix decision adds no error path |
| Temporal viewport | prefix nodes are the same nodes read under the same viewport; `chainPrefixDecides` already refuses `functionTemporalConstructor` argument forms and non-catalog functions |
| Permissions / visibility | prefix nodes come from the same endpoint reader (`loadTraversalEndpointNode` → `newRelationshipEndpointReader(...).readRetained` → `getQueryNode`), so system-node hiding, namespace/composite scoping and the query view are unchanged; no new read capability is added |
| Fine-grained authorization | a fine-grained request already searches a private `Service` built over its authorized graph view (`pkg/search/fine_grained_search.go:13`); the owner inside that view reads through the same scoped engine, so the reduction cannot see nodes the view cannot |
| Current-read policy inside a write | prefix evaluation runs inside the same statement context, so `statementAdjacentRead`/`relationshipDecisionKey` policy is unchanged |
| Grouped and complete-ranked pages | untouched: continuation depth, grouping, gating and page assembly stay with `pkg/search` and `resultstream` owners |
| Ranking, fusion, scoring | untouched |

## Soundness of deciding a conjunct early

`boundChainCount` builds its final row as the enclosing row's values with the path's nodes
and relationships written over them (`subquery_value_expression.go:674`), so the complete
`WHERE` is evaluated with the path's bindings. `chainPrefixPrune.keep` evaluates the same
text with the path prefix's bindings and the statement's parameters, and only for a
conjunct whose every free name is bound by that prefix, whose calls the catalog declares
stable, and which holds no subquery expression (`chainPrefixDecides`). A prefix that fails
such a conjunct cannot extend to a complete path that passes the same conjunct under the
same bindings, so the count, the EXISTS answer and the surviving path order are unchanged.
This is the argument the clause pipeline already relies on at `traversal.go:505`; the
bound-chain predicate entry is the only chained entry in scope that skips it.

Correlation is not weakened: if the enclosing row binds an intermediate variable, the
prefix decision uses the path's binding while the correlation loop separately requires the
row's binding to be the same entity; a path that differs is rejected either way.

## Where the prepared prefix predicate lives

`traverseChainedGraph` builds its prune with `newChainPrefixPrune` on each call. The
bound-chain entry runs once per candidate, so leaving construction there would split
conjuncts and prepare a predicate per candidate. Two shapes are acceptable, and the parent
chooses:

1. Extend `boundChainReadPlan` with the statement-built prefix plan (parameters stay
   unbound, values are never retained) and let the chain traversal accept it. One build per
   public statement, matching `TestRetrievalWhereReusesChainSyntaxWithFreshStatementBindings`.
2. Keep a `pruneWhere`-equivalent text on the per-row copy and let the existing traversal
   build it. Correct, but it moves string splitting and predicate preparation into the
   per-candidate path, which contradicts the point of the change.

Shape 1 is the proposal's default; shape 2 is recorded only so the trade-off is explicit.
Either way the prune's conjunct set must be row-independent, which is what
`chainPrefixDecides` already guarantees by dropping any conjunct that names a name the
pattern does not bind.

## Reuse review (duplicate-checker candidates)

Graphify retrieval for this worktree returned an incomplete search
(`/db/nornicdbcode/tx/commit`, `/nornicdb/search` timed out; empty code baseline), so the
advisory duplicate analysis is **not** passed. The candidates below were found by reading
the checkout; each states what overlaps and what the disposition is.

| Candidate | Relationship | Disposition |
|---|---|---|
| `chainPrefixPrune` + `chainPrefixDecides` (`traversal.go:1817`, `:1875`) | the same decision this change needs, already the pipeline's owner | reuse; this change makes the bound-chain entry supply its `WHERE` |
| `preparedRowPredicate` / `evaluateRowPredicate` | same predicate semantics on a different binding set | reuse; no new evaluator |
| `boundChainReadPlan` (`subquery_value_expression.go:577`) | retained, row-independent chain syntax, already copied per row | extend for the prepared prefix plan |
| `filterVerdicts` (`pkg/search/filter_verdict.go:24`) | already prevents a second verdict per candidate in one search invocation, including across both retrieval branches and adaptive prefixes | unchanged; it does not and should not cache chain expansion or group outcomes |
| `boundNodeLookupCount`, `boundDegreeCount` | sibling owners for other body shapes | unchanged, not generalized |
| `match_multi.go:187`, `:1566` bound chain entries | call `traverseChainedGraph` with a `TraversalMatch` whose `pruneWhere` is empty, while the enclosing `WHERE` is applied later | same divergence, outside this change's scope; recorded for the parent |
| `row_predicate_plan.go:314` compiled plans | declines bodies with braces, which is why this body is text-evaluated per candidate | unchanged; compiling correlated bodies is a planner change and excluded |
| corpus/knowledge caches, service result cache | `opts.Where != nil` already disables the result cache (`search.go:5213`) | unchanged; this change adds no cache |

## Predicted effect, and what it does not claim

Per fixture structure (356 groups, one current source per group at index = group id, 178
admitted identities):

| run | today | after (predicted) |
|---|---|---|
| 1,000 all-admitted | 3,000 adjacency + 3,000 body reads | 1,000 + 2×356 = 1,712 + 1,712 |
| 2,000 all-admitted | 6,000 + 6,000 | 2,000 + 2×356 = 2,712 + 2,712 |
| 2,000 first page | 6,000 + 6,000 | 2,000 + 2×356 = 2,712 + 2,712 |
| 1,000 first page (369 evaluated) | 1,107 + 1,107 | 369 + 2k, 178 ≤ k ≤ 356 (k measured) |

The formula counts one `HAS_SOURCE` expansion per evaluated candidate plus two expansions
for each candidate that is its group's current source. After prefix pruning no duplicated
suffix remains to reuse: every level-1 passer is a distinct group current source with a
distinct version, which is why no memo of group outcomes is proposed.

This does not claim the seconds-per-call case is fixed. Evaluation count still equals the
candidate pool of the request, so the remaining per-candidate `HAS_SOURCE` expansion,
endpoint body read and row work keep the cost proportional to evaluated candidates; that
term belongs to the candidate-narrowing decision the parent reserved. No allocation split
by path copies, endpoint copies and row maps exists, so the allocation reduction is
expected to follow the expansion count but is not predicted.
