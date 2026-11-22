# Reduce bound-chain EXISTS predicate expansion to its decidable prefix

Issue: NOR-406 (`db.retrieve` / `db.rretrieve` take seconds per call on a 49,000-node
store; #1683 does not cover them). Parent acceptance of the measured root is recorded in
`openspec/changes/nor406-current-retrieval-cursor-audit/parent-root-acceptance.md` in
worktree `nor406-v151-retrieval-cost`. This change is a proposal only: no production
edit, no test edit, no build and no benchmark has been run for it.

Convergence plan: no plan step covers engine-side retrieval predicate cost; this change
reuses the existing traversal and predicate owners rather than adding a planner stage.

## Problem and evidence

The public retrieval `where` of the qualification workload is

```
EXISTS { (node)<-[:HAS_SOURCE]-(v:Version)<-[:HAS_VERSION]-(:Document)
         -[:MEMBER_OF]->(c:Collection)
         WHERE c.id IN $ids AND NOT c.archived
           AND v.current_source_id = node.id }
```

`db.retrieve` compiles it once per statement as a retained node predicate
(`pkg/cypher/retained_predicate.go:124`). The row-predicate planner declines the text
because it holds braces (`pkg/cypher/row_predicate_plan.go:314`), so each candidate row
reaches `rowSubqueryValue` (`pkg/cypher/subquery_value_expression.go:293`), which offers
the body to `boundChainCount` (`:592`). The parent's 1.5.2 CPU profile shows that chain
positively: `evaluateWhere` 30.77s → `rowSubqueryValue` 29.60s → `boundChainCount`
29.12s → `traverseChainedGraph` 25.38s → `findPaths` 22.52s cumulative in a 396.44s
sample of a 262s public macOS run.

`boundChainCount` traverses the whole chain from the bound candidate and then applies the
body `WHERE` to each complete path. `traverseChainedGraph`
(`pkg/cypher/traversal.go:1711`) can also decide a conjunct on a path prefix
(`chainPrefixPrune`, `:1817`), but only when `TraversalMatch.pruneWhere` is set, and only
the clause pipeline sets it (`:505`). The bound-chain predicate entry never does, so a
conjunct that is already decidable after one segment is evaluated only after all three.

The extracted read-count fixture measures exactly that. At 1,000 and 2,000 sources,
through both statement parsers, with no label scan, no `AllNodes` and no node stream:

| run | evaluations | adjacency reads | query-node body reads |
|---|---|---|---|
| 1,000 first page (EXISTS) | 369 | 1,107 = 3×369 | 1,107 |
| 1,000 all-admitted | 1,000 | 3,000 | 3,000 |
| 2,000 first page (EXISTS) | 2,000 | 6,000 | 6,000 |
| 2,000 all-admitted | 2,000 | 6,000 | 6,000 |

and `seeds-by-type` shows the middle and last segments expanding from only 356 distinct
versions and 356 distinct documents while running once per candidate.

The 1.5.2 release head reports the same public cost on 12,500 sources: first-page
`db.retrieve` plain 4.25 ms / 1.03 MB versus correlated EXISTS 153.28 ms / 88.90 MB per
call, `db.rretrieve` 10.57 ms / 1.75 MB versus 161.85 ms / 88.55 MB. The earlier Linux
49,000-source table (12.207/16.440 ms plain versus 2223.431/2201.368 ms EXISTS) remains
that run's evidence and is not re-measured here.

For this body, `v.current_source_id = node.id` binds only `node` (the pattern's start
variable) and `v` (the first segment's end), so it is decidable after segment 1 for every
candidate. The fixture's graph makes the prediction exact: 356 groups, one current source
per group at index = group id, 178 admitted identities. Pruning that conjunct leaves one
`HAS_SOURCE` expansion plus a second and third segment only for the candidates that are
their group's current source:

- all-admitted: `population + 2 × min(population, 356)` → 1,712 at 1,000 sources and
  2,712 at 2,000, against 3,000 and 6,000 now.
- 2,000-source first page: 2,000 + 2 × 356 = 2,712, against 6,000 now.
- 1,000-source first page: `369 + 2k`, where `k` is how many of those 369 evaluated
  candidates are their group's current source (178 ≤ k ≤ 356); `k` and the resulting count
  are measured by the same ledger, not assumed.

## Required outcome

A candidate that cannot satisfy a conjunct already decidable on its path prefix stops
expanding at that prefix, in the one bound-chain predicate owner, with exactly the rows,
pages and EXISTS/COUNT values it returns today.

## Correction

Extend the existing chain-prefix prune owner to the bound-chain predicate entry:

- `boundChainCount` supplies the body `WHERE` it already applies to complete paths as the
  chain's prefix predicate, on a per-row copy of the retained structural plan
  (`boundChainReadPlan`, `subquery_value_expression.go:577` already copies for a row-bound
  end node). The cached `TraversalMatch` is never mutated.
- The prepared prefix predicate is built once per statement with the retained plan, not
  once per row and not once per candidate.
- `chainPrefixDecides`/`chainPrefixMatch`/`chainPrefixPrune.keep` stay the only owners of
  which conjunct may be decided early and of how a prefix row is built. The prefix row is
  evaluated by the same `prepareRowPredicate(...).holds` owner the final `WHERE` uses.
- Nothing else changes: the search-side per-invocation candidate verdict
  (`filterVerdicts`, `pkg/search/filter_verdict.go:24`, created at
  `pkg/search/search.go:5202`), candidate enumeration, adaptive depth, grouping, ranking,
  scoring, result assembly and the public procedure are untouched.

## Scope boundaries

In scope: the bound-chain EXISTS/COUNT predicate entry and the prune decision it
delegates to.

Not in scope, and not to be added by this change:

- a second planner, a compiled semi-join for correlated bodies, or a new predicate
  evaluator;
- a private graph scan, label scan, `AllNodes` read or a new storage capability;
- any cache: no global cache, no cross-request cache, and no memo of group or predicate
  outcomes (the parent's acceptance note explicitly withholds approval for memoizing group
  outcomes);
- candidate-narrowing, index-seek or start/end seed choice (NOR-408 / NOR-414 owners);
- ranking, fusion, reranking, grouping, page assembly, continuation depth or any other
  provisional ranking behavior;
- the sibling bound-chain entries that also pass no prefix predicate
  (`pkg/cypher/match_multi.go:187`, `:1566`). They show the same divergence; whether to
  converge them is a scope decision for the parent, recorded in `design.md`.

## Compatibility and risk

The prefix decision is the same predicate text evaluated on a subset of the same
bindings, and the complete `WHERE` still runs on every surviving path, so EXISTS, COUNT
and `first`-match ordering are unchanged. Risk is concentrated in four places, each with
an existing control: correlation with row-bound intermediate variables, volatile
conjuncts (never pruned, `pkg/cypher/chain_prefix_volatile_evaluation_test.go`),
parameter binding and row-value binding, and the read lifetime (cancellation, memory
accounting, temporal viewport, permissions/visibility, grouped and complete-ranked
pages).

## Qualification

Before and after must be measured on the same host and configuration with the extracted
fixture, reporting read counts, rows/pages, latency and allocations together. The
fixture's current assertions characterize the present cost and are not a fail-first
regression; the cost contract and its assertions need parent approval before any
implementation, and the exact commands and the unresolved evidence are in `evidence.md`.
