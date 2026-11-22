# NOR-120 / N152-47: deterministic accumulation before the target-ID tie key

## Problem and contract

The NOR-120 audit reproduced, on an unchanged graph, a change of the selected
top-1 candidate with no node-ID or graph change. On this worktree base
`163cfa2e0e497b464eda5d9473625ad8c125bf54`, the supplied witness
(`pkg/cypher/nor120_linkprediction_unchanged_graph_test.go`: twelve nodes,
fifteen edges, `s`, `x`, `y` and shared hubs of degree 4, 5 and 6, 64 uncached
public executions per parser) first failed with nornic `x:52 y:12` and ANTLR
`x:58 y:6`, and score bits in the two adjacent representations
`3ffe69a6019be459` / `3ffe69a6019be45a`.

The contract is unchanged: equal scores are ordered by target node ID
(`rankedBefore`, commit 74e55097d, recorded in the v1.4.2 changelog), so an
unchanged graph keeps the same top-K candidates. The defect is not in that
comparator. `PairScore` accumulated the Adamic-Adar (and resource allocation)
terms over Go map iteration order, and floating-point addition is not
associative, so two candidates with the same multiset of contribution degrees
received adjacent values. The comparator then saw a real difference and ranked
one of them first, whatever their IDs.

## Implementation and reuse

The correction is in the existing canonical owner,
`pkg/linkpredict/topology_scores.go` `PairScore`. For `adamic_adar` and
`resource_allocation` it collects the common neighbours' degrees and adds the
terms from the smallest degree up (`slices.Sort` over `[]int`). Since each
term is monotone in its degree, two candidates whose common neighbours carry
the same degree multiset produce the same bits even when those neighbours have
different node IDs, and the exact target-ID tie key then decides the order.
The value is one of the sums a map order already produced, not a new formula
value, and every formula, the target-ID tie key, candidate cardinality, the
`score > 0` filter and the degree semantics (relationships, parallel edges and
self-loops; `Graph.Degree` when nil) are untouched. No epsilon comparison, no
business-key ordering and no second scoring implementation were added.

The count-only scores (`common_neighbors`, `jaccard`, `total_neighbors`) and
`preferential_attachment` allocate nothing and keep their previous arithmetic;
only the accumulating scores gain a degree buffer.

Owner inventory at this revision: `PairScore` is the only place that sums these
terms. `TopologyScores`/`RankedPredictions` and `AdamicAdar`/`ResourceAllocation`
(`topology.go`), the hybrid scorer (`hybrid.go`), inference's topology
integration (`pkg/inference/topology_integration.go`, which delegates), the GDS
stream procedures (`pkg/cypher/linkprediction.go`) and the
`gds.alpha.linkprediction.*` scalar functions (`pkg/cypher/gds_functions.go`)
all reach it. The duplicate checker and the configured code graph were
unavailable (see evidence), so this is a source-level inventory, not a clean
indexed verdict.

## Evidence and limits

Red and green public matrices, unit coverage, benchmarks, profiles and exact
commands are in `evidence.md`. The parent candidate keeps up to 32 degrees on the stack, grows
only with actual shared neighbours and skips the unused union traversal for
weighted scores. See `parent-acceptance.md` for its final workload costs;
`evidence.md` preserves the superseded worker candidate measurements.

Measurement is one host (Apple M2 Max, darwin/arm64) with `GOMAXPROCS=2`.
Full suite, Linux, HTTP/Bolt and release qualification remain separate from
the focused parent checks recorded in `parent-acceptance.md`. The repository's older generic `BenchmarkAdamicAdar` does
not exercise this path (its fixture's node IDs are built with `string(rune(i))`,
so the source node is absent and it returns in 8 ns); the workload benchmarks
in `pkg/linkpredict/topology_scores_bench_test.go` are the qualifying evidence.
