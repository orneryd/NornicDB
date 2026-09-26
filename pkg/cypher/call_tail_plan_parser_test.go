package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParseCallTailVariableLengthMaxLengthPlan(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_varlen_parser_cov"))
	ctx := context.Background()

	tail := `
MATCH p = (node)-[:BENCH_HOP*1..6]->(:BenchmarkHop)
WITH node, score, max(length(p)) AS maxDepth
RETURN node.textKey AS textKey, maxDepth, score
ORDER BY score DESC
LIMIT 5
`
	plan, ok := exec.parseCallTailVariableLengthMaxLengthPlan(ctx, tail)
	require.True(t, ok)
	require.NotNil(t, plan)
	require.Equal(t, "node", plan.nodeVar)
	require.Equal(t, "maxDepth", plan.aggregateAlias)
	require.Equal(t, "score DESC", plan.orderBy)
	require.Equal(t, "5", plan.limitToken)
	require.Equal(t, "", plan.skipToken)
	require.Len(t, plan.returnItems, 3)

	// SKIP / LIMIT expressions stay whole, so the plan can't read them as
	// their first number; they don't resolve, and the plan declines (#572).
	plan, ok = exec.parseCallTailVariableLengthMaxLengthPlan(ctx, "MATCH p = (node)-[:BENCH_HOP*1..6]->(:BenchmarkHop) WITH node, score, max(length(p)) AS maxDepth RETURN node.textKey AS textKey, maxDepth, score SKIP 1 + 3 LIMIT 1 + 1")
	require.True(t, ok)
	require.Equal(t, "1 + 1", plan.limitToken)
	require.Equal(t, "1 + 3", plan.skipToken)
	for _, token := range []string{plan.limitToken, plan.skipToken} {
		_, resolved := resolveOptionalIntLiteralOrParam(ctx, token)
		require.False(t, resolved)
	}

	_, ok = exec.parseCallTailVariableLengthMaxLengthPlan(ctx, "MATCH (node)-[:BENCH_HOP*1..6]->(:BenchmarkHop) RETURN node")
	require.False(t, ok)
}

func TestParseCallTailBranchingPathCountPlan(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_branch_parser_cov"))
	ctx := context.Background()

	tail := `
MATCH p = (node)-[:BENCH_HOP|REL_A|REL_B*1..4]->(x)
WHERE ALL(n IN nodes(p) WHERE size(labels(n)) > 0)
WITH node, score, p, length(p) AS d
ORDER BY d ASC
WITH node, score, collect(p)[0..$pathCap] AS paths
RETURN elementId(node) AS nodeID, score, size(paths) AS pathCount
LIMIT $topK
`
	plan, ok := exec.parseCallTailBranchingPathCountPlan(ctx, tail)
	require.True(t, ok)
	require.NotNil(t, plan)
	require.Equal(t, "node", plan.nodeVar)
	require.Equal(t, "paths", plan.pathsAlias)
	require.Equal(t, "$pathCap", plan.pathCapToken)
	require.Equal(t, "$topK", plan.limitToken)
	require.Equal(t, []string{"nodeID", "score", "pathCount"}, plan.resultColumns())

	_, ok = exec.parseCallTailBranchingPathCountPlan(ctx, "MATCH p = (node)-[:REL*1..3]->(x) RETURN p")
	require.False(t, ok)
}

func TestParseCallTailFrontierReachablePlan(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_frontier_parser_cov"))
	ctx := context.Background()

	tail := `
MATCH (node)-[:REL*1..4]->(x)
WITH node, score, length(shortestPath((node)-[:REL*1..4]->(x))) AS d
WITH node, score, min(d) AS nearest, count(*) AS reachable
RETURN elementId(node) AS nodeID, score, nearest, reachable
LIMIT $topK
`
	plan, ok := exec.parseCallTailFrontierReachablePlan(ctx, tail)
	require.True(t, ok)
	require.NotNil(t, plan)
	require.Equal(t, "node", plan.nodeVar)
	require.Equal(t, "nearest", plan.nearestAlias)
	require.Equal(t, "reachable", plan.reachableAlias)
	require.Equal(t, "$topK", plan.limitToken)
	require.Equal(t, []string{"nodeID", "score", "nearest", "reachable"}, plan.resultColumns())

	_, ok = exec.parseCallTailFrontierReachablePlan(ctx, "MATCH (node)-[:REL*1..4]->(x) RETURN node")
	require.False(t, ok)
}

func TestParseCallTailConstrainedMaxDepthPlanAndReturnOptions(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_constrained_parser_cov"))
	ctx := context.Background()

	tail := `
MATCH p = (node)-[:REL*1..4]->(x)
WHERE any(r IN relationships(p) WHERE r.weight >= $minWeight)
  AND any(n IN nodes(p) WHERE n.category IN $cats)
RETURN elementId(node) AS nodeID, score, max(length(p)) AS maxDepth
LIMIT $topK
`
	plan, ok := exec.parseCallTailConstrainedMaxDepthPlan(ctx, tail)
	require.True(t, ok)
	require.NotNil(t, plan)
	require.Equal(t, "node", plan.nodeVar)
	require.Equal(t, "maxDepth", plan.aggregateAlias)
	require.Equal(t, "$minWeight", plan.minWeightToken)
	require.Equal(t, "$cats", plan.categoriesToken)
	require.Equal(t, "$topK", plan.limitToken)
	require.Equal(t, []string{"nodeID", "score", "maxDepth"}, plan.resultColumns())

	_, ok = exec.parseCallTailConstrainedMaxDepthPlan(ctx, "MATCH p = (node)-[:REL*1..4]->(x) RETURN node")
	require.False(t, ok)

	projection, orderBy, limitToken, skipToken := splitCallTailProjectionModifiers("nodeID ORDER BY score DESC SKIP 2 + 1 LIMIT $topK")
	require.Equal(t, "nodeID", projection)
	require.Equal(t, "ORDER BY score DESC", orderBy)
	require.Equal(t, "2 + 1", skipToken)
	require.Equal(t, "$topK", limitToken)
}
