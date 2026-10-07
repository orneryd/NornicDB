package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParseMatchWithStages_Branches(t *testing.T) {
	_, _, ok := parseMatchWithStages("RETURN 1")
	require.False(t, ok)

	_, _, ok = parseMatchWithStages("MATCH (n) RETURN n")
	require.False(t, ok)

	_, _, ok = parseMatchWithStages("MATCH (n) WITH n MATCH (m) WITH m")
	require.False(t, ok)

	stages, ret, ok := parseMatchWithStages("MATCH (n:A) WITH count(n) AS c MATCH (m:B) WITH c AS c2, count(m) AS d RETURN c2, d")
	require.True(t, ok)
	require.Len(t, stages, 2)
	require.Equal(t, "(n:A)", stages[0].matchClause)
	require.Equal(t, "count(n) AS c", stages[0].withClause)
	require.Equal(t, "(m:B)", stages[1].matchClause)
	require.Equal(t, "c AS c2, count(m) AS d", stages[1].withClause)
	require.Equal(t, "c2, d", ret)
}

func TestEvaluateMatchClauseNodes_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "with_chain_eval"))
	ctx := context.Background()

	_, _, err := exec.evaluateMatchClauseNodes(ctx, "(:Person)")
	require.ErrorContains(t, err, "missing variable")

	_, err = exec.storage.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"k": "x", "v": int64(1)}})
	require.NoError(t, err)
	_, err = exec.storage.CreateNode(&storage.Node{ID: "a2", Labels: []string{"A"}, Properties: map[string]interface{}{"k": "y", "v": int64(2)}})
	require.NoError(t, err)

	nodes, v, err := exec.evaluateMatchClauseNodes(ctx, "(n:A {k:'x'})")
	require.NoError(t, err)
	require.Equal(t, "n", v)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("a1"), nodes[0].ID)

	nodes, v, err = exec.evaluateMatchClauseNodes(ctx, "(n:A) WHERE n.v = 2")
	require.NoError(t, err)
	require.Equal(t, "n", v)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("a2"), nodes[0].ID)
}

func TestExecuteChainedMatchWithAggregations_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "with_chain_exec"))
	ctx := context.Background()

	res, used, err := exec.sharedChainedPipelineForTest(ctx, "RETURN 1")
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, res)

	for _, n := range []*storage.Node{
		{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"k": "x"}},
		{ID: "a2", Labels: []string{"A"}, Properties: map[string]interface{}{"k": "y"}},
		{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"z": int64(1)}},
		{ID: "b2", Labels: []string{"B"}, Properties: map[string]interface{}{"z": nil}},
	} {
		_, err = exec.storage.CreateNode(n)
		require.NoError(t, err)
	}

	res, used, err = exec.sharedChainedPipelineForTest(ctx,
		"MATCH (a:A) WITH count(a) AS aCount MATCH (b:B) WITH aCount AS aCount, count(b.z) AS bz RETURN aCount, bz")
	require.NoError(t, err)
	require.True(t, used)
	require.Equal(t, []string{"aCount", "bz"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, int64(2), res.Rows[0][0])
	require.EqualValues(t, int64(1), res.Rows[0][1])

	res, used, err = exec.sharedChainedPipelineForTest(ctx,
		"MATCH (a:A) WITH sum(1) AS s MATCH (b:B) WITH s AS s2, count(b) AS c RETURN s2, c")
	require.NoError(t, err)
	require.True(t, used)
	require.Equal(t, [][]interface{}{{int64(2), int64(2)}}, res.Rows)

	res, used, err = exec.sharedChainedPipelineForTest(ctx,
		"MATCH (a:A) WITH count(a) AS aCount MATCH (b:B) WITH missingAlias AS x, count(b) AS c RETURN x, c")
	require.Error(t, err)
	require.True(t, used)
	require.ErrorContains(t, err, "missingAlias")
	require.Nil(t, res)
}

func TestProjectionAndCountHelpers_Branches(t *testing.T) {
	expr, alias := parseProjectionExprAlias(" n.name AS nn ")
	require.Equal(t, "n.name", expr)
	require.Equal(t, "nn", alias)

	expr, alias = parseProjectionExprAlias("n")
	require.Equal(t, "n", expr)
	require.Equal(t, "n", alias)

	expr, alias = parseProjectionExprAlias(" ")
	require.Empty(t, expr)
	require.Empty(t, alias)

	nodes := []*storage.Node{{Properties: map[string]interface{}{"x": 1}}, {Properties: map[string]interface{}{"x": nil}}, nil}

	c, ok := countForExpr(nodes, "n", "*")
	require.True(t, ok)
	require.EqualValues(t, 3, c)

	c, ok = countForExpr(nodes, "n", "n")
	require.True(t, ok)
	require.EqualValues(t, 3, c)

	c, ok = countForExpr(nodes, "n", "n.x")
	require.True(t, ok)
	require.EqualValues(t, 1, c)

	c, ok = countForExpr(nodes, "n", "m.x")
	require.False(t, ok)
	require.Zero(t, c)
}
