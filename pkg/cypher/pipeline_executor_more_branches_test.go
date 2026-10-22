package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineApplyWith_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	rows := []pipelineRow{
		{
			"n": &storage.Node{ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{"name": "alice"}},
			"m": map[string]interface{}{"score": int64(9)},
		},
	}

	fixtureAdmission1748Context, fixtureAdmission1748 := statementTestContext(context.Background(), "WITH")
	_ = fixtureAdmission1748Context
	out, ok := exec.pipelineApplyWith(fixtureAdmission1748, fixtureAdmission1748Context, rows, "WITH")
	require.True(t, ok)
	require.Equal(t, rows, out)

	// Aggregate with alias collapses rows.
	fixtureAdmission1749Context, fixtureAdmission1749 := statementTestContext(context.Background(), "WITH count(*) AS c")
	_ = fixtureAdmission1749Context
	aggOut, ok := exec.pipelineApplyWith(fixtureAdmission1749, fixtureAdmission1749Context, []pipelineRow{{"x": 1}, {"x": 2}}, "WITH count(*) AS c")
	require.True(t, ok)
	require.Len(t, aggOut, 1)
	require.EqualValues(t, int64(2), aggOut[0]["c"])

	// Property projection from node and map plus literal scalar.
	fixtureAdmission1750Context, fixtureAdmission1750 := statementTestContext(context.Background(), "WITH n.name AS name, m.score AS score, 42 AS answer")
	_ = fixtureAdmission1750Context
	out, ok = exec.pipelineApplyWith(fixtureAdmission1750, fixtureAdmission1750Context, rows, "WITH n.name AS name, m.score AS score, 42 AS answer")
	require.True(t, ok)
	require.Len(t, out, 1)
	require.Equal(t, "alice", out[0]["name"])
	require.EqualValues(t, int64(9), out[0]["score"])
	require.EqualValues(t, int64(42), out[0]["answer"])

	// Nested literals and postfix operations share the converged expression
	// evaluator instead of being classified by a list-subscript specialization.
	fixtureAdmission1751Context, fixtureAdmission1751 := statementTestContext(context.Background(), "WITH [x, [], {}] AS values, [x, []][1] AS nested")
	_ = fixtureAdmission1751Context
	out, ok = exec.pipelineApplyWith(fixtureAdmission1751, fixtureAdmission1751Context, []pipelineRow{{"x": int64(7)}},
		"WITH [x, [], {}] AS values, [x, []][1] AS nested")
	require.True(t, ok)
	require.Len(t, out, 1)
	require.Equal(t, []interface{}{int64(7), []interface{}{}, map[string]interface{}{}}, out[0]["values"])
	require.Equal(t, []interface{}{}, out[0]["nested"])

	// COUNT without alias is unsupported in WITH projection and must fall back.
	fixtureAdmission1752Context, fixtureAdmission1752 := statementTestContext(context.Background(), "WITH count(*)")
	_ = fixtureAdmission1752Context
	out, ok = exec.pipelineApplyWith(fixtureAdmission1752, fixtureAdmission1752Context, rows, "WITH count(*)")
	require.False(t, ok)
	require.Nil(t, out)

	// Unknown expression also falls back.
	fixtureAdmission1753Context, fixtureAdmission1753 := statementTestContext(context.Background(), "WITH unknownExpr AS x")
	_ = fixtureAdmission1753Context
	out, ok = exec.pipelineApplyWith(fixtureAdmission1753, fixtureAdmission1753Context, rows, "WITH unknownExpr AS x")
	require.False(t, ok)
	require.Nil(t, out)
}

func TestPipelineApplyReturn_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	rows := []pipelineRow{{"x": int64(1)}, {"x": int64(2)}}
	fixtureAdmission1754Context, fixtureAdmission1754 := statementTestContext(context.Background(), "RETURN ")
	_ = fixtureAdmission1754Context
	res, ok := exec.pipelineApplyReturn(fixtureAdmission1754, fixtureAdmission1754Context, rows, "RETURN ")
	require.False(t, ok)
	require.Nil(t, res)

	fixtureAdmission1755Context, fixtureAdmission1755 := statementTestContext(context.Background(), "RETURN count(*) AS c, count(x) AS cx")
	_ = fixtureAdmission1755Context
	res, ok = exec.pipelineApplyReturn(fixtureAdmission1755, fixtureAdmission1755Context, rows, "RETURN count(*) AS c, count(x) AS cx")
	require.True(t, ok)
	require.Equal(t, []string{"c", "cx"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, int64(2), res.Rows[0][0])
	require.EqualValues(t, int64(2), res.Rows[0][1])

	mixRows := []pipelineRow{{"x": int64(1)}, {"x": int64(2)}}
	fixtureAdmission1756Context, fixtureAdmission1756 := statementTestContext(context.Background(), "RETURN count(*) AS c, x")
	_ = fixtureAdmission1756Context
	res, ok = exec.pipelineApplyReturn(fixtureAdmission1756, fixtureAdmission1756Context, mixRows, "RETURN count(*) AS c, x")
	require.True(t, ok)
	require.Len(t, res.Rows, 2)
	require.EqualValues(t, int64(1), res.Rows[0][0])
	require.EqualValues(t, int64(1), res.Rows[0][1])
	require.EqualValues(t, int64(1), res.Rows[1][0])
	require.EqualValues(t, int64(2), res.Rows[1][1])

	fixtureAdmission1757Context, fixtureAdmission1757 := statementTestContext(context.Background(), "RETURN missing")
	_ = fixtureAdmission1757Context
	res, ok = exec.pipelineApplyReturn(fixtureAdmission1757, fixtureAdmission1757Context, mixRows, "RETURN missing")
	require.False(t, ok)
	require.Nil(t, res)
}

func TestEvaluateListForPipeline_AdditionalBranches(t *testing.T) {
	row := pipelineRow{
		"arr":  []interface{}{int64(1), int64(2)},
		"obj":  map[string]interface{}{"items": []interface{}{"a", "b"}},
		"node": &storage.Node{ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{"vals": []interface{}{int64(3)}}},
	}

	require.Equal(t, []interface{}{int64(1), int64(2)}, evaluateListForPipeline("arr", row))
	require.Equal(t, []interface{}{"a", "b"}, evaluateListForPipeline("obj.items", row))
	require.Equal(t, []interface{}{int64(3)}, evaluateListForPipeline("node.vals", row))
	require.Equal(t, []interface{}{}, evaluateListForPipeline("[]", row))
	require.Equal(t, []interface{}{int64(1), "x", true}, evaluateListForPipeline("[1, 'x', true]", row))
	require.Nil(t, evaluateListForPipeline("[{bad]", row))
	require.Nil(t, evaluateListForPipeline("[unknown]", row))
}

func TestPipelineLiteralParsers_AdditionalBranches(t *testing.T) {
	require.Nil(t, parseLiteralMapForPipeline("[]"))
	require.Equal(t, map[string]interface{}{}, parseLiteralMapForPipeline("{}"))
	require.Nil(t, parseLiteralMapForPipeline("{bad}"))
	require.Nil(t, parseLiteralMapForPipeline("{k: badExpr}"))
	require.Equal(
		t,
		map[string]interface{}{"a": int64(1), "b": map[string]interface{}{"x": "y"}},
		parseLiteralMapForPipeline("{a: 1, b: {x: 'y'}}"),
	)

	v, ok := parseLiteralScalarForPipeline("'x'")
	require.True(t, ok)
	require.Equal(t, "x", v)
	v, ok = parseLiteralScalarForPipeline("\"y\"")
	require.True(t, ok)
	require.Equal(t, "y", v)
	v, ok = parseLiteralScalarForPipeline("true")
	require.True(t, ok)
	require.Equal(t, true, v)
	v, ok = parseLiteralScalarForPipeline("false")
	require.True(t, ok)
	require.Equal(t, false, v)
	v, ok = parseLiteralScalarForPipeline("null")
	require.True(t, ok)
	require.Nil(t, v)
	v, ok = parseLiteralScalarForPipeline("+12")
	require.True(t, ok)
	require.EqualValues(t, int64(12), v)
	v, ok = parseLiteralScalarForPipeline("1.25")
	require.True(t, ok)
	require.Equal(t, 1.25, v)
	_, ok = parseLiteralScalarForPipeline("")
	require.False(t, ok)
	_, ok = parseLiteralScalarForPipeline("not_literal")
	require.False(t, ok)
}

func TestReferencesVariable_AdditionalBranches(t *testing.T) {
	require.False(t, referencesVariable("MATCH (n)", ""))
	require.True(t, referencesVariable("MATCH (n) RETURN n", "n"))
	require.False(t, referencesVariable("RETURN 'n'", "n"))
	require.True(t, referencesVariable("RETURN n.name", "n"))
}
