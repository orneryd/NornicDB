package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineApplyReturn_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	rows := []pipelineRow{{"x": int64(1)}, {"x": int64(2)}}
	res, ok := exec.pipelineApplyReturn(context.Background(), rows, "RETURN ")
	require.False(t, ok)
	require.Nil(t, res)

	res, ok = exec.pipelineApplyReturn(context.Background(), rows, "RETURN count(*) AS c, count(x) AS cx")
	require.True(t, ok)
	require.Equal(t, []string{"c", "cx"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, int64(2), res.Rows[0][0])
	require.EqualValues(t, int64(2), res.Rows[0][1])

	mixRows := []pipelineRow{{"x": int64(1)}, {"x": int64(2)}}
	res, ok = exec.pipelineApplyReturn(context.Background(), mixRows, "RETURN count(*) AS c, x")
	require.True(t, ok)
	require.Len(t, res.Rows, 2)
	require.EqualValues(t, int64(1), res.Rows[0][0])
	require.EqualValues(t, int64(1), res.Rows[0][1])
	require.EqualValues(t, int64(1), res.Rows[1][0])
	require.EqualValues(t, int64(2), res.Rows[1][1])

	res, ok = exec.pipelineApplyReturn(context.Background(), mixRows, "RETURN missing")
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
