package fn

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeCtx builds a Context with the given nodes and rels for testing.
func makeCtx(nodes map[string]*storage.Node, rels map[string]*storage.Edge) Context {
	return Context{
		Nodes:    nodes,
		Rels:     rels,
		Database: "test",
		Eval: func(expr string) (interface{}, error) {
			return nil, nil
		},
	}
}

// makeCtxWithEval builds a Context where Eval returns the given value for any expr.
func makeCtxWithEval(v interface{}) Context {
	return Context{
		Nodes:    map[string]*storage.Node{},
		Rels:     map[string]*storage.Edge{},
		Database: "test",
		Eval: func(expr string) (interface{}, error) {
			return v, nil
		},
	}
}

// ============================================================================
// evalID – edge/via-eval branches
// ============================================================================

func TestEvalID_ViaRel(t *testing.T) {
	rel := &storage.Edge{ID: "nornic:r1", Type: "KNOWS"}
	ctx := makeCtx(nil, map[string]*storage.Edge{"r": rel})
	v, _, err := EvaluateFunction("id", []string{"r"}, ctx)
	require.NoError(t, err)
	assert.Equal(t, "nornic:r1", v)
}

func TestEvalID_ViaEval_Node(t *testing.T) {
	node := &storage.Node{ID: "nornic:n1"}
	ctx := makeCtxWithEval(node)
	v, _, err := EvaluateFunction("id", []string{"expr"}, ctx)
	require.NoError(t, err)
	assert.Equal(t, "nornic:n1", v)
}

func TestEvalID_ViaEval_Edge(t *testing.T) {
	edge := &storage.Edge{ID: "nornic:e1"}
	ctx := makeCtxWithEval(edge)
	v, _, err := EvaluateFunction("id", []string{"expr"}, ctx)
	require.NoError(t, err)
	assert.Equal(t, "nornic:e1", v)
}

func TestEvalID_NoArgs(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("id", []string{}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

// ============================================================================
// evalElementID – rel branch
// ============================================================================

func TestEvalElementID_ViaRel(t *testing.T) {
	rel := &storage.Edge{ID: "nornic:r1", Type: "LIKES"}
	ctx := makeCtx(nil, map[string]*storage.Edge{"r": rel})
	v, _, err := EvaluateFunction("elementid", []string{"r"}, ctx)
	require.NoError(t, err)
	assert.Equal(t, "5:test:nornic:r1", v)
}

func TestEvalElementID_NoArgs(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("elementid", []string{}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

// ============================================================================
// evalLabels – via-eval branch
// ============================================================================

func TestEvalLabels_ViaEval_Node(t *testing.T) {
	node := &storage.Node{ID: "nornic:n1", Labels: []string{"Person", "Employee"}}
	ctx := makeCtxWithEval(node)
	v, _, err := EvaluateFunction("labels", []string{"expr"}, ctx)
	require.NoError(t, err)
	labels, ok := v.([]interface{})
	require.True(t, ok)
	assert.Contains(t, labels, "Person")
	assert.Contains(t, labels, "Employee")
}

func TestEvalLabels_NoArgs(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("labels", []string{}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestEvalLabels_NoMatch(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("labels", []string{"nonexistent"}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

// ============================================================================
// evalType – via-eval map branch
// ============================================================================

func TestEvalType_ViaEvalMap(t *testing.T) {
	ctx := makeCtxWithEval(map[string]interface{}{"type": "CONNECTED_TO"})
	v, _, err := EvaluateFunction("type", []string{"expr"}, ctx)
	require.NoError(t, err)
	assert.Equal(t, "CONNECTED_TO", v)
}

func TestEvalType_NoArgs(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("type", []string{}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

// ============================================================================
// evalKeys – rel branch
// ============================================================================

func TestEvalKeys_ViaRel(t *testing.T) {
	rel := &storage.Edge{
		ID:         "nornic:r1",
		Type:       "KNOWS",
		Properties: map[string]interface{}{"since": 2020, "weight": 0.9},
	}
	ctx := makeCtx(nil, map[string]*storage.Edge{"r": rel})
	v, _, err := EvaluateFunction("keys", []string{"r"}, ctx)
	require.NoError(t, err)
	keys, ok := v.([]interface{})
	require.True(t, ok)
	assert.Len(t, keys, 2)
}

func TestEvalKeys_NoArgs(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("keys", []string{}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

// ============================================================================
// evalProperties – rel branch
// ============================================================================

func TestEvalProperties_ViaRel(t *testing.T) {
	rel := &storage.Edge{
		ID:         "nornic:r1",
		Type:       "LIKES",
		Properties: map[string]interface{}{"strength": 0.8},
	}
	ctx := makeCtx(nil, map[string]*storage.Edge{"r": rel})
	v, _, err := EvaluateFunction("properties", []string{"r"}, ctx)
	require.NoError(t, err)
	props, ok := v.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, 0.8, props["strength"])
}

func TestEvalProperties_NoArgs(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("properties", []string{}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestEvalProperties_NoMatch(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("properties", []string{"ghost"}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

// ============================================================================
// evalToUpper / evalToLower – nil branch
// ============================================================================

func TestEvalToUpper_Nil(t *testing.T) {
	ctx := makeCtxWithEval(nil)
	v, _, err := EvaluateFunction("toupper", []string{"x"}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestEvalToLower_Nil(t *testing.T) {
	ctx := makeCtxWithEval(nil)
	v, _, err := EvaluateFunction("tolower", []string{"x"}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

func TestEvalToUpper_NoArgs(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("toupper", []string{}, ctx)
	require.NoError(t, err)
	assert.Nil(t, v)
}

// ============================================================================
// evalSize – []string branch
// ============================================================================

func TestEvalSize_StringSlice(t *testing.T) {
	ctx := makeCtxWithEval([]string{"a", "b", "c"})
	v, _, err := EvaluateFunction("size", []string{"x"}, ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), v)
}

func TestEvalSize_NoArgs(t *testing.T) {
	ctx := makeCtx(nil, nil)
	v, _, err := EvaluateFunction("size", []string{}, ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), v)
}
