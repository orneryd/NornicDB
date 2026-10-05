package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResolveReturnExprFromVarMap_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "resolve_return_expr_cov"))
	ctx := context.Background()

	n1 := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	n2 := &storage.Node{ID: storage.NodeID("n2"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
	rel := &storage.Edge{ID: storage.EdgeID("r1"), Type: "KNOWS", StartNode: n1.ID, EndNode: n2.ID, Properties: map[string]interface{}{"weight": 0.8}}

	val := exec.resolveReturnExprFromVarMap(ctx, "m.name", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, "Bob", val)

	val = exec.resolveReturnExprFromVarMap(ctx, "r.weight", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, 0.8, val)

	val = exec.resolveReturnExprFromVarMap(ctx, "n.name", map[string]interface{}{"n": n1}, "m", "r", nil, nil)
	require.Equal(t, "Alice", val)

	val = exec.resolveReturnExprFromVarMap(ctx, "m", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, n2, val)

	val = exec.resolveReturnExprFromVarMap(ctx, "r", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, rel, val)

	val = exec.resolveReturnExprFromVarMap(ctx, "n", map[string]interface{}{"n": n1}, "m", "r", nil, nil)
	require.Equal(t, n1, val)

	val = exec.resolveReturnExprFromVarMap(ctx, "42", map[string]interface{}{}, "m", "r", nil, nil)
	require.EqualValues(t, 42, val)
}
