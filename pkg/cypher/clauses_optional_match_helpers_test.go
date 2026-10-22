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

	fixtureAdmission1046Context, fixtureAdmission1046 := expressionTestContext(ctx, "m.name")
	_ = fixtureAdmission1046Context
	val := exec.resolveReturnExprFromVarMap(fixtureAdmission1046, fixtureAdmission1046Context, "m.name", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, "Bob", val)

	fixtureAdmission1047Context, fixtureAdmission1047 := expressionTestContext(ctx, "r.weight")
	_ = fixtureAdmission1047Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1047, fixtureAdmission1047Context, "r.weight", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, 0.8, val)

	fixtureAdmission1048Context, fixtureAdmission1048 := expressionTestContext(ctx, "n.name")
	_ = fixtureAdmission1048Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1048, fixtureAdmission1048Context, "n.name", map[string]interface{}{"n": n1}, "m", "r", nil, nil)
	require.Equal(t, "Alice", val)

	fixtureAdmission1049Context, fixtureAdmission1049 := expressionTestContext(ctx, "m")
	_ = fixtureAdmission1049Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1049, fixtureAdmission1049Context, "m", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, n2, val)

	fixtureAdmission1050Context, fixtureAdmission1050 := expressionTestContext(ctx, "r")
	_ = fixtureAdmission1050Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1050, fixtureAdmission1050Context, "r", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, rel, val)

	fixtureAdmission1051Context, fixtureAdmission1051 := expressionTestContext(ctx, "n")
	_ = fixtureAdmission1051Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1051, fixtureAdmission1051Context, "n", map[string]interface{}{"n": n1}, "m", "r", nil, nil)
	require.Equal(t, n1, val)

	fixtureAdmission1052Context, fixtureAdmission1052 := expressionTestContext(ctx, "42")
	_ = fixtureAdmission1052Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1052, fixtureAdmission1052Context, "42", map[string]interface{}{}, "m", "r", nil, nil)
	require.EqualValues(t, 42, val)
}
