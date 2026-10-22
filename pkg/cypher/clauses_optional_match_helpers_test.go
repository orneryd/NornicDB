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

	fixtureAdmission1035Context, fixtureAdmission1035 := expressionTestContext(ctx, "m.name")
	_ = fixtureAdmission1035Context
	val := exec.resolveReturnExprFromVarMap(fixtureAdmission1035, fixtureAdmission1035Context, "m.name", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, "Bob", val)

	fixtureAdmission1036Context, fixtureAdmission1036 := expressionTestContext(ctx, "r.weight")
	_ = fixtureAdmission1036Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1036, fixtureAdmission1036Context, "r.weight", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, 0.8, val)

	fixtureAdmission1037Context, fixtureAdmission1037 := expressionTestContext(ctx, "n.name")
	_ = fixtureAdmission1037Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1037, fixtureAdmission1037Context, "n.name", map[string]interface{}{"n": n1}, "m", "r", nil, nil)
	require.Equal(t, "Alice", val)

	fixtureAdmission1038Context, fixtureAdmission1038 := expressionTestContext(ctx, "m")
	_ = fixtureAdmission1038Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1038, fixtureAdmission1038Context, "m", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, n2, val)

	fixtureAdmission1039Context, fixtureAdmission1039 := expressionTestContext(ctx, "r")
	_ = fixtureAdmission1039Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1039, fixtureAdmission1039Context, "r", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, rel, val)

	fixtureAdmission1040Context, fixtureAdmission1040 := expressionTestContext(ctx, "n")
	_ = fixtureAdmission1040Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1040, fixtureAdmission1040Context, "n", map[string]interface{}{"n": n1}, "m", "r", nil, nil)
	require.Equal(t, n1, val)

	fixtureAdmission1041Context, fixtureAdmission1041 := expressionTestContext(ctx, "42")
	_ = fixtureAdmission1041Context
	val = exec.resolveReturnExprFromVarMap(fixtureAdmission1041, fixtureAdmission1041Context, "42", map[string]interface{}{}, "m", "r", nil, nil)
	require.EqualValues(t, 42, val)
}
