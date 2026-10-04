package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCreatePropertyValues_ResolveBoundLoopVariables pins the bound-child
// property parsing for CREATE/MERGE (§6.2): a bare identifier or a dotted
// reference whose root lives in the value scope (FOREACH x, UNWIND rows)
// resolves to its real Go value without query-text substitution.
func TestCreatePropertyValues_ResolveBoundLoopVariables(t *testing.T) {
	exec, engine := newTestExecutor(t)
	ctx := context.Background()

	// Bare scalar bound value in CREATE properties.
	res, err := exec.executeInternal(withValueBindings(ctx, map[string]interface{}{"x": int64(7)}), "CREATE (:BoundScalar {k: x})", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	nodes, err := engine.GetNodesByLabel("BoundScalar")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.EqualValues(t, 7, nodes[0].Properties["k"])

	// Dotted reference through a bound map.
	res, err = exec.executeInternal(withValueBindings(ctx, map[string]interface{}{"row": map[string]interface{}{"name": "ann"}}), "CREATE (:BoundDotted {v: row.name})", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	nodes, err = engine.GetNodesByLabel("BoundDotted")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.EqualValues(t, "ann", nodes[0].Properties["v"])

	// MERGE property through a bound value.
	_, err = exec.executeMergeWithContext(withValueBindings(ctx, map[string]interface{}{"x": int64(9)}), "MERGE (m:BoundMerge {id: x})", map[string]*storage.Node{}, map[string]*storage.Edge{})
	require.NoError(t, err)
	nodes, err = engine.GetNodesByLabel("BoundMerge")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.EqualValues(t, 9, nodes[0].Properties["id"])

	// An unbound bare identifier is an unevaluable expression: a SyntaxError
	// with nothing written, as in Neo4j (#514). The historical literal-string
	// fallback stored the query text as data.
	res, err = exec.executeInternal(ctx, "CREATE (:UnboundRef {k: x})", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SyntaxError")
	require.Nil(t, res)
	nodes, err = engine.GetNodesByLabel("UnboundRef")
	require.NoError(t, err)
	require.Empty(t, nodes)
}
