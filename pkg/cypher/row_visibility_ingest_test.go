package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

var rowVisibilitySink any

// A row property access must not copy and project unrelated parameter data.
// Real Northwind batches perform this access for every MATCH/CREATE field.
func TestVisibleRowPropertyAccessAvoidsUnselectedProjection(t *testing.T) {
	e, _ := reviewPropertyExecutor(t)
	expr := &rowPropertyChainNode{variable: "row", chain: "quantity"}
	values := map[string]interface{}{"row": map[string]interface{}{"quantity": int64(3), "unselected": map[string]interface{}{"name": "other", "values": []interface{}{int64(1), int64(2)}}}}
	value, ok, err := expr.eval(e, values, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(3), value)
	allocs := testing.AllocsPerRun(100, func() { rowVisibilitySink, _, _ = expr.eval(e, values, nil) })
	require.LessOrEqual(t, allocs, float64(1), "scalar row access should not copy unrelated nested values")
}

func BenchmarkVisibleRowPropertyAccess(b *testing.B) {
	e, _ := reviewPropertyExecutor(b)
	expr := &rowPropertyChainNode{variable: "row", chain: "quantity"}
	values := map[string]interface{}{"row": map[string]interface{}{"quantity": int64(3), "unselected": map[string]interface{}{"name": "other", "values": []interface{}{int64(1), int64(2)}}}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rowVisibilitySink, _, _ = expr.eval(e, values, nil)
	}
}

func TestVisibleRowPropertyChainPreservesNestedEntityVisibility(t *testing.T) {
	e, b := reviewPropertyExecutor(t)
	node, err := b.GetNode("review:a")
	require.NoError(t, err)
	edge, err := b.GetEdge("review:ab")
	require.NoError(t, err)
	base := map[string]interface{}{"outer": map[string]interface{}{"node": node, "edge": edge, "plain": map[string]interface{}{"quantity": int64(3)}}}
	for _, chain := range []string{"outer.node.body", "outer.node.title", "outer.edge.summary", "outer.node", "outer.plain.quantity", "outer.plain", "outer.absent"} {
		t.Run(chain, func(t *testing.T) {
			oldValue, oldOK := evaluateRowPropertyChain(e.queryVisibleValue(base), chain, nil, false)
			got, ok := evaluateRowPropertyChain(base, chain, e, false)
			require.Equal(t, oldOK, ok)
			require.Equal(t, oldValue, got)
		})
	}
	hidden, ok := evaluateRowPropertyChain(base, "outer.node.body", e, false)
	require.True(t, ok)
	require.Nil(t, hidden)
	require.Equal(t, "hidden", node.Properties["body"], "visibility must not mutate the stored binding")
	result, err := e.Execute(context.Background(), "UNWIND $rows AS row RETURN row.outer.node.body AS hidden, row.outer.node.title AS title", map[string]interface{}{"rows": []interface{}{base}})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil, "a"}}, result.Rows)
}
