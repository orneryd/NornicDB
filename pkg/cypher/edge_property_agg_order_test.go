package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Per-end-node min and max don't depend on the order relationships are read
// in. Relationships are read in ID order, so half the products see 1 then 2
// and the other half 2 then 1.
func TestEdgePropertyAggMinMaxIgnoreReadOrder(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := store.CreateNode(&storage.Node{ID: "c", Labels: []string{"Customer"}})
	require.NoError(t, err)
	const products = 4
	for i := 0; i < products; i++ {
		id := storage.NodeID(fmt.Sprintf("p%02d", i))
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Product"}, Properties: map[string]interface{}{"name": string(id)}})
		require.NoError(t, err)
		ratings := []int64{1, 2}
		if i%2 == 1 {
			ratings = []int64{2, 1}
		}
		for j, rating := range ratings {
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%02d-%d", i, j)), StartNode: "c", EndNode: id, Type: "REVIEWED", Properties: map[string]interface{}{"rating": rating}}))
		}
	}
	res, err := exec.executeEdgePropertyAggOptimized(context.Background(),
		"MATCH (c)-[r:REVIEWED]->(p) RETURN p.name AS product, min(r.rating) AS min, max(r.rating) AS max",
		PatternInfo{Pattern: PatternEdgePropertyAgg, RelType: "REVIEWED", AggProperty: "rating", AggFunctions: []string{"min", "max"}, Limit: products})
	require.NoError(t, err)
	require.Len(t, res.Rows, products)
	for _, row := range res.Rows {
		require.Equal(t, 1.0, row[1])
		require.Equal(t, 2.0, row[2])
	}
}
