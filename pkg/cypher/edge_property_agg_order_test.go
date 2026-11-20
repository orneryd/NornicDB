package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// edgeAggReadOrderExecutor seeds one customer and four products. Each product
// receives the same two integer ratings, and half of them see them in the
// opposite relationship read order.
func edgeAggReadOrderExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	_, err := store.CreateNode(t.Context(), &storage.Node{ID: "c", Labels: []string{"Customer"}})
	require.NoError(t, err)
	const products = 4
	for i := 0; i < products; i++ {
		id := storage.NodeID(fmt.Sprintf("p%02d", i))
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Product"}, Properties: map[string]interface{}{"name": string(id)}})
		require.NoError(t, err)
		ratings := []int64{1, 2}
		if i%2 == 1 {
			ratings = []int64{2, 1}
		}
		for j, rating := range ratings {
			require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%02d-%d", i, j)), StartNode: "c", EndNode: id, Type: "REVIEWED", Properties: map[string]interface{}{"rating": rating}}))
		}
	}
	return exec
}

// TestEdgePropertyAggMinMaxIgnoreReadOrder runs the
// min/max shape through Execute. Per-end-node
// min and max don't depend on the order relationships are read in: half the
// products see 1 then 2 and the other half 2 then 1, so every group must still
// report min 1 and max 2.
func TestEdgePropertyAggMinMaxIgnoreReadOrder(t *testing.T) {
	const query = "MATCH (c)-[r:REVIEWED]->(p) RETURN p.name AS product, min(r.rating) AS min, max(r.rating) AS max"
	exec := edgeAggReadOrderExecutor(t)
	ctx := context.Background()

	res, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"product", "min", "max"}, res.Columns)
	require.Len(t, res.Rows, 4)

	seen := make(map[string]bool, len(res.Rows))
	for _, row := range res.Rows {
		require.Len(t, row, 3)
		product, ok := row[0].(string)
		require.True(t, ok, "grouped name is a string, got %T", row[0])
		seen[product] = true
		require.Equal(t, int64(1), row[1], "min must not depend on relationship read order")
		require.Equal(t, int64(2), row[2], "max must not depend on relationship read order")
	}
	require.Equal(t, map[string]bool{"p00": true, "p01": true, "p02": true, "p03": true}, seen,
		"every product is its own group")
}
