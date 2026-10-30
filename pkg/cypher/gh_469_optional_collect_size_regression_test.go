package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH469_OptionalCollectSizeMatchesCollectedValues(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh469")
			for _, node := range []*storage.Node{
				{ID: "a1", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "a1", "price": float64(10.5), "qty": int64(3)}},
				{ID: "b2", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "b2", "price": float64(20), "qty": int64(0)}},
				{ID: "c3", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "c3", "price": float64(5.25), "qty": int64(7)}},
				{ID: "o1", Labels: []string{"O"}, Properties: map[string]interface{}{"id": int64(1)}},
				{ID: "o2", Labels: []string{"O"}, Properties: map[string]interface{}{"id": int64(2)}},
				{ID: "o3", Labels: []string{"O"}, Properties: map[string]interface{}{"id": int64(3)}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			for _, edge := range []*storage.Edge{
				{ID: "o1-a1", StartNode: "o1", EndNode: "a1", Type: "HAS", Properties: map[string]interface{}{"n": int64(2)}},
				{ID: "o1-c3", StartNode: "o1", EndNode: "c3", Type: "HAS", Properties: map[string]interface{}{"n": int64(1)}},
				{ID: "o2-a1", StartNode: "o2", EndNode: "a1", Type: "HAS", Properties: map[string]interface{}{"n": int64(5)}},
			} {
				require.NoError(t, store.CreateEdge(edge))
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			optional, err := exec.Execute(ctx, `
MATCH (o:O) OPTIONAL MATCH (o)-[h:HAS]->(i:I)
WITH o, collect(i.sku) AS skus
RETURN o.id AS o, size(skus) AS n, skus ORDER BY o`, nil)
			require.NoError(t, err)
			require.Len(t, optional.Rows, 3)
			require.Equal(t, []interface{}{int64(1), int64(2)}, optional.Rows[0][:2])
			require.ElementsMatch(t, []interface{}{"a1", "c3"}, optional.Rows[0][2])
			require.Equal(t, []interface{}{int64(2), int64(1)}, optional.Rows[1][:2])
			require.ElementsMatch(t, []interface{}{"a1"}, optional.Rows[1][2])
			require.Equal(t, []interface{}{int64(3), int64(0)}, optional.Rows[2][:2])
			require.Empty(t, optional.Rows[2][2], "OPTIONAL MATCH with no child should collect an empty list")

			plain, err := exec.Execute(ctx, `
MATCH (o:O) MATCH (o)-[h:HAS]->(i:I)
WITH o, collect(i.sku) AS skus
RETURN o.id AS o, size(skus) AS n ORDER BY o`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(1)}}, plain.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
