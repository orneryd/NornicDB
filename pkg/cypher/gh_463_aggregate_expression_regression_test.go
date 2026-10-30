package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH463_AggregateWrappedInArithmeticUsesAllRows(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh463")
			for _, node := range []*storage.Node{
				{ID: "a", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "a1", "price": 10.5, "qty": 3}},
				{ID: "b", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "b2", "price": 20.0, "qty": 0}},
				{ID: "c", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "c3", "price": 5.25, "qty": 7}},
				{ID: "o1", Labels: []string{"O"}, Properties: map[string]interface{}{"id": 1}},
				{ID: "o2", Labels: []string{"O"}, Properties: map[string]interface{}{"id": 2}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			for _, edge := range []*storage.Edge{
				{ID: "o1-a", StartNode: "o1", EndNode: "a", Type: "HAS", Properties: map[string]interface{}{"n": int64(2)}},
				{ID: "o1-c", StartNode: "o1", EndNode: "c", Type: "HAS", Properties: map[string]interface{}{"n": int64(1)}},
				{ID: "o2-a", StartNode: "o2", EndNode: "a", Type: "HAS", Properties: map[string]interface{}{"n": int64(5)}},
			} {
				require.NoError(t, store.CreateEdge(edge))
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			average, err := exec.Execute(ctx, "MATCH (i:I) RETURN avg(i.price) AS av", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{float64(11.916666666666666)}}, average.Rows)

			rounded, err := exec.Execute(ctx, "MATCH (i:I) RETURN round(avg(i.price) * 100) / 100 AS av", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{float64(11.92)}}, rounded.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
