package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH464_WithOrderByPreservesAggregateAlias(t *testing.T) {
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "ORDER BY aggregate alias in WITH",
			query: "MATCH (o:O)-[h:HAS]->(i:I) WITH i, sum(h.n) AS sold ORDER BY sold DESC RETURN i.sku AS s, sold",
		},
		{
			name:  "ORDER BY aggregate alias in RETURN control",
			query: "MATCH (o:O)-[h:HAS]->(i:I) WITH i, sum(h.n) AS sold RETURN i.sku AS s, sold ORDER BY sold DESC",
		},
	}
	want := [][]interface{}{{"a1", int64(7)}, {"c3", int64(1)}}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh464")
			for _, node := range []*storage.Node{
				{ID: "a1", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "a1", "price": 10.5, "qty": 3}},
				{ID: "b2", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "b2", "price": 20.0, "qty": 0}},
				{ID: "c3", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "c3", "price": 5.25, "qty": 7}},
				{ID: "o1", Labels: []string{"O"}, Properties: map[string]interface{}{"id": int64(1)}},
				{ID: "o2", Labels: []string{"O"}, Properties: map[string]interface{}{"id": int64(2)}},
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

			for _, testCase := range queries {
				t.Run(testCase.name, func(t *testing.T) {
					result, err := exec.Execute(ctx, testCase.query, nil)
					require.NoError(t, err)
					require.Equal(t, want, result.Rows)
				})
			}

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
