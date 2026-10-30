package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH465_PropertyAccessOnEndpointFunctionResult(t *testing.T) {
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "direct function-result property access",
			query: "MATCH (:O {id: 1})-[h:HAS]->(:I {sku:'a1'}) RETURN startNode(h).id AS from_id, endNode(h).sku AS to",
		},
		{
			name:  "WITH-bound endpoint control",
			query: "MATCH (:O {id: 1})-[h:HAS]->(:I {sku:'a1'}) WITH startNode(h) AS s, endNode(h) AS e RETURN s.id AS from_id, e.sku AS to",
		},
	}

	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh465")
			for _, node := range []*storage.Node{
				{ID: "order", Labels: []string{"O"}, Properties: map[string]interface{}{"id": int64(1)}},
				{ID: "item", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "a1"}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			require.NoError(t, store.CreateEdge(&storage.Edge{
				ID: "has", StartNode: "order", EndNode: "item", Type: "HAS",
				Properties: map[string]interface{}{"n": int64(2)},
			}))
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
					require.Equal(t, [][]interface{}{{int64(1), "a1"}}, result.Rows)
				})
			}

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
