package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH462_RelationshipSetEvaluatesBoundPropertyExpressions(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh462")
			for _, node := range []*storage.Node{
				{ID: "o", Labels: []string{"O"}, Properties: map[string]interface{}{"id": int64(2)}},
				{ID: "i", Labels: []string{"I"}, Properties: map[string]interface{}{"sku": "a1"}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			require.NoError(t, store.CreateEdge(&storage.Edge{
				ID: "has", StartNode: "o", EndNode: "i", Type: "HAS",
				Properties: map[string]interface{}{"n": int64(5)},
			}))
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			assertValue := func(query string, want interface{}) {
				t.Helper()
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
			}
			assertValue("MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = h.n + 1 RETURN h.n AS n", int64(6))
			assertValue("MATCH (:O {id: 2})-[h:HAS]->(i:I) RETURN h.n AS n", int64(6))
			assertValue("MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = 10 RETURN h.n AS n", int64(10))
			assertValue("MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.n = 2 * h.n RETURN h.n AS n", int64(20))
			assertValue("MATCH (:O {id: 2})-[h:HAS]->(i:I) SET h.m = toString(h.n) + 'x' RETURN h.m AS m", "20x")
			assertValue("MATCH (:O {id: 2})-[h:HAS]->(i:I) WITH h, h.n AS old SET h.n = old + 1 RETURN h.n AS n", int64(21))

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			readback, err := exec.Execute(ctx, "MATCH (:O {id: 2})-[h:HAS]->(i:I) RETURN h.n AS n, h.m AS m", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(21), "20x"}}, readback.Rows)
		})
	}
}
