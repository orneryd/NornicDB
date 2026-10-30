package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH478_ListSubscriptInMatchPropertyMap(t *testing.T) {
	pairs := []interface{}{
		[]interface{}{int64(3), int64(4)},
		[]interface{}{int64(1), int64(2)},
	}
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh478")
			for index := int64(1); index <= 4; index++ {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(string(rune('0' + index))), Labels: []string{"A"},
					Properties: map[string]interface{}{"id": index, "name": "n" + string(rune('0'+index)), "tags": []string{"t", "all"}},
				})
				require.NoError(t, err)
			}
			for _, edge := range []*storage.Edge{
				{ID: "next-1-2", StartNode: "1", EndNode: "2", Type: "NEXT", Properties: map[string]interface{}{"w": int64(10)}},
				{ID: "next-2-3", StartNode: "2", EndNode: "3", Type: "NEXT", Properties: map[string]interface{}{"w": int64(20)}},
			} {
				require.NoError(t, store.CreateEdge(edge))
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			listRows, err := exec.Execute(ctx,
				"UNWIND $pairs AS p MATCH (x:A {id: p[0]}), (y:A {id: p[1]}) MERGE (x)-[r:NEXT]->(y) SET r.w = p[0] * 10 RETURN count(r) AS c",
				map[string]interface{}{"pairs": pairs})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, listRows.Rows)

			all, err := exec.Execute(ctx, "MATCH ()-[r:NEXT]->() RETURN count(r) AS c", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(3)}}, all.Rows)

			updated, err := exec.Execute(ctx, "MATCH (:A {id: 3})-[r:NEXT]->(:A {id: 4}) RETURN r.w AS w", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(30)}}, updated.Rows)

			mapRows, err := exec.Execute(ctx,
				"UNWIND $pm AS p MATCH (x:A {id: p.a}), (y:A {id: p.b}) MERGE (x)-[r:NEXT]->(y) RETURN count(r) AS c",
				map[string]interface{}{"pm": []interface{}{map[string]interface{}{"a": int64(3), "b": int64(4)}}})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, mapRows.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
