package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH456_RemovePropertyThenSetLabelPreservesMatchedScope(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh456")
			for _, node := range []*storage.Node{
				{ID: "ann", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Ann", "age": 30}},
				{ID: "bob", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Bob", "age": 25}},
				{ID: "cid", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Cid", "age": 35}},
				{ID: "dee", Labels: []string{"P"}, Properties: map[string]interface{}{"name": "Dee", "city": "Riga"}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			result, err := exec.Execute(ctx,
				"MATCH (p:P {name:'Dee'}) REMOVE p.city SET p:VIP RETURN p.name AS n, p.city AS city, labels(p) AS l", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"Dee", nil, []interface{}{"P", "VIP"}}}, result.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
			count, err := exec.Execute(ctx, "MATCH (p:VIP) RETURN count(p) AS vip", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, count.Rows)
			readback, err := exec.Execute(ctx, "MATCH (p:P {name:'Dee'}) RETURN p.city AS city", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{nil}}, readback.Rows)
		})
	}
}
