package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH459_HiddenFunctionOrderKeyWithLimit(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh459")
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

			result, err := exec.Execute(ctx, "MATCH (p:P) RETURN p.name AS n ORDER BY toLower(p.name) DESC", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"Dee"}, {"Cid"}, {"Bob"}, {"Ann"}}, result.Rows)

			limited, err := exec.Execute(ctx, "MATCH (p:P) RETURN p.name AS n ORDER BY toLower(p.name) DESC LIMIT 1", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"Dee"}}, limited.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
