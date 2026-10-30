package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH477_MinMaxAggregateStrings(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh477")
			for index, name := range []string{"n1", "n2", "n3", "n4"} {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(name), Labels: []string{"A"},
					Properties: map[string]interface{}{"id": int64(index + 1), "name": name, "tags": []string{"t", "all"}},
				})
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			result, err := exec.Execute(ctx, "MATCH (a:A) RETURN min(a.name) AS lo, max(a.name) AS hi, min(a.id) AS lo_id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"n1", "n4", int64(1)}}, result.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
