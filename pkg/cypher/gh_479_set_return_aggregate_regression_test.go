package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH479_SetThenReturnAggregatesAllMatchedRows(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh479")
			for index := int64(1); index <= 4; index++ {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(string(rune('0' + index))), Labels: []string{"A"},
					Properties: map[string]interface{}{"id": index, "name": "n" + string(rune('0'+index)), "tags": []string{"t", "all"}},
				})
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			query := "MATCH (a:A) WHERE a.id <= 2 SET a.hits = coalesce(a.hits, 0) + 1 RETURN sum(a.hits) AS s"
			first, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, first.Rows)
			second, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(4)}}, second.Rows)
			readback, err := exec.Execute(ctx, "MATCH (a:A) WHERE a.id <= 2 RETURN a.id AS id, a.hits AS h ORDER BY id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(2)}}, readback.Rows)

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
