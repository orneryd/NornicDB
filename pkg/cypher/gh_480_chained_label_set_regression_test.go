package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH480_ChainedLabelSetMatchesCommaForm(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh480")
			for _, id := range []int64{1, 2} {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(string(rune('0' + id))), Labels: []string{"A"},
					Properties: map[string]interface{}{"id": id, "name": "n" + string(rune('0'+id))},
				})
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			if explicit {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			for _, testCase := range []struct {
				query string
				id    int64
			}{
				{"MATCH (a:A {id: 1}) SET a:Extra:Hot RETURN labels(a) AS l", 1},
				{"MATCH (a:A {id: 2}) SET a:Extra, a:Hot RETURN labels(a) AS l", 2},
			} {
				result, err := exec.Execute(ctx, testCase.query, nil)
				require.NoError(t, err, testCase.query)
				require.Len(t, result.Rows, 1)
				require.ElementsMatch(t, []interface{}{"A", "Extra", "Hot"}, result.Rows[0][0])
			}
			repeated, err := exec.Execute(ctx, "MATCH (a:A {id: 1}) SET a:Extra:Hot RETURN labels(a) AS l", nil)
			require.NoError(t, err)
			require.ElementsMatch(t, []interface{}{"A", "Extra", "Hot"}, repeated.Rows[0][0])

			if explicit {
				_, err := exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
