package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH476_StoredDatetimeComparesAsTemporalValue(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		mode := "autocommit"
		if explicit {
			mode = "explicit transaction"
		}
		t.Run(mode, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "gh476")
			for index := int64(1); index <= 4; index++ {
				_, err := store.CreateNode(&storage.Node{
					ID: storage.NodeID(string(rune('0' + index))), Labels: []string{"A"},
					Properties: map[string]interface{}{"id": index, "name": "n" + string(rune('0'+index)), "tags": []string{"t", "all"}},
				})
				require.NoError(t, err)
			}
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "MATCH (a:A) SET a.ts = datetime('2026-09-1' + toString(a.id) + 'T10:00:00Z') RETURN count(a) AS c", nil)
			require.NoError(t, err)

			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}

			stored, err := exec.Execute(ctx, "MATCH (a:A) WHERE a.ts >= datetime('2026-09-13T00:00:00Z') RETURN count(a) AS c", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}}, stored.Rows)

			stringComparison, err := exec.Execute(ctx, "MATCH (a:A) WHERE a.ts >= '2026-09-13T00:00:00Z' RETURN count(a) AS c", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, stringComparison.Rows)

			ordered, err := exec.Execute(ctx, "MATCH (a:A) RETURN a.id AS id ORDER BY a.ts DESC LIMIT 1", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(4)}}, ordered.Rows)

			if explicit {
				_, err = exec.Execute(ctx, "COMMIT", nil)
				require.NoError(t, err)
			}
		})
	}
}
