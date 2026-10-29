package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameHintSourcePreservation(t *testing.T) {
	for _, name := range []string{"Plain", "two  spaces", "\t", "  "} {
		for _, hinted := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/without_hint", true: "/with_hint"}[hinted], func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5_hint")
				executor := NewStorageExecutor(store)
				label, key := cyphertext.QuoteName(name), cyphertext.QuoteName(name)
				ctx := context.Background()
				_, err := executor.Execute(ctx, "CREATE (n:"+label+" {"+key+":7})", nil)
				require.NoError(t, err)
				query := "MATCH (n:" + label + ") "
				if hinted {
					query += "USING INDEX n:" + label + "(" + key + ") "
				}
				query += "RETURN n." + key + " AS value"
				hints, clean := ParseIndexHints(query)
				require.Len(t, hints, map[bool]int{false: 0, true: 1}[hinted])
				if hinted {
					require.Equal(t, name, hints[0].Label)
					require.Equal(t, name, hints[0].Property)
				}
				result, err := executor.Execute(ctx, clean, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows, "removing hints must preserve the source of every decoded name")
			})
		}
	}
	t.Run("literal", func(t *testing.T) {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r5_hint"))
		_, clean := ParseIndexHints("RETURN 'two  spaces' AS value")
		result, err := executor.Execute(context.Background(), clean, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"two  spaces"}}, result.Rows)
	})
}
