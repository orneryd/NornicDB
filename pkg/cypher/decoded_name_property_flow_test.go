package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNamePropertyFlow(t *testing.T) {
	for _, key := range []string{"plain", " ", "  ", "\t", "a.b", "a`b", "`a`"} {
		t.Run(key, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "property_flow"))
			ctx := context.Background()
			written := cyphertext.QuoteName(key)
			_, err := exec.Execute(ctx, "CREATE (:PropertyFlow {"+written+":7})", nil)
			require.NoError(t, err)
			for _, tc := range []struct {
				name, tail string
				value      int64
			}{
				{"projected scan", "WHERE n." + written + " = 7 RETURN n." + written + " AS value", 7},
				{"filtered count", "WHERE n." + written + " = 7 RETURN count(n) AS value", 1},
				{"with scan", "WITH n RETURN n." + written + " AS value", 7},
				{"distinct order", "RETURN DISTINCT n." + written + " AS value ORDER BY n." + written, 7},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := "MATCH (n:PropertyFlow) " + tc.tail
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					require.Equal(t, [][]interface{}{{tc.value}}, result.Rows, query)
				})
			}
		})
	}
}
