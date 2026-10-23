package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Values are pinned to Neo4j 5.26.30, reference job
// j20261020193359-13000-7742. Exercise the same operands in imported CALL scope.
func TestAggregateDistinctInputPublic(t *testing.T) {
	cases := []struct {
		name, query string
		want        interface{}
	}{
		{"list", "UNWIND [1,1,2] AS k WITH k,k%2 AS h RETURN count(DISTINCT [k,h]) AS value", int64(2)},
		{"collect", "UNWIND [1,1,2] AS k WITH k,k%2 AS h ORDER BY k RETURN collect(DISTINCT [k,h]) AS value", []interface{}{[]interface{}{int64(1), int64(1)}, []interface{}{int64(2), int64(0)}}},
		{"quoted-variable", "WITH [1,2] AS `DISTINCT` RETURN count(`DISTINCT`[0]) AS value", int64(1)},
		{"empty", "UNWIND [] AS k RETURN count(DISTINCT [k]) AS value", int64(0)},
		{"null-element", "UNWIND [null,null] AS k RETURN count(DISTINCT [k]) AS value", int64(1)},
		{"comprehension", "UNWIND [1,1,2] AS k WITH k,k%2 AS h RETURN count(DISTINCT [x IN [k,h] | x+1]) AS value", int64(2)},
		{"wrapped", "UNWIND [1,1,2] AS k WITH k,k%2 AS h RETURN 10+count(DISTINCT [k,h]) AS value", int64(12)},
	}
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, scoped := range []bool{false, true} {
				for _, sample := range cases {
					t.Run(fmt.Sprintf("%s/normalize=%t/explicit=%t/scoped=%t", sample.name, normalize, explicit, scoped), func(t *testing.T) {
						config.SetCypherQueryNormalizationEnabled(normalize)
						store := storage.NewMemoryEngine()
						t.Cleanup(func() { require.NoError(t, store.Close()) })
						executor := NewStorageExecutor(store)
						ctx := context.Background()
						if explicit {
							_, err := executor.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
						}
						query := sample.query
						if scoped {
							query = "WITH [1,2] AS big CALL (big) { " + query + " } RETURN value"
						}
						result, err := executor.Execute(ctx, query, nil)
						require.NoError(t, err, query)
						require.Equal(t, [][]interface{}{{sample.want}}, result.Rows, query)
					})
				}
			}
		}
	}
	t.Run("undefined-input-still-rejected", func(t *testing.T) {
		store := storage.NewMemoryEngine()
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		_, err := NewStorageExecutor(store).Execute(context.Background(), "UNWIND [1] AS k RETURN count(DISTINCT [missing]) AS value", nil)
		require.ErrorContains(t, err, "missing")
	})
}
