package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Lowering a supported conditional introduces a branch binding. It is an
// execution dependency even though wildcard output must hide it. These public
// contracts also cover a nested CALL and its zero-input schema path.
func TestResultHeaderConditionalBindingScope(t *testing.T) {
	for _, normalized := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, sample := range []struct {
				query   string
				columns []string
				rows    [][]interface{}
			}{
				{"WHEN true THEN RETURN 1 AS x", []string{"x"}, [][]interface{}{{int64(1)}}},
				{"CALL () { WHEN true THEN RETURN 1 AS x } RETURN x", []string{"x"}, [][]interface{}{{int64(1)}}},
				{"WHEN null THEN RETURN 1 AS x ELSE RETURN 2 AS x", []string{"x"}, [][]interface{}{{int64(2)}}},
				{"UNWIND [] AS i CALL (i) { WHEN i = 1 THEN RETURN 'a' AS r } RETURN i, r", []string{"i", "r"}, nil},
			} {
				t.Run(fmt.Sprintf("normalization=%v/explicit=%v/%s", normalized, explicit, sample.query), func(t *testing.T) {
					restore := config.WithCypherQueryNormalizationDisabled()
					defer restore()
					config.SetCypherQueryNormalizationEnabled(normalized)
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
					ctx := context.Background()
					if explicit {
						_, err := executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer executor.Execute(ctx, "ROLLBACK", nil)
					}
					result, err := executor.Execute(ctx, "CYPHER 25 "+sample.query, nil)
					require.NoError(t, err)
					require.Equal(t, sample.columns, result.Columns)
					if len(sample.rows) == 0 {
						require.Empty(t, result.Rows)
					} else {
						require.Equal(t, sample.rows, result.Rows)
					}
				})
			}
		}
	}
}
