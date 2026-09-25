package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestWithPaginationThenWhere covers the #571 section on WITH … SKIP / LIMIT
// n WHERE p: the SKIP / LIMIT expression ends at WHERE, and the WHERE filters
// the rows SKIP / LIMIT keep, as in Neo4j 5.26.
func TestWithPaginationThenWhere(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:P {id: 1, g: 0}), (:P {id: 2, g: 1}), (:P {id: 5, g: 1})", nil)
	require.NoError(t, err)
	for query, want := range map[string][]interface{}{
		"MATCH (n:P) WITH n ORDER BY n.id LIMIT 2 WHERE n.id > 1 RETURN collect(n.id) AS l":             {int64(2)},
		"MATCH (n:P) WITH n ORDER BY n.id SKIP 1 WHERE n.id > 1 RETURN collect(n.id) AS l":              {int64(2), int64(5)},
		"MATCH (n:P) WITH n ORDER BY n.id SKIP 1 LIMIT 1 WHERE n.g = 1 RETURN collect(n.id) AS l":       {int64(2)},
		"MATCH (n:P) WITH n ORDER BY n.id SKIP 1 WHERE n.g = 1 RETURN collect(n.id) AS l":               {int64(2), int64(5)},
		"MATCH (n:P) WITH DISTINCT n.g AS g ORDER BY g LIMIT 1 WHERE g >= 0 RETURN collect(g) AS l":     {int64(0)},
		"MATCH (n:P) WITH n.g AS g, count(*) AS c ORDER BY g LIMIT 1 WHERE c > 1 RETURN collect(g) AS l": {},
		"MATCH (n:P) WITH n ORDER BY n.id WHERE n.id > 1 RETURN collect(n.id) AS l":                     {int64(2), int64(5)},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.ElementsMatch(t, want, result.Rows[0][0], query)
	}
}
