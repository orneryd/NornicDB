package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An empty alias (AS “) names the column "", on every route, as Neo4j
// 5.26.30 does (#907); other columns keep their names.
func TestEmptyAliasNamesTheColumnEmpty(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "empty_column"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:EC {id: 1, s: 'ab'})-[:R]->(:EC {id: 2})", nil)
	require.NoError(t, err)
	for query, columns := range map[string][]string{
		"MATCH (n:EC {id: 1}) RETURN n.id AS `a b`, n.s AS ``": {"a b", ""},
		"RETURN 1 AS ``":                                                {""},
		"MATCH (n:EC) RETURN count(n) AS ``":                            {""},
		"MATCH (n:EC)-[:R]->(m) RETURN m.id AS ``, n.id":                {"", "n.id"},
		"UNWIND [1] AS x WITH x AS `` RETURN `` AS v":                   {"v"},
		"MATCH (n:EC {id: 1}) RETURN n.s AS ``, n.id AS id ORDER BY id": {"", "id"},
		"MATCH (n:EC {id: 1}) RETURN n.id, n.s":                         {"n.id", "n.s"},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, columns, result.Columns, query)
	}
}
