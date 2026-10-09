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

// Every route names its columns from the items: the traversal wildcard, the
// vector fast path's empty result, and a CALL tail's WITH.
func TestColumnNamesOnEveryRoute(t *testing.T) {
	expanded := expandTraversalWildcardReturnItems([]returnItem{{expr: "*", alias: "*"}}, &TraversalMatch{
		StartNode:    nodePatternInfo{variable: "a"},
		EndNode:      nodePatternInfo{variable: "b"},
		Relationship: RelationshipPattern{Variable: "r"},
	}, "p")
	require.Equal(t, []returnItem{{expr: "a", alias: "a"}, {expr: "b", alias: "b"}, {expr: "p", alias: "p"}, {expr: "r", alias: "r"}}, expanded)

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "empty_column_vector"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX ec_emb FOR (n:ECV) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:ECV {uuid: 'a', emb: [1.0, 0.0, 0.0]})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (n:ECV) RETURN n.uuid AS ``, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT 0",
		map[string]interface{}{"q": []float64{1, 0, 0}})
	require.NoError(t, err)
	require.Equal(t, []string{"", "score"}, result.Columns)
	require.Empty(t, result.Rows)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)

	_, ok := callTailWithProjectionColumns("WITH a,,b")
	require.False(t, ok)
}
