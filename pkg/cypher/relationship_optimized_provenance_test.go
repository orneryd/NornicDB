package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRelationshipOptimizedPublicProvenance(t *testing.T) {
	ctx := context.Background()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := e.Execute(ctx, "CREATE (a {name:'a'})-[:B]->(b {name:'b'})-[:B]->(a)", nil)
	require.NoError(t, err)
	query := "MATCH (a)-[:A|B]->(b)-[:A|B]->(a) RETURN a.name,b.name"
	info := DetectQueryPattern(ctx, query)
	result, applied := e.ExecuteOptimized(ctx, query, info)
	if !applied {
		result, err = e.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	require.Len(t, result.Rows, 2, "the public optimized API discarded B alternatives")
	for _, source := range []string{
		"MATCH (a)-[:$(type)]->(b)-[:$(type)]->(a) RETURN a.name,b.name",
		"MATCH (a)-[:B {p:1}]->(b)-[:B]->(a) RETURN a.name,b.name",
	} {
		_, applied = e.ExecuteOptimized(ctx, source, PatternInfo{Pattern: PatternMutualRelationship, RelType: "B"})
		require.False(t, applied, source)
	}
	_, err = e.Execute(ctx, "CREATE (a {name:'literal a'})-[:`A|B`]->(b {name:'literal b'})-[:`A|B`]->(a)", nil)
	require.NoError(t, err)
	query = "MATCH (a)-[:`A|B`]->(b)-[:`A|B`]->(a) RETURN a.name,b.name"
	result, applied = e.ExecuteOptimized(ctx, query, DetectQueryPattern(ctx, query))
	require.True(t, applied)
	require.Len(t, result.Rows, 1)
	require.Contains(t, result.Rows[0][0], "literal")
}
