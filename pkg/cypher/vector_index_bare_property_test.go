package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// CREATE VECTOR INDEX … ON n.e, the property without parentheses, as Neo4j
// accepts it, with both parsers (#907): the ANTLR grammar took only ON (n.e).
func TestCreateVectorIndexOnBareProperty(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_index_bare"))
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE VECTOR INDEX movieEmb IF NOT EXISTS FOR (m:Movie) ON m.emb OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
		"CREATE VECTOR INDEX likesEmb IF NOT EXISTS FOR ()-[r:LIKES]-() ON r.emb OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	result, err := exec.Execute(ctx, "SHOW VECTOR INDEXES YIELD name, entityType, labelsOrTypes, properties RETURN name, entityType, labelsOrTypes, properties ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"likesEmb", "RELATIONSHIP", []string{"LIKES"}, []string{"emb"}},
		{"movieEmb", "NODE", []string{"Movie"}, []string{"emb"}},
	}, result.Rows)
}
