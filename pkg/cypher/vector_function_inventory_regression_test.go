package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These declarations are pinned to Neo4j 5.26.30, which has no VECTOR type.
// Keep this regression independent of the optional live-reference inventory test.
func TestShowVectorSimilaritySignaturesMatchNeo4j526(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	result, err := executor.Execute(ctx, "SHOW FUNCTIONS YIELD name, signature, argumentDescription WHERE name IN ['vector.similarity.cosine', 'vector.similarity.euclidean'] RETURN name, signature, argumentDescription ORDER BY name", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	for i, name := range []string{"vector.similarity.cosine", "vector.similarity.euclidean"} {
		row := result.Rows[i]
		require.Equal(t, name, row[0])
		require.Equal(t, name+"(a :: LIST<INTEGER | FLOAT>, b :: LIST<INTEGER | FLOAT>) :: FLOAT", row[1])
		arguments, ok := row[2].([]interface{})
		require.True(t, ok, "%T", row[2])
		require.Len(t, arguments, 2)
		for _, argument := range arguments {
			require.Equal(t, "LIST<INTEGER | FLOAT>", argument.(map[string]interface{})["type"])
		}
	}
}
