package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSemanticValidationCachesOnlySuccessfulQueries(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "semantic_cache"))
	validQuery := "RETURN true AS value"

	_, err := exec.Execute(context.Background(), validQuery, nil)
	require.NoError(t, err)
	require.True(t, exec.semanticValidationCache.contains(validQuery))

	invalidQuery := "RETURN true AS value, true AS value"
	_, err = exec.Execute(context.Background(), invalidQuery, nil)
	require.Error(t, err)
	require.False(t, exec.semanticValidationCache.contains(invalidQuery))
}

// Literal-bearing statements cache under their canonical form: repeated
// statements (explicit transactions, batch tooling, the match-create-delete
// gate) and their literal variants hit, and the sharded caches removed the
// contention that once motivated skipping them.
func TestSemanticValidationCacheCachesLiteralStatements(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "semantic_cache"))
	literalQuery := "RETURN 1 AS value"

	_, err := exec.Execute(context.Background(), literalQuery, nil)
	require.NoError(t, err)
	canonical, changed := normalizeLiteralText(literalQuery)
	require.True(t, changed)
	require.True(t, exec.semanticValidationCache.contains(canonical),
		"accepted literal-bearing statements are cached")
	_, err = exec.Execute(context.Background(), "RETURN 2 AS value", nil)
	require.NoError(t, err)
}
