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

// Literal-heavy statements never repeat a text, so they skip the shared
// cache instead of contending on it (PR #1043 write-path profile).
func TestSemanticValidationCacheSkipsLiteralStatements(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "semantic_cache"))
	literalQuery := "RETURN 1 AS value"

	_, err := exec.Execute(context.Background(), literalQuery, nil)
	require.NoError(t, err)
	require.False(t, exec.semanticValidationCache.contains(literalQuery),
		"literal-bearing statements must not pollute the semantic cache")
}
