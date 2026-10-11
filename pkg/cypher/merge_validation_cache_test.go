package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A MERGE statement that passed validation is answered from the cache the
// next time.
func TestMergeSemanticValidationCachesAcceptedStatement(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	query := "MERGE (n:Cached) RETURN n"
	require.NoError(t, exec.validateMergeSemanticScopes(query))
	require.True(t, exec.mergeSemanticValidationCache.contains(query))
	require.NoError(t, exec.validateMergeSemanticScopes(query))
}

// Literal-bearing MERGE statements skip the shared cache (they never repeat
// a text), per the PR #1043 write-path cache-contention fix.
func TestMergeSemanticValidationSkipsLiteralStatements(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	query := "MERGE (n:Cached {id: 1}) RETURN n"
	require.NoError(t, exec.validateMergeSemanticScopes(query))
	require.False(t, exec.mergeSemanticValidationCache.contains(query))
}
