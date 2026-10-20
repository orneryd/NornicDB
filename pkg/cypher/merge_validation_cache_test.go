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
	require.NoError(t, exec.validateMergeSemanticScopes(query, false))
	require.True(t, exec.semanticValidationCache.contains(query))
	require.NoError(t, exec.validateMergeSemanticScopes(query, false))
}

// Literal-bearing MERGE statements cache under their canonical form:
// repeated statements (explicit transactions, batch tooling, the
// match-create-delete gate) and their literal variants hit, and sharding
// removed the contention that once motivated skipping them.
func TestMergeSemanticValidationCachesLiteralStatements(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	query := "MERGE (n:Cached {id: 1}) RETURN n"
	require.NoError(t, exec.validateMergeSemanticScopes(query, false))
	canonical, changed := normalizeLiteralText(query)
	require.True(t, changed)
	require.True(t, exec.semanticValidationCache.contains(canonical))
	require.NoError(t, exec.validateMergeSemanticScopes("MERGE (n:Cached {id: 2}) RETURN n", false))
}
