package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Cache publication and delivery own separate result graphs. Verify real
// cache lookups and equivalent values, with independent warm-hit wrappers.
func requireOwnedResultCacheHit(t *testing.T, exec *StorageExecutor, first *ExecuteResult, run func() (*ExecuteResult, error)) *ExecuteResult {
	t.Helper()
	hits, misses, _, _, _ := exec.cache.Stats()
	second, err := run()
	require.NoError(t, err)
	afterHits, afterMisses, _, _, _ := exec.cache.Stats()
	require.Equal(t, hits+1, afterHits, "the repeated public execution must hit the outer result cache")
	require.Equal(t, misses, afterMisses, "a fresh execution must not stand in for a cache hit")
	require.Equal(t, first.Columns, second.Columns)
	require.Equal(t, first.Rows, second.Rows)
	require.NotSame(t, first, second, "publication gives the cache its own result wrapper")
	if len(first.Rows) > 0 {
		require.NotSame(t, &first.Rows[0], &second.Rows[0], "the cache owns its visible outer row window")
	}
	third, err := run()
	require.NoError(t, err)
	warmHits, warmMisses, _, _, _ := exec.cache.Stats()
	require.Equal(t, afterHits+1, warmHits)
	require.Equal(t, afterMisses, warmMisses)
	require.NotSame(t, second, third, "each warm hit delivers its own result")
	require.Equal(t, second.Columns, third.Columns)
	require.Equal(t, second.Rows, third.Rows)
	return second
}
