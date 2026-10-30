package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOrdinaryAutoCommitResultCacheStillHits(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	const read = "MATCH (n:OrdinaryCacheControl) RETURN count(n) AS c"
	_, err := exec.Execute(context.Background(), read, nil)
	require.NoError(t, err)
	before, _, _, _, _ := exec.cache.Stats()
	_, err = exec.Execute(context.Background(), read, nil)
	require.NoError(t, err)
	after, _, _, _, _ := exec.cache.Stats()
	require.Greater(t, after, before, "ordinary auto-commit reads must still hit the cache")
}
