package cypher

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// accessCountingEngine records the entity ids a result materializes.
type accessCountingEngine struct {
	*storage.MemoryEngine
	mu       sync.Mutex
	accessed []string
}

func (e *accessCountingEngine) RecordMaterializedAccess(entityID string) {
	e.mu.Lock()
	e.accessed = append(e.accessed, entityID)
	e.mu.Unlock()
}

func (e *accessCountingEngine) take() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	accessed := e.accessed
	e.accessed = nil
	sort.Strings(accessed)
	return accessed
}

// TestMaterializedAccessRecordedOnCacheHits: a statement answered from the
// result cache records an access for every node and relationship it returns,
// as the statement that filled the cache did; a result without any records
// nothing.
func TestMaterializedAccessRecordedOnCacheHits(t *testing.T) {
	inner := &accessCountingEngine{MemoryEngine: storage.NewMemoryEngine()}
	t.Cleanup(func() { _ = inner.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(inner, "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:AccessT {v: 1})-[:ACCESS_R]->(:AccessT {v: 2})", nil)
	require.NoError(t, err)
	inner.take()

	for _, query := range []string{
		"MATCH (n:AccessT) RETURN n ORDER BY n.v",
		"MATCH (a:AccessT)-[r:ACCESS_R]->(b:AccessT) RETURN [a, r, b] AS path",
		"MATCH (n:AccessT) RETURN {node: n} AS m ORDER BY n.v",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		first := inner.take()
		require.NotEmpty(t, first, query)

		hits, _, _, _, _ := exec.cache.Stats()
		_, err = exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		hitsAfter, _, _, _, _ := exec.cache.Stats()
		require.Equal(t, hits+1, hitsAfter, "%s: the second run is a cache hit", query)
		require.Equal(t, first, inner.take(), "%s: the cache hit records the same accesses", query)
	}

	const scalars = "MATCH (n:AccessT) RETURN n.v AS v ORDER BY v"
	for i := 0; i < 2; i++ {
		result, err := exec.Execute(ctx, scalars, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
		require.Empty(t, inner.take())
	}
}
