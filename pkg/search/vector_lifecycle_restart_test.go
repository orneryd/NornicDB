package search

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// settableWriteTimeEngine reports a settable storage write time, which decides whether
// a rebuild may resume the saved vector store or must replace it.
type settableWriteTimeEngine struct {
	*storage.MemoryEngine
	last atomic.Int64
}

func (e *settableWriteTimeEngine) LastWriteTime() time.Time {
	if n := e.last.Load(); n != 0 {
		return time.Unix(0, n)
	}
	return time.Time{}
}

// F-I86 B2: a rebuild that finds storage newer than the saved vector store
// replaces that store. The replacement is a source transition: the closed
// store must not stay published to queries for the rest of the rebuild.
func TestBaseRestartRetiresReplacedVectorStore(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100") // keep the exact pipeline
	ctx := context.Background()
	engine := &settableWriteTimeEngine{MemoryEngine: storage.NewMemoryEngine()}
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	for i := 0; i < 3; i++ {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i)), ChunkEmbeddings: [][]float32{{1, float32(i), 0}}})
		require.NoError(t, err)
	}
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	// Only a vector path: the rebuild iterates storage instead of restoring.
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	require.NoError(t, svc.BuildIndexes(ctx))

	query, opts := []float32{1, 0, 0}, &SearchOptions{Limit: 10}
	_, err := svc.VectorSearchCandidates(ctx, query, opts)
	require.NoError(t, err)
	require.NotNil(t, cachedVectorPipeline(svc), "a query caches the exact pipeline over the saved store")

	engine.last.Store(time.Now().Add(time.Hour).UnixNano())
	reached := false
	var cachedAtRestart *VectorSearchPipeline
	var queryErr error
	baseSourceRestartHook = func() {
		reached = true
		cachedAtRestart = cachedVectorPipeline(svc)
		_, queryErr = svc.VectorSearchCandidates(ctx, query, opts)
	}
	t.Cleanup(func() { baseSourceRestartHook = nil })
	require.NoError(t, svc.BuildIndexes(ctx))
	require.True(t, reached, "the rebuild did not replace the saved store")
	require.Nil(t, cachedAtRestart, "the replaced store is still published to queries")
	require.NoError(t, queryErr, "a query during the rebuild read the closed store")

	results, err := svc.VectorSearchCandidates(ctx, query, opts)
	require.NoError(t, err)
	require.Len(t, results, 3)
}

