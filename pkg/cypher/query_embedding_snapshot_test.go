package cypher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type statementSnapshotEmbedder struct {
	vector  []float32
	calls   atomic.Int64
	onChunk func()
	once    sync.Once
}

func (e *statementSnapshotEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls.Add(1)
	return append([]float32(nil), e.vector...), nil
}

func (e *statementSnapshotEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	if e.onChunk != nil {
		e.once.Do(e.onChunk)
	}
	return []string{text}, nil
}

func (*statementSnapshotEmbedder) Dimensions() int { return 2 }

// Publish a replacement between the first and second embedding in one public
// statement. An overlap test with only one embedding cannot detect a reload.
func TestQueryEmbeddingStatementKeepsProviderAcrossCalls(t *testing.T) {
	for _, fabricQuery := range []bool{false, true} {
		name := "plain"
		if fabricQuery {
			name = "fabric"
		}
		t.Run(name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "snapshot"))
			query := "CALL db.index.vector.embed('alpha') YIELD embedding AS first CALL db.index.vector.embed('beta') YIELD embedding AS second RETURN first, second"
			if fabricQuery {
				exec, _ = compositeTestFixture(t)
				query = "CALL { USE comp.a " + query + " } RETURN first, second"
			}
			replacement := &statementSnapshotEmbedder{vector: []float32{0, 1}}
			old := &statementSnapshotEmbedder{vector: []float32{1, 0}}
			old.onChunk = func() { exec.SetEmbedder(replacement) }
			exec.SetEmbedder(old)
			result, err := exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			for _, cell := range result.Rows[0] {
				require.Equal(t, old.vector, toFloat32Slice(cell))
			}
			require.Equal(t, int64(2), old.calls.Load())
			require.Zero(t, replacement.calls.Load())
			result, err = exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			for _, cell := range result.Rows[0] {
				require.Equal(t, replacement.vector, toFloat32Slice(cell))
			}
			require.Equal(t, int64(2), replacement.calls.Load())
			hits, _, _, _, _ := exec.cache.Stats()
			_, err = exec.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			afterHits, _, _, _, _ := exec.cache.Stats()
			require.Greater(t, afterHits, hits, "verify that this query actually exercises the outer cache")
			require.Equal(t, int64(2), replacement.calls.Load())
		})
	}
}

type statementSnapshotVersionEngine struct {
	storage.Engine
	hook func()
}

func (e *statementSnapshotVersionEngine) GraphMutationVersion() (uint64, bool) {
	if e.hook != nil {
		fn := e.hook
		e.hook = nil
		fn()
	}
	return 0, true
}

func TestQueryEmbeddingNilSnapshotFinishesAfterReadiness(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "snapshot")
	_, err := store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	svc := search.NewService(store, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	engine := &statementSnapshotVersionEngine{Engine: store}
	exec := NewStorageExecutor(engine)
	exec.SetSearchService(svc)
	ready := &statementSnapshotEmbedder{vector: []float32{1, 0}}
	engine.hook = func() { exec.SetEmbedder(ready) }
	const query = "CALL db.rretrieve({query: 'alpha', limit: 5}) YIELD fallback_reason RETURN fallback_reason"
	before, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	// The statement captured nil before the graph-version hook published readiness.
	// It may complete using nil, but its late cache insertion must remain generation 0.
	require.Equal(t, "no_embedder", before.Rows[0][0])
	require.Same(t, ready, exec.GetEmbedder())
	require.Zero(t, ready.calls.Load())
	after, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, "", after.Rows[0][0])
	require.Positive(t, ready.calls.Load())
	calls := ready.calls.Load()
	hits, _, _, _, _ := exec.cache.Stats()
	_, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	afterHits, _, _, _, _ := exec.cache.Stats()
	require.Greater(t, afterHits, hits)
	require.Equal(t, calls, ready.calls.Load())
}

func TestQueryEmbeddingNilAndSameProviderAreNewPublications(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "snapshot"))
	provider := &statementSnapshotEmbedder{vector: []float32{1, 0}}
	const query = "CALL db.index.vector.embed('alpha') YIELD embedding RETURN embedding"
	exec.SetEmbedder(provider)
	firstGeneration := exec.currentQueryEmbedder().generation
	_, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), provider.calls.Load())
	exec.SetEmbedder(nil)
	require.NotEqual(t, firstGeneration, exec.currentQueryEmbedder().generation)
	_, err = exec.Execute(context.Background(), query, nil)
	require.Error(t, err, "nil publication must not return the old cached embedding")
	exec.SetEmbedder(provider)
	require.NotEqual(t, firstGeneration, exec.currentQueryEmbedder().generation)
	_, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), provider.calls.Load())
	exec.SetEmbedder(provider)
	_, err = exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, int64(3), provider.calls.Load(), "even republishing the same pointer must replace cache identity")
}
