package nornicdb

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClearDatabaseEmbeddingsPreservesOtherNamespaces(t *testing.T) {
	config := DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	defer db.Close()
	first := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "scope_first")
	second := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "scope_second")
	for _, engine := range []storage.Engine{first, second} {
		_, err = engine.CreateNode(t.Context(), &storage.Node{ID: "shared", Labels: []string{"Doc"}, Properties: map[string]any{"content": "same body"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}, NamedEmbeddings: map[string][]float32{"user": {4, 5, 6}}})
		require.NoError(t, err)
	}
	count, err := db.ClearDatabaseEmbeddings("scope_first", first)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	one, err := first.GetNode(t.Context(), "shared")
	require.NoError(t, err)
	two, err := second.GetNode(t.Context(), "shared")
	require.NoError(t, err)
	require.Empty(t, one.ChunkEmbeddings)
	require.Equal(t, [][]float32{{1, 2, 3}}, two.ChunkEmbeddings)
	require.Equal(t, []float32{4, 5, 6}, one.NamedEmbeddings["user"])
	_, err = db.ClearDatabaseEmbeddings("scope_second", first)
	require.Error(t, err)
	two, err = second.GetNode(t.Context(), "shared")
	require.NoError(t, err)
	require.NotEmpty(t, two.ChunkEmbeddings)
}

func TestDatabaseEmbeddingRegenerationKeepsOtherWorkerState(t *testing.T) {
	queue := &EmbedWorker{recentlyProcessed: map[string]recentNodeEmbedding{"scope:one": {}, "scope_other:two": {}}, loggedSkip: map[string]bool{"scope:one": true, "scope_other:two": true}, nodeUpdates: map[string]struct{}{"scope_other:pending": {}}}
	queue.processed.Store(42)
	queue.forgetDatabaseRecentEmbeddings("scope")
	require.NotContains(t, queue.recentlyProcessed, "scope:one")
	require.NotContains(t, queue.loggedSkip, "scope:one")
	require.Contains(t, queue.recentlyProcessed, "scope_other:two")
	require.Contains(t, queue.loggedSkip, "scope_other:two")
	require.Contains(t, queue.nodeUpdates, "scope_other:pending")
	require.Equal(t, int64(42), queue.processed.Load())
}
