package nornicdb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// openEmbeddingClearDB opens a journaled database with an embedding-free worker
// and its search service built and ready. Refreshes stay queued (a one-hour
// debounce) until the test applies them, so every publication in these tests is
// applied by the test rather than by the timer.
func openEmbeddingClearDB(t *testing.T) (*DB, *search.Service, *dbSearchService) {
	t.Helper()
	config := DefaultConfig()
	config.Database.AsyncWritesEnabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Memory.EmbeddingDimensions = embeddingClearDimensions
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	svc, err := db.EnsureSearchIndexes(t.Context(), db.defaultDatabaseName(), nil, SearchIndexBuildWait)
	require.NoError(t, err)
	db.searchServicesMu.RLock()
	entry := db.searchServices[db.defaultDatabaseName()]
	db.searchServicesMu.RUnlock()
	require.NotNil(t, entry)
	entry.pendingFlushDelay = time.Hour
	return db, svc, entry
}

const embeddingClearDimensions = 16

func embeddingClearVector(component int) []float32 {
	vector := make([]float32, embeddingClearDimensions)
	vector[component] = 1
	return vector
}

// embeddingClearHits is the number of nodes vector search returns for vector.
func embeddingClearHits(t *testing.T, svc *search.Service, vector []float32) int {
	t.Helper()
	options := search.DefaultSearchOptions()
	options.Limit = 10
	minimum := 0.9
	options.MinSimilarity = &minimum
	response, err := svc.Search(t.Context(), "", vector, options)
	require.NoError(t, err)
	return len(response.Results)
}

// The reset clears managed embeddings only. A vector a node carries in a
// property is not managed state: storage keeps it, so the serving index must
// keep it too. A wholesale index clear after the reset would delete it, because
// that node's storage is never touched and so publishes no refresh to put it
// back. Report 47 (group B16).
func TestEmbeddingClearKeepsVectorsStorageStillHolds(t *testing.T) {
	ctx := context.Background()
	db, svc, entry := openEmbeddingClearDB(t)
	store := db.GetStorage()
	managed, property := embeddingClearVector(0), embeddingClearVector(1)

	_, err := store.CreateNode(ctx, &storage.Node{
		ID: "managed", Labels: []string{"Doc"},
		Properties:      map[string]any{"text": "t"},
		ChunkEmbeddings: [][]float32{managed},
		EmbedMeta:       map[string]any{"has_embedding": true},
	})
	require.NoError(t, err)
	_, err = store.CreateNode(ctx, &storage.Node{
		ID: "property", Labels: []string{"Doc"},
		Properties: map[string]any{"text": "u", "emb": property},
	})
	require.NoError(t, err)
	applyQueued(t, entry)
	require.Equal(t, 2, svc.EmbeddingCount(), "both stored vectors are served")
	require.Equal(t, 1, embeddingClearHits(t, svc, property))

	count, err := db.ClearAllEmbeddings()
	require.NoError(t, err)
	require.Equal(t, 1, count, "only the node with managed embeddings is reset")
	applyQueued(t, entry)

	storedProperty, err := store.GetNode(ctx, "property")
	require.NoError(t, err)
	require.NotNil(t, storedProperty.Properties["emb"], "the reset leaves a property vector in storage")
	storedManaged, err := store.GetNode(ctx, "managed")
	require.NoError(t, err)
	require.Empty(t, storedManaged.ChunkEmbeddings)

	require.Equal(t, 1, svc.EmbeddingCount(), "the reset publishes exactly the node it cleared")
	require.Equal(t, 1, embeddingClearHits(t, svc, property), "a vector storage still holds stays served")
	require.Zero(t, embeddingClearHits(t, svc, managed), "the cleared vector is no longer served")
}

// The reset is per node and not atomic: when the journal refuses the second
// node, the first node's reset has committed and its own storage event
// publishes it, while the refused node keeps its vector served because storage
// still holds it.
func TestPartialEmbeddingClearPublishesOnlyTheClearedNode(t *testing.T) {
	ctx := context.Background()
	db, svc, entry := openEmbeddingClearDB(t)
	store := db.GetStorage()
	vectors := map[string][]float32{"a": embeddingClearVector(0), "b": embeddingClearVector(1)}
	for _, id := range []string{"a", "b"} {
		_, err := store.CreateNode(ctx, &storage.Node{
			ID: storage.NodeID(id), Labels: []string{"Doc"},
			Properties:      map[string]any{"text": id},
			ChunkEmbeddings: [][]float32{vectors[id]},
			EmbedMeta:       map[string]any{"has_embedding": true},
		})
		require.NoError(t, err)
	}
	applyQueued(t, entry)
	require.Equal(t, 2, svc.EmbeddingCount())

	// Node a sorts before node b, so the reset clears a and then reaches b,
	// whose journal append fails because the test closed the journal once the
	// first reset committed. The callback runs on the commit's delivery
	// goroutine, so it records its outcome instead of asserting.
	var once sync.Once
	var closeErr error
	require.NoError(t, storage.SubscribeEvents(store, storage.EventHandlers{NodeUpdated: func(*storage.Node) {
		once.Do(func() {
			journaled, ok := db.GetBaseStorageForManager().(*storage.WALEngine)
			if !ok {
				closeErr = errors.New("the default store is not journaled")
				return
			}
			closeErr = journaled.GetWAL().Close()
		})
	}}))
	count, err := db.ClearAllEmbeddings()
	require.NoError(t, closeErr, "the test closed the journal")
	require.Error(t, err, "the refused node reports the journal's refusal")
	require.Equal(t, 1, count, "the returned count is what the reset actually cleared")
	require.ErrorContains(t, err, "after clearing 1 of 2 nodes")
	applyQueued(t, entry)

	storedA, err := store.GetNode(ctx, "a")
	require.NoError(t, err)
	require.Empty(t, storedA.ChunkEmbeddings)
	storedB, err := store.GetNode(ctx, "b")
	require.NoError(t, err)
	require.Len(t, storedB.ChunkEmbeddings, 1, "the refused node keeps its vector in storage")

	require.Equal(t, 1, svc.EmbeddingCount(), "the reset publishes exactly the node it cleared")
	require.Zero(t, embeddingClearHits(t, svc, vectors["a"]))
	require.Equal(t, 1, embeddingClearHits(t, svc, vectors["b"]), "the refused node stays served")
}

// A vector the embedding worker regenerates after the reset's storage commit is
// served from the node's own storage commit (indexNodeFromEvent ->
// publishEmbeddedNode), and the reset's queued publication reads storage when
// it applies, so it keeps the regenerated vector instead of removing it.
func TestRegeneratedEmbeddingIsServedAfterTheReset(t *testing.T) {
	ctx := context.Background()
	db, svc, entry := openEmbeddingClearDB(t)
	store := db.GetStorage()
	cleared, regenerated := embeddingClearVector(0), embeddingClearVector(1)

	_, err := store.CreateNode(ctx, &storage.Node{
		ID: "v", Labels: []string{"Doc"},
		Properties:      map[string]any{"text": "t"},
		ChunkEmbeddings: [][]float32{cleared},
		EmbedMeta:       map[string]any{"has_embedding": true},
	})
	require.NoError(t, err)
	applyQueued(t, entry)
	require.Equal(t, 1, embeddingClearHits(t, svc, cleared))

	count, err := db.ClearAllEmbeddings()
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// The worker regenerates the cleared node and its accepted vector is
	// published through the node's storage commit, before the reset's queued
	// refresh applies.
	stored, err := store.GetNode(ctx, "v")
	require.NoError(t, err)
	stored.ChunkEmbeddings = [][]float32{regenerated}
	stored.EmbedMeta = map[string]any{"has_embedding": true}
	require.NoError(t, store.UpdateNode(ctx, storage.NodeUpdateRequest{After: stored}))
	embedded := storage.CopyNode(stored)
	embedded.ID = storage.NodeID(db.defaultDatabaseName() + ":" + string(stored.ID))
	require.True(t, db.publishEmbeddedNode(ctx, embedded), "the ready service serves the worker's vector at once")
	require.Equal(t, 1, embeddingClearHits(t, svc, regenerated), "the regenerated vector is served")

	// The queued reset publication applies after the regeneration and reads
	// storage, so it cannot remove the regenerated vector.
	applyQueued(t, entry)
	require.Equal(t, 1, svc.EmbeddingCount())
	require.Equal(t, 1, embeddingClearHits(t, svc, regenerated), "a reset publication must not delete a newer vector")
	require.Zero(t, embeddingClearHits(t, svc, cleared))
}
