package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// vectorOnlySearch searches for a word no node contains, so only a vector
// match can answer it.
func vectorOnlySearch(t *testing.T, svc *search.Service, vector []float32) *search.SearchResponse {
	t.Helper()
	opts := search.DefaultSearchOptions()
	opts.Limit, opts.MinRRFScore = 10, 0
	response, err := svc.Search(context.Background(), "zulu", vector, opts)
	require.NoError(t, err)
	return response
}

// Once the embed callback returns, which the worker does before it reports
// the node embedded, a ready search service finds the persisted vector:
// publication does not wait for the deferred search-mutation flush.
func TestEmbeddedNodeIsSearchableWhenEmbedCallbackReturns(t *testing.T) {
	ctx := context.Background()
	config := DefaultConfig()
	config.Memory.DecayEnabled, config.Memory.AutoLinksEnabled = false, false
	config.Memory.EmbeddingEnabled = true
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	svc, err := db.EnsureSearchIndexesBuilt(ctx, db.defaultDatabaseName(), nil)
	require.NoError(t, err)
	require.True(t, svc.IsReady())
	_, err = db.storage.CreateNode(&storage.Node{ID: "source", Labels: []string{"SearchSource"}, Properties: map[string]any{"content": "bravo filler"}})
	require.NoError(t, err)
	vector := unitVector(config.Memory.EmbeddingDimensions)
	worker := NewEmbedWorker(&unitVectorEmbedder{dims: config.Memory.EmbeddingDimensions}, db.baseStorage, &EmbedWorkerConfig{NumWorkers: 1, DeferWorkerStart: true, EmbedBatchSize: 1, ChunkSize: 512})
	t.Cleanup(worker.Close)
	type observation struct {
		response *search.SearchResponse
		err      error
	}
	observed := make(chan observation, 1)
	worker.SetOnEmbedded(func(node *storage.Node) {
		opts := search.DefaultSearchOptions()
		opts.Limit, opts.MinRRFScore = 10, 0
		response, err := svc.Search(ctx, "zulu", vector, opts)
		observed <- observation{response, err}
	})
	worker.StartWorkers()
	select {
	case got := <-observed:
		require.NoError(t, got.err)
		require.Len(t, got.response.Results, 1, "reason=%s", got.response.FallbackReason)
		require.Equal(t, storage.NodeID("source"), got.response.Results[0].NodeID)
		require.Equal(t, 1, got.response.Results[0].VectorRank)
		require.Equal(t, 1, svc.EmbeddingCount())
	case <-time.After(10 * time.Second):
		t.Fatal("the public worker did not publish its accepted outcome")
	}
	stored, err := db.storage.GetNode("source")
	require.NoError(t, err)
	require.Len(t, stored.ChunkEmbeddings, 1, "the searchable vector is persisted")
}

// End to end: when WaitForEmbeddings reports the worker's embeddings done, a
// search on the already-ready service finds them by vector.
func TestEmbeddingsAreSearchableWhenWaitForEmbeddingsReturns(t *testing.T) {
	ctx := context.Background()
	const dims = 64
	db := openEmbeddingSearchDB(t, dims)
	svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), nil)
	require.NoError(t, err)

	const sources = 3
	for range sources {
		_, err := db.CreateNode(ctx, []string{"SearchSource"}, map[string]interface{}{"content": "bravo filler"})
		require.NoError(t, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, db.WaitForEmbeddings(waitCtx))

	response := vectorOnlySearch(t, svc, unitVector(dims))
	require.Len(t, response.Results, sources, "reason=%s", response.FallbackReason)
	for _, result := range response.Results {
		require.Positive(t, result.VectorRank)
	}
}
