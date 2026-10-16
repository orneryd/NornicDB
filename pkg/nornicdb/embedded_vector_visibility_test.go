package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// unitVectorEmbedder embeds every text as the same unit vector, so a query
// with that vector finds every embedded node by vector, and by nothing else.
type unitVectorEmbedder struct {
	dims int
}

var _ embed.Embedder = (*unitVectorEmbedder)(nil)

func (e *unitVectorEmbedder) Embed(context.Context, string) ([]float32, error) {
	return unitVector(e.dims), nil
}

func (e *unitVectorEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i], _ = e.Embed(ctx, texts[i])
	}
	return out, nil
}

func (e *unitVectorEmbedder) Dimensions() int { return e.dims }
func (e *unitVectorEmbedder) Model() string   { return "unit-vector-test" }
func (e *unitVectorEmbedder) Backend() string { return "cpu" }

func (e *unitVectorEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

func unitVector(dims int) []float32 {
	vector := make([]float32, dims)
	vector[0] = 1
	return vector
}

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

func openVisibilityTestDB(t *testing.T, embeddingEnabled bool) *DB {
	t.Helper()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = embeddingEnabled
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	db, err := Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Once the embed callback returns, which the worker does before it reports
// the node embedded, a ready search service finds the persisted vector:
// publication does not wait for the deferred search-mutation flush.
func TestEmbeddedNodeIsSearchableWhenEmbedCallbackReturns(t *testing.T) {
	ctx := context.Background()
	db := openVisibilityTestDB(t, false)
	dbName := db.defaultDatabaseName()
	svc, err := db.EnsureSearchIndexesBuilt(ctx, dbName, nil)
	require.NoError(t, err)
	require.True(t, svc.IsReady())

	_, err = db.storage.CreateNode(&storage.Node{ID: "source", Labels: []string{"SearchSource"}, Properties: map[string]interface{}{"content": "bravo filler"}})
	require.NoError(t, err)
	vector := unitVector(db.config.Memory.EmbeddingDimensions)
	stored, err := db.storage.GetNode("source")
	require.NoError(t, err)
	embedded := storage.CopyNode(stored)
	embedded.ChunkEmbeddings = [][]float32{vector}
	// The worker's write: the embedding key space only, with no node event.
	sidecar, ok := db.storage.(storage.EmbeddingSidecarUpdater)
	require.True(t, ok)
	require.NoError(t, sidecar.UpdateNodeEmbeddingSidecar(embedded))
	stored, err = db.storage.GetNode("source")
	require.NoError(t, err)
	require.Len(t, stored.ChunkEmbeddings, 1, "the vector is persisted")

	qualified := storage.CopyNode(embedded)
	qualified.ID = storage.NodeID(dbName + ":source")
	db.onNodeEmbedded(qualified)

	response := vectorOnlySearch(t, svc, vector)
	require.Len(t, response.Results, 1, "reason=%s", response.FallbackReason)
	require.Equal(t, storage.NodeID("source"), response.Results[0].NodeID)
	require.Equal(t, 1, response.Results[0].VectorRank)
	require.Equal(t, 1, svc.EmbeddingCount())
}

// End to end: when WaitForEmbeddings reports the worker's embeddings done, a
// search on the already-ready service finds them by vector.
func TestEmbeddingsAreSearchableWhenWaitForEmbeddingsReturns(t *testing.T) {
	ctx := context.Background()
	db := openVisibilityTestDB(t, true)
	db.embedWorkerConfig.BatchDelay = 0
	dims := db.config.Memory.EmbeddingDimensions
	db.SetEmbedder(&unitVectorEmbedder{dims: dims})
	svc, err := db.EnsureSearchIndexesBuilt(ctx, db.defaultDatabaseName(), nil)
	require.NoError(t, err)
	require.True(t, svc.IsReady())

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
