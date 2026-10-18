package nornicdb

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
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

// openEmbeddingSearchDB opens a database whose embed worker starts on a write
// without delay, with its search indexes built and ready.
func openEmbeddingSearchDB(tb testing.TB, dims int) *DB {
	tb.Helper()
	config := DefaultConfig()
	config.Memory.EmbeddingEnabled = true
	config.Memory.EmbeddingDimensions = dims
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.EmbeddingWorker.BatchDelay = 0
	config.EmbeddingWorker.TriggerDebounceDelay = 0
	db, err := Open(tb.TempDir(), config)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })
	db.SetEmbedder(&unitVectorEmbedder{dims: dims})
	svc, err := db.EnsureSearchIndexesBuilt(context.Background(), db.defaultDatabaseName(), nil)
	require.NoError(tb, err)
	require.True(tb, svc.IsReady())
	return db
}

// BenchmarkEmbeddedNodesSearchable measures bulk ingestion through the embed
// worker into a ready search service: from creating the nodes until the
// worker reports them embedded (embedded_ms), and until every one is in the
// service's vector source (searchable_ms).
func BenchmarkEmbeddedNodesSearchable(b *testing.B) {
	const nodes = 256
	ctx := context.Background()
	var embeddedTotal, searchableTotal time.Duration
	for range b.N {
		b.StopTimer()
		db := openEmbeddingSearchDB(b, 64)
		svc, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), nil)
		require.NoError(b, err)
		b.StartTimer()
		start := time.Now()
		for range nodes {
			_, err := db.CreateNode(ctx, []string{"Doc"}, map[string]interface{}{"content": "bulk ingestion text"})
			require.NoError(b, err)
		}
		waitCtx, cancel := context.WithTimeout(ctx, time.Minute)
		require.NoError(b, db.WaitForEmbeddings(waitCtx))
		cancel()
		embeddedTotal += time.Since(start)
		deadline := time.Now().Add(time.Minute)
		for svc.EmbeddingCount() < nodes {
			require.True(b, time.Now().Before(deadline), "embedded nodes never became searchable")
			time.Sleep(time.Millisecond)
		}
		searchableTotal += time.Since(start)
		b.StopTimer()
		require.NoError(b, db.Close())
	}
	b.ReportMetric(float64(embeddedTotal.Milliseconds())/float64(b.N), "embedded_ms")
	b.ReportMetric(float64(searchableTotal.Milliseconds())/float64(b.N), "searchable_ms")
}
