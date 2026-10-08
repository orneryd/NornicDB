package search

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// shortApproximateCandidateGenerator is an approximate generator that never
// reaches more than its candidates and never reports itself exhausted: an
// HNSW graph some of whose nodes the search can't reach from the query.
type shortApproximateCandidateGenerator struct {
	candidates []Candidate
	population int
	calls      int
}

func (g *shortApproximateCandidateGenerator) SearchCandidates(ctx context.Context, query []float32, limit int, minSimilarity float64) ([]Candidate, error) {
	candidates, _, err := g.searchCandidatesWithExhaustion(ctx, query, limit, minSimilarity)
	return candidates, err
}

func (g *shortApproximateCandidateGenerator) searchCandidatesWithExhaustion(_ context.Context, _ []float32, limit int, _ float64) ([]Candidate, bool, error) {
	g.calls++
	return g.candidates[:min(limit, len(g.candidates))], false, nil
}

func (g *shortApproximateCandidateGenerator) candidatePopulation() int { return g.population }

// sparseWordEmbedding is the reporter's embedding: one dimension per word
// (md5 of the word modulo dimensions), normalized. Texts that share words
// are at exactly the same similarity to each other.
func sparseWordEmbedding(text string, dimensions int) []float32 {
	embedding := make([]float32, dimensions)
	for _, word := range strings.Fields(strings.ToLower(text)) {
		sum := md5.Sum([]byte(word))
		embedding[binary.BigEndian.Uint64(sum[8:])%uint64(dimensions)]++
	}
	var norm float64
	for _, value := range embedding {
		norm += float64(value) * float64(value)
	}
	norm = math.Sqrt(norm)
	for index := range embedding {
		embedding[index] = float32(float64(embedding[index]) / norm)
	}
	return embedding
}

// db.index.vector.queryNodes with k above what the approximate search (#974)
// reaches returns: the exact completion fills the page instead of the
// widening running on without end.
func TestVectorQueryNodesReturnsWhenApproximateSearchReachesFewerThanK(t *testing.T) {
	const dimensions = 2048
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), dimensions)
	config := DefaultHNSWConfig()
	config.EfConstruction, config.EfSearch, config.UseGPUBuild = 100, 50, false
	index := NewHNSWIndex(dimensions, config)
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("src%d", i)
		embedding := sparseWordEmbedding(fmt.Sprintf("text: source document %d word%d", i, i), dimensions)
		require.NoError(t, service.IndexNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"PDSearchSource"}, ChunkEmbeddings: [][]float32{embedding}}))
		require.NoError(t, index.Add(id, embedding))
	}
	service.pipelineMu.Lock()
	service.vectorPipeline = NewVectorSearchPipeline(NewHNSWCandidateGen(index), NewCPUExactScorer(service.vectorIndex))
	service.pipelineMu.Unlock()

	query := sparseWordEmbedding("text: source document 0 word0", dimensions)
	for _, k := range []int{10, 25, 45, 50, 100} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		hits, err := service.vectorQueryNodesIndexedWithOptions(ctx, query, VectorQuerySpec{Label: "PDSearchSource", Similarity: "cosine", Limit: k}, "default", nil)
		cancel()
		require.NoError(t, err, "k=%d", k)
		require.Len(t, hits, min(k, 50), "k=%d", k)
		require.Equal(t, "src0", hits[0].ID, "k=%d", k)
	}
}

func TestAdaptiveVectorSearchCompletesOnceRequestCoversPopulation(t *testing.T) {
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	for index, embedding := range [][]float32{{1, 0}, {0.9, 0.1}, {0.8, 0.2}} {
		require.NoError(t, service.IndexNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", index+1)), Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{embedding}}))
	}
	generator := &shortApproximateCandidateGenerator{
		candidates: []Candidate{{ID: "doc-1-chunk-0", Score: 1}, {ID: "doc-2-chunk-0", Score: 0.99}},
		population: 3,
	}
	service.pipelineMu.Lock()
	service.vectorPipeline = NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	service.pipelineMu.Unlock()

	hits, err := service.vectorQueryNodesIndexedWithOptions(context.Background(), []float32{1, 0}, VectorQuerySpec{Label: "Doc", Similarity: "cosine", Limit: 3}, "default", nil)

	require.NoError(t, err)
	require.Equal(t, []string{"doc-1", "doc-2", "doc-3"}, vectorQueryHitIDs(hits))
	// One request covers the three vectors; a wider one couldn't reach more.
	require.Equal(t, 1, generator.calls)
}

func TestAdaptiveVectorSearchStopsWhenContextEnds(t *testing.T) {
	service := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	generator := &shortApproximateCandidateGenerator{candidates: []Candidate{{ID: "doc-1", Score: 1}}, population: 1 << 40}
	pipeline := NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	opts := adaptiveOverfetchTestOptions(3)
	opts.MaxCandidateLimit = 0
	opts.MaxOverfetchRatio = math.MaxFloat64
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := service.adaptiveVectorSearch(ctx, pipeline, []float32{1, 0}, opts, nil, nil)

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, generator.calls)
}

func TestNextCandidateLimitSaturatesAtCeiling(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	require.Equal(t, 4, nextCandidateLimit(2, 2, 100))
	require.Equal(t, 100, nextCandidateLimit(60, 2, 100))
	require.Equal(t, 100, nextCandidateLimit(100, 2, 100))
	require.Equal(t, 3, nextCandidateLimit(2, 1.01, 100))
	// Doubling past the largest int saturates instead of overflowing.
	require.Equal(t, maxInt, nextCandidateLimit(1<<62, 2, maxInt))
	require.Equal(t, maxInt, nextCandidateLimit(maxInt-1, 2, maxInt))
}
