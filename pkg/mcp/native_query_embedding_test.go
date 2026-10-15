package mcp

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/nornicgrpc"
	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

type nativeSnapshotEmbedder struct {
	readinessEmbedder
	vector  []float32
	onEmbed func()
	once    sync.Once
}

func (e *nativeSnapshotEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls.Add(1)
	if e.onEmbed != nil {
		e.once.Do(e.onEmbed)
	}
	return append([]float32(nil), e.vector...), nil
}

func (*nativeSnapshotEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	if text == "alpha beta" {
		return []string{"alpha", "beta"}, nil
	}
	return []string{text}, nil
}

func (e *nativeSnapshotEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i, text := range texts {
		result[i], _ = e.Embed(ctx, text)
	}
	return result, nil
}

type nativeSnapshotSearcher struct{ vectors [][]float32 }

func (s *nativeSnapshotSearcher) Search(_ context.Context, _ string, vector []float32, _ *search.SearchOptions) (*search.SearchResponse, error) {
	s.vectors = append(s.vectors, append([]float32(nil), vector...))
	return &search.SearchResponse{SearchMethod: "rrf_hybrid", Results: []search.SearchResult{{ID: "doc", Score: 1, Similarity: 1, VectorRank: 1}}}, nil
}

func nativeSnapshotService(t *testing.T, db *nornicdb.DB, searcher *nativeSnapshotSearcher) *nornicgrpc.Service {
	t.Helper()
	service, err := nornicgrpc.NewService(nornicgrpc.Config{
		ResolveDatabase: func(_ context.Context, database string) (nornicgrpc.DatabaseDependencies, error) {
			provider := db.QueryEmbeddingForDB(database)
			return nornicgrpc.DatabaseDependencies{Searcher: searcher, EmbedQuery: provider.EmbedQuery, ChunkQuery: provider.ChunkQuery}, nil
		},
	}, nil, nil, searcher)
	require.NoError(t, err)
	return service
}

func TestNativeSearchKeepsOneProvider(t *testing.T) {
	db, server := newReadinessServer(t)
	old := &nativeSnapshotEmbedder{vector: []float32{1, 0}}
	replacement := &nativeSnapshotEmbedder{vector: []float32{0, 1}}
	old.onEmbed = func() { server.SetEmbedder(replacement) }
	db.SetEmbedder(old)
	searcher := &nativeSnapshotSearcher{}
	service := nativeSnapshotService(t, db, searcher)
	_, err := service.SearchText(context.Background(), &gen.SearchTextRequest{Query: "alpha beta", Limit: 5})
	require.NoError(t, err)
	t.Logf("search vectors=%v; old calls=%d, replacement calls=%d", searcher.vectors, old.calls.Load(), replacement.calls.Load())
	require.Equal(t, [][]float32{{1, 0}, {1, 0}}, searcher.vectors, "one native request must not fuse chunks embedded by different provider publications")
	require.Equal(t, int64(2), old.calls.Load())
	require.Zero(t, replacement.calls.Load())
	searcher.vectors = nil
	_, err = service.SearchText(context.Background(), &gen.SearchTextRequest{Query: "alpha beta", Limit: 5})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0, 1}, {0, 1}}, searcher.vectors, "the next request must observe the replacement")
}

func TestMCPPublicationReachesNativeQueryEmbedding(t *testing.T) {
	db, server := newReadinessServer(t)
	old := &nativeSnapshotEmbedder{vector: []float32{1, 0}}
	replacement := &nativeSnapshotEmbedder{vector: []float32{0, 1}}
	db.SetEmbedder(old)
	server.SetEmbedder(replacement)
	require.Same(t, replacement, db.GetCypherExecutor().GetEmbedder())
	vector, err := db.EmbedQuery(context.Background(), "alpha")
	require.NoError(t, err)
	t.Logf("binding provider=%p; native vector=%v", replacement, vector)
	require.Equal(t, replacement.vector, vector, "native query embedding must observe the shared query publication")
}

func TestNativeSearchReportsStartupFallback(t *testing.T) {
	db, server := newReadinessServer(t)
	searcher := &nativeSnapshotSearcher{}
	service := nativeSnapshotService(t, db, searcher)
	request := &gen.SearchTextRequest{Query: "alpha", Limit: 5}
	before, err := service.SearchText(context.Background(), request)
	require.NoError(t, err)
	require.True(t, before.FallbackTriggered)
	require.Equal(t, string(search.SearchFallbackQueryEmbeddingUnavailable), before.FallbackReason)
	provider := &nativeSnapshotEmbedder{vector: []float32{0, 1}}
	server.SetEmbedder(provider)
	searcher.vectors = nil
	after, err := service.SearchText(context.Background(), request)
	require.NoError(t, err)
	require.False(t, after.FallbackTriggered)
	require.Empty(t, after.FallbackReason)
	require.Equal(t, [][]float32{{0, 1}}, searcher.vectors)
	require.Equal(t, int64(1), provider.calls.Load())
}
