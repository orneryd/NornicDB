package search

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type coverageReranker struct {
	enabled bool
	results []RerankResult
	err     error
	seen    []RerankCandidate
}

type recordingReranker struct {
	calls [][]RerankCandidate
}

func (r *recordingReranker) Name() string                     { return "recording_reranker" }
func (r *recordingReranker) Enabled() bool                    { return true }
func (r *recordingReranker) IsAvailable(context.Context) bool { return true }
func (r *recordingReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	r.calls = append(r.calls, append([]RerankCandidate(nil), candidates...))
	results := make([]RerankResult, len(candidates))
	for i, candidate := range candidates {
		results[i] = RerankResult{
			ID:         candidate.ID,
			BiScore:    candidate.Score,
			FinalScore: 1 - float64(i)/10,
		}
	}
	return results, nil
}

func (r *coverageReranker) Name() string  { return "coverage_reranker" }
func (r *coverageReranker) Enabled() bool { return r.enabled }
func (r *coverageReranker) IsAvailable(ctx context.Context) bool {
	return r.enabled
}
func (r *coverageReranker) Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error) {
	r.seen = append([]RerankCandidate(nil), candidates...)
	return r.results, r.err
}

type mmrGetNodeErrorEngine struct {
	storage.Engine
	err error
}

func (e *mmrGetNodeErrorEngine) GetNode(storage.NodeID) (*storage.Node, error) {
	return nil, e.err
}

func TestSearchRerankExtraApplyMMRBranches(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { engine.Close() })
	svc := NewServiceWithDimensions(engine, 2)

	results := []rrfResult{{ID: "nornic:a", RRFScore: 0.9}, {ID: "nornic:b", RRFScore: 0.8}, {ID: "nornic:c", RRFScore: 0.7}}
	require.Equal(t, results[:1], svc.applyMMR(context.Background(), results[:1], []float32{1, 0}, 3, 0.5, nil))
	require.Equal(t, results, svc.applyMMR(context.Background(), results, []float32{1, 0}, 3, 1.0, nil))

	_, err := engine.CreateNode(&storage.Node{ID: "nornic:a", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:b", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:c", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0, 1}}})
	require.NoError(t, err)

	diverse := svc.applyMMR(context.Background(), results, []float32{1, 0}, 2, 0.2, map[string]bool{})
	require.Len(t, diverse, 2)
	require.Equal(t, "nornic:a", diverse[0].ID)
	require.NotEqual(t, diverse[0].ID, diverse[1].ID)

	noEmbeddingResults := []rrfResult{{ID: "nornic:d", RRFScore: 0.6}}
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:d", Labels: []string{"Doc"}})
	require.NoError(t, err)
	selected := svc.applyMMR(context.Background(), noEmbeddingResults, []float32{1, 0}, 3, 0.5, map[string]bool{})
	require.Equal(t, noEmbeddingResults, selected)
}

func TestSearchRerankExtraApplyMMRErrorBranches(t *testing.T) {
	missingEngine := storage.NewMemoryEngine()
	t.Cleanup(func() { missingEngine.Close() })
	_, err := missingEngine.CreateNode(&storage.Node{ID: "nornic:ok", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	missingSvc := NewServiceWithDimensions(missingEngine, 2)
	seen := map[string]bool{}
	selected := missingSvc.applyMMR(context.Background(), []rrfResult{
		{ID: "nornic:missing", RRFScore: 0.9},
		{ID: "nornic:ok", RRFScore: 0.8},
	}, []float32{1, 0}, 2, 0.5, seen)
	require.Equal(t, []rrfResult{{ID: "nornic:ok", RRFScore: 0.8}}, selected)
	require.True(t, seen["nornic:missing"])

	boomEngine := &mmrGetNodeErrorEngine{Engine: storage.NewMemoryEngine(), err: errors.New("storage boom")}
	t.Cleanup(func() { boomEngine.Close() })
	boomSvc := NewServiceWithDimensions(boomEngine, 2)
	results := []rrfResult{{ID: "a", RRFScore: 0.9}, {ID: "b", RRFScore: 0.8}}
	selected = boomSvc.applyMMR(context.Background(), results, []float32{1, 0}, 2, 0.5, nil)
	require.Equal(t, results, selected)
}

func TestSearchRerankExtraApplyStage2Branches(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { engine.Close() })
	svc := NewServiceWithDimensions(engine, 2)
	ctx := context.Background()

	base := []rrfResult{
		{ID: "nornic:a", RRFScore: 0.9, VectorRank: 1, BM25Rank: 2, OriginalScore: 0.9},
		{ID: "nornic:b", RRFScore: 0.8, VectorRank: 2, BM25Rank: 1, OriginalScore: 0.8},
	}
	require.Equal(t, base, svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{}, nil, nil))
	require.Equal(t, []rrfResult{}, svc.applyStage2Rerank(ctx, "query", []rrfResult{}, &SearchOptions{}, nil, &coverageReranker{enabled: true}))
	require.Equal(t, base, svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{}, nil, &coverageReranker{enabled: false}))

	_, err := engine.CreateNode(&storage.Node{ID: "nornic:a", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Alpha", "content": "alpha content"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:b", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Beta", "content": "beta content"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:c", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Gamma", "content": "gamma content"}})
	require.NoError(t, err)

	failing := &coverageReranker{enabled: true, err: errors.New("rerank failed")}
	require.Equal(t, base[:1], svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{RerankTopK: 1}, nil, failing))
	require.Len(t, failing.seen, 1)

	extended := append(append([]rrfResult(nil), base...), rrfResult{ID: "nornic:c", RRFScore: 0.7, VectorRank: 3, BM25Rank: 3, OriginalScore: 0.7})
	bounded := &coverageReranker{enabled: true, results: []RerankResult{{ID: "nornic:b", BiScore: 0.8, FinalScore: 0.97}, {ID: "nornic:a", BiScore: 0.9, FinalScore: 0.2}}}
	require.Equal(t, []rrfResult{
		{ID: "nornic:b", RRFScore: 0.97, VectorRank: 2, BM25Rank: 1, OriginalScore: 0.8},
		{ID: "nornic:a", RRFScore: 0.2, VectorRank: 1, BM25Rank: 2, OriginalScore: 0.9},
	}, svc.applyStage2Rerank(ctx, "query", extended, &SearchOptions{RerankTopK: 2}, nil, bounded))
	require.Len(t, bounded.seen, 2)

	flat := &coverageReranker{enabled: true, results: []RerankResult{{ID: "nornic:b", BiScore: 0.8, FinalScore: 0.51}, {ID: "nornic:a", BiScore: 0.9, FinalScore: 0.50}}}
	require.Equal(t, base, svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{}, nil, flat))

	reranker := &coverageReranker{enabled: true, results: []RerankResult{{ID: "nornic:b", BiScore: 0.8, FinalScore: 0.95}, {ID: "missing", BiScore: 0.1, FinalScore: 0.2}, {ID: "nornic:a", BiScore: 0.9, FinalScore: 0.1}}}
	reranked := svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{RerankMinScore: 0.15}, nil, reranker)
	require.Equal(t, []rrfResult{{ID: "nornic:b", RRFScore: 0.95, VectorRank: 2, BM25Rank: 1, OriginalScore: 0.8}, {ID: "missing", RRFScore: 0.2, OriginalScore: 0.1}}, reranked)
}

func TestStage2RerankUsesWinningPassageAndBoundsFallbackContent(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { engine.Close() })
	svc := NewServiceWithDimensions(engine, 2)

	longPrefix := strings.Repeat("предисловие ", 80)
	longSuffix := strings.Repeat(" заключение", 80)
	nodes := []*storage.Node{
		{
			ID:         "nornic:vector",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"text": longPrefix + "whole node" + longSuffix},
			EmbedMeta:  map[string]interface{}{"chunk_texts": []string{"first passage", "matching vector passage"}},
		},
		{
			ID:         "nornic:bm25",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"text": longPrefix + "alpha tango sierra" + longSuffix},
		},
		{
			ID:         "nornic:prefix",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"text": strings.Repeat("я", 100)},
		},
	}
	for _, node := range nodes {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	reranker := &recordingReranker{}
	results := []rrfResult{
		{ID: "nornic:vector", MatchID: "nornic:vector-chunk-1", RRFScore: 0.9, VectorRank: 1},
		{ID: "nornic:bm25", RRFScore: 0.8, BM25Rank: 1},
		{ID: "nornic:prefix", RRFScore: 0.7},
	}
	svc.applyStage2Rerank(context.Background(), "alpha tango", results, &SearchOptions{RerankMaxChars: 96}, nil, reranker)

	require.Len(t, reranker.calls, 1)
	require.Len(t, reranker.calls[0], 3)
	// The matched chunk comes first and is extended with its neighbours within the budget.
	require.Equal(t, "first passage matching vector passage", reranker.calls[0][0].Content)
	require.Contains(t, reranker.calls[0][1].Content, "alpha")
	for _, candidate := range reranker.calls[0] {
		require.LessOrEqual(t, utf8.RuneCountInString(candidate.Content), 96)
		require.True(t, utf8.ValidString(candidate.Content))
	}
}

func TestRerankCandidateCharCeilingUsesOptionThenEnvironmentThenDefault(t *testing.T) {
	t.Setenv(EnvSearchRerankMaxDocumentChars, "4096")
	require.Equal(t, 1024, effectiveRerankMaxChars(&SearchOptions{RerankMaxChars: 1024}))
	require.Equal(t, 4096, effectiveRerankMaxChars(&SearchOptions{}))
	t.Setenv(EnvSearchRerankMaxDocumentChars, "invalid")
	require.Equal(t, defaultRerankMaxDocumentChars, effectiveRerankMaxChars(&SearchOptions{}))
	t.Setenv(EnvSearchRerankMaxDocumentChars, "0")
	require.Equal(t, defaultRerankMaxDocumentChars, effectiveRerankMaxChars(&SearchOptions{}))
	// The deprecated BYTES variable is honoured when CHARS is unset, as a character count.
	t.Setenv(EnvSearchRerankMaxDocumentChars, "")
	t.Setenv(EnvSearchRerankMaxDocumentBytes, "3000")
	require.Equal(t, 3000, effectiveRerankMaxChars(&SearchOptions{}))
	t.Setenv(EnvSearchRerankMaxDocumentChars, "512")
	require.Equal(t, 512, effectiveRerankMaxChars(&SearchOptions{}), "CHARS wins over BYTES")
}

func TestStage2RerankMemoSubmitsOnlyNewCandidates(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { engine.Close() })
	svc := NewServiceWithDimensions(engine, 2)
	for _, id := range []string{"nornic:a", "nornic:b", "nornic:c"} {
		_, err := engine.CreateNode(&storage.Node{
			ID:         storage.NodeID(id),
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"text": id + " searchable content"},
		})
		require.NoError(t, err)
	}

	reranker := &recordingReranker{}
	ctx := withRerankMemo(context.Background(), newRerankMemo())
	first := []rrfResult{{ID: "nornic:a", RRFScore: 0.9}, {ID: "nornic:b", RRFScore: 0.8}}
	require.Len(t, svc.applyStage2Rerank(ctx, "query", first, &SearchOptions{}, nil, reranker), 2)
	second := append(append([]rrfResult(nil), first...), rrfResult{ID: "nornic:c", RRFScore: 0.7})
	require.Len(t, svc.applyStage2Rerank(ctx, "query", second, &SearchOptions{}, nil, reranker), 3)

	require.Len(t, reranker.calls, 2)
	require.Len(t, reranker.calls[0], 2)
	require.Equal(t, []RerankCandidate{{ID: "nornic:c", Content: "Doc nornic:c searchable content", Score: 0.7}}, reranker.calls[1])

	svc.applyStage2Rerank(ctx, "different query", first, &SearchOptions{}, nil, reranker)
	require.Len(t, reranker.calls, 3)
	require.Len(t, reranker.calls[2], 2, "a distinct query must not reuse prior scores")
}

var benchmarkRerankCandidateContent string

func BenchmarkRerankCandidateContentForLongDocument(b *testing.B) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	text := strings.Repeat("предисловие ", 7000) + "alpha tango" + strings.Repeat(" заключение", 7000)
	node := &storage.Node{
		ID:         "nornic:long",
		Labels:     []string{"LongDoc"},
		Properties: map[string]interface{}{"text": text},
	}
	result := rrfResult{ID: "nornic:long", BM25Rank: 1}
	b.Run("bounded_matching_window", func(b *testing.B) {
		b.SetBytes(int64(len(text)))
		b.ReportAllocs()
		for range b.N {
			content := svc.rerankCandidateContent(node, result, "alpha tango", 4096)
			if utf8.RuneCountInString(content) > 4096 {
				b.Fatalf("candidate content exceeded character ceiling: %d", utf8.RuneCountInString(content))
			}
			benchmarkRerankCandidateContent = content
		}
	})
	b.Run("whole_searchable_node", func(b *testing.B) {
		b.SetBytes(int64(len(text)))
		b.ReportAllocs()
		for range b.N {
			benchmarkRerankCandidateContent = svc.extractSearchableText(node)
		}
	})
}
