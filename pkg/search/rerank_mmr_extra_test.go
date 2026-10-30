package search

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sort"
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

type identityOrderedReranker struct {
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

func (r *identityOrderedReranker) Name() string                     { return "identity_ordered_reranker" }
func (r *identityOrderedReranker) Enabled() bool                    { return true }
func (r *identityOrderedReranker) IsAvailable(context.Context) bool { return true }
func (r *identityOrderedReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	r.calls = append(r.calls, append([]RerankCandidate(nil), candidates...))
	ordered := append([]RerankCandidate(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		priority := func(id string) int {
			switch id {
			case "nornic:b", "doc-b":
				return 0
			case "nornic:a", "doc-a":
				return 1
			default:
				return 2
			}
		}
		return priority(ordered[i].ID) < priority(ordered[j].ID)
	})
	results := make([]RerankResult, len(ordered))
	for index, candidate := range ordered {
		results[index] = RerankResult{ID: candidate.ID, FinalScore: 1 - float64(index)/10}
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

// TestSearchRerankExtraApplyMMRBranches covers each way applyMMR can decline
// to diversify and each way it diversifies: a result set no larger than the
// requested k, a lambda that keeps the order, a fully diverse selection, a
// node without an embedding, candidates the store cannot read (dropped as
// orphans), and a storage error (the input order stands).
func TestSearchRerankExtraApplyMMRBranches(t *testing.T) {
	results := []rrfResult{
		{ID: "nornic:a", RRFScore: 0.9},
		{ID: "nornic:b", RRFScore: 0.8},
		{ID: "nornic:c", RRFScore: 0.7},
	}
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, engine *storage.MemoryEngine)
	}{
		{
			name: "result set no larger than k keeps every result",
			run: func(t *testing.T, engine *storage.MemoryEngine) {
				svc := NewService(engine, 2, nil)
				require.Equal(t, results[:1], svc.applyMMR(context.Background(), results[:1], []float32{1, 0}, 3, 0.5, nil))
			},
		},
		{
			name: "lambda at one keeps the input order",
			run: func(t *testing.T, engine *storage.MemoryEngine) {
				svc := NewService(engine, 2, nil)
				require.Equal(t, results, svc.applyMMR(context.Background(), results, []float32{1, 0}, 3, 1.0, nil))
			},
		},
		{
			name: "diverse selection and a node without embeddings",
			run: func(t *testing.T, engine *storage.MemoryEngine) {
				for _, node := range []*storage.Node{
					{ID: "nornic:a", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}},
					{ID: "nornic:b", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}},
					{ID: "nornic:c", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0, 1}}},
				} {
					_, err := engine.CreateNode(node)
					require.NoError(t, err)
				}
				svc := NewService(engine, 2, nil)

				diverse := svc.applyMMR(context.Background(), results, []float32{1, 0}, 2, 0.2, map[string]bool{})
				require.Len(t, diverse, 2)
				require.Equal(t, "nornic:a", diverse[0].ID)
				require.NotEqual(t, diverse[0].ID, diverse[1].ID)

				noEmbeddingResults := []rrfResult{{ID: "nornic:d", RRFScore: 0.6}}
				_, err := engine.CreateNode(&storage.Node{ID: "nornic:d", Labels: []string{"Doc"}})
				require.NoError(t, err)
				require.Equal(t, noEmbeddingResults, svc.applyMMR(context.Background(), noEmbeddingResults, []float32{1, 0}, 3, 0.5, map[string]bool{}))
			},
		},
		{
			name: "unreadable candidates are dropped as orphans",
			run: func(t *testing.T, engine *storage.MemoryEngine) {
				_, err := engine.CreateNode(&storage.Node{ID: "nornic:ok", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}})
				require.NoError(t, err)
				svc := NewService(engine, 2, nil)
				seen := map[string]bool{}
				require.Equal(t, []rrfResult{{ID: "nornic:ok", RRFScore: 0.8}}, svc.applyMMR(context.Background(), []rrfResult{
					{ID: "nornic:missing", RRFScore: 0.9},
					{ID: "nornic:ok", RRFScore: 0.8},
				}, []float32{1, 0}, 2, 0.5, seen))
				require.True(t, seen["nornic:missing"])
			},
		},
		{
			name: "storage error keeps the input order",
			run: func(t *testing.T, engine *storage.MemoryEngine) {
				boomEngine := &mmrGetNodeErrorEngine{Engine: engine, err: errors.New("storage boom")}
				boomSvc := NewService(boomEngine, 2, nil)
				input := []rrfResult{{ID: "a", RRFScore: 0.9}, {ID: "b", RRFScore: 0.8}}
				require.Equal(t, input, boomSvc.applyMMR(context.Background(), input, []float32{1, 0}, 2, 0.5, nil))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)))
		})
	}
}

func TestSearchRerankExtraApplyStage2Branches(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { engine.Close() })
	svc := NewService(engine, 2, nil)
	ctx := context.Background()

	base := []rrfResult{
		{ID: "nornic:a", RRFScore: 0.9, VectorRank: 1, BM25Rank: 2, OriginalScore: 0.9},
		{ID: "nornic:b", RRFScore: 0.8, VectorRank: 2, BM25Rank: 1, OriginalScore: 0.8},
	}
	rerank := func(results []rrfResult, opts *SearchOptions, reranker Reranker, want SearchFallbackReason) []rrfResult {
		t.Helper()
		out, outcome := svc.applyStage2Rerank(ctx, "query", results, opts, nil, reranker)
		require.Equal(t, want, outcome.FallbackReason)
		return out
	}
	require.Equal(t, base, rerank(base, &SearchOptions{}, nil, SearchFallbackNone))
	require.Equal(t, []rrfResult{}, rerank([]rrfResult{}, &SearchOptions{}, &coverageReranker{enabled: true}, SearchFallbackNone))
	require.Equal(t, base, rerank(base, &SearchOptions{}, &coverageReranker{enabled: false}, SearchFallbackNone))
	// No candidate has content to score yet: nothing reranks.
	require.Equal(t, base, rerank(base, &SearchOptions{}, &coverageReranker{enabled: true}, SearchFallbackRerankNoCandidates))

	_, err := engine.CreateNode(&storage.Node{ID: "nornic:a", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Alpha", "content": "alpha content"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:b", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Beta", "content": "beta content"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:c", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Gamma", "content": "gamma content"}})
	require.NoError(t, err)

	failing := &coverageReranker{enabled: true, err: errors.New("rerank failed")}
	require.Equal(t, base[:1], rerank(base, &SearchOptions{RerankTopK: 1}, failing, SearchFallbackRerankFailed))
	require.Len(t, failing.seen, 1)

	extended := append(append([]rrfResult(nil), base...), rrfResult{ID: "nornic:c", RRFScore: 0.7, VectorRank: 3, BM25Rank: 3, OriginalScore: 0.7})
	bounded := &coverageReranker{enabled: true, results: []RerankResult{{ID: "nornic:b", BiScore: 0.8, FinalScore: 0.97}, {ID: "nornic:a", BiScore: 0.9, FinalScore: 0.2}}}
	require.Equal(t, []rrfResult{
		{ID: "nornic:b", RRFScore: 0.97, VectorRank: 2, BM25Rank: 1, OriginalScore: 0.8},
		{ID: "nornic:a", RRFScore: 0.2, VectorRank: 1, BM25Rank: 2, OriginalScore: 0.9},
	}, rerank(extended, &SearchOptions{RerankTopK: 2}, bounded, SearchFallbackNone))
	require.Len(t, bounded.seen, 2)

	flat := &coverageReranker{enabled: true, results: []RerankResult{{ID: "nornic:b", BiScore: 0.8, FinalScore: 0.51}, {ID: "nornic:a", BiScore: 0.9, FinalScore: 0.50}}}
	require.Equal(t, base, rerank(base, &SearchOptions{}, flat, SearchFallbackRerankScoresFlat))

	reranker := &coverageReranker{enabled: true, results: []RerankResult{{ID: "nornic:b", BiScore: 0.8, FinalScore: 0.95}, {ID: "missing", BiScore: 0.1, FinalScore: 0.2}, {ID: "nornic:a", BiScore: 0.9, FinalScore: 0.1}}}
	require.Equal(t, base, rerank(base, &SearchOptions{RerankMinScore: 0.15}, reranker, SearchFallbackRerankFailed))
}

func TestStage2RerankUsesWinningPassageAndBoundsFallbackContent(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { engine.Close() })
	svc := NewService(engine, 2, nil)

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
		{ID: "nornic:vector", MatchID: nodeChunkVectorKey("nornic:vector", 1), RRFScore: 0.9, VectorRank: 1},
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
