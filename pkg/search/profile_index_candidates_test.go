package search

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveVectorObservationsCountRepeatedDistinctNodePrefixes(t *testing.T) {
	generator := &recordingCandidateGenerator{candidates: []Candidate{
		{ID: nodeChunkVectorKey("doc-1", 0), Score: 1}, {ID: nodeChunkVectorKey("doc-1", 1), Score: 0.9},
		{ID: nodeChunkVectorKey("doc-2", 0), Score: 0.8}, {ID: nodeChunkVectorKey("doc-3", 0), Score: 0.7},
	}}
	service := &Service{}
	observer := new(fixtureowner.QueryReadObserver)
	ctx := queryprofile.WithObserver(t.Context(), observer)
	results, _, err := service.adaptiveVectorSearch(ctx, NewVectorSearchPipeline(generator, &IdentityExactScorer{}), []float32{1, 0}, adaptiveOverfetchTestOptions(2), nil, nil, queryprofile.Access(ctx, profileKindNodeVectorCandidates, ""))
	require.NoError(t, err)
	require.Equal(t, []string{"doc-1", "doc-2"}, indexResultIDs(results))
	require.Equal(t, []int{2, 4}, generator.limits)
	rows, hits, recorded := observer.Reads(profileKindNodeVectorCandidates, "")
	require.True(t, recorded)
	require.EqualValues(t, 4, rows, "one distinct node in the first prefix and three in the second")
	require.EqualValues(t, 2, hits)
	require.Equal(t, []string{"vector candidate generator", "vector candidate generator"}, observer.Callbacks(), "a custom generator remains opaque despite reporting candidate rows")
}

func TestAdaptiveBM25ObservationsPrecedeFilteringAndIncludeRetries(t *testing.T) {
	index := &recordingBM25Index{results: []indexResult{{ID: "doc-1"}, {ID: "doc-2"}, {ID: "doc-3"}, {ID: "doc-4"}}}
	observer := new(fixtureowner.QueryReadObserver)
	ctx := queryprofile.WithObserver(t.Context(), observer)
	service := &Service{}
	results, _, err := service.adaptiveBM25Search(ctx, index, "needle", adaptiveOverfetchTestOptions(2), func(input []indexResult) ([]indexResult, error) {
		var kept []indexResult
		for _, r := range input {
			if r.ID == "doc-3" || r.ID == "doc-4" {
				kept = append(kept, r)
			}
		}
		return kept, nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"doc-3", "doc-4"}, indexResultIDs(results))
	require.Equal(t, []int{2, 4}, index.limits)
	rows, hits, recorded := observer.Reads(profileKindNodeFulltextCandidates, "")
	require.True(t, recorded)
	require.EqualValues(t, 6, rows)
	require.EqualValues(t, 2, hits)
}

func TestBM25CompletionObservationsCountItsSeparateFullPrefix(t *testing.T) {
	index := &recordingBM25Index{results: []indexResult{{ID: "doc-1"}, {ID: "doc-2"}, {ID: "doc-3"}, {ID: "doc-4"}}}
	observer := new(fixtureowner.QueryReadObserver)
	ctx := queryprofile.WithObserver(t.Context(), observer)
	service := &Service{}
	results, _, err := service.completeBM25Search(ctx, index.Search, "needle", 4, 1, 2, func(input []indexResult) ([]indexResult, error) {
		for _, r := range input {
			if r.ID == "doc-4" {
				return []indexResult{r}, nil
			}
		}
		return nil, nil
	}, vectorOverfetchStats{}, queryprofile.Access(ctx, profileKindNodeFulltextCandidates, ""))
	require.NoError(t, err)
	require.Equal(t, []string{"doc-4"}, indexResultIDs(results))
	rows, hits, recorded := observer.Reads(profileKindNodeFulltextCandidates, "")
	require.True(t, recorded)
	require.EqualValues(t, 4, rows)
	require.EqualValues(t, 1, hits)
}

type failedObservedGenerator struct {
	cause error
	calls int
}

func (g *failedObservedGenerator) SearchCandidates(context.Context, []float32, int, float64) ([]Candidate, error) {
	g.calls++
	return nil, g.cause
}
func TestFailedVectorPassStillReportsItsActualAttempt(t *testing.T) {
	failure := errors.New("candidate provider failed")
	generator := &failedObservedGenerator{cause: failure}
	observer := new(fixtureowner.QueryReadObserver)
	ctx := queryprofile.WithObserver(t.Context(), observer)
	service := &Service{}
	result, _, err := service.adaptiveVectorSearch(ctx, NewVectorSearchPipeline(generator, &IdentityExactScorer{}), []float32{1, 0}, adaptiveOverfetchTestOptions(2), nil, nil, queryprofile.Access(ctx, profileKindNodeVectorCandidates, ""))
	require.ErrorIs(t, err, failure)
	require.Nil(t, result)
	require.Equal(t, 1, generator.calls)
	rows, hits, recorded := observer.Reads(profileKindNodeVectorCandidates, "")
	require.True(t, recorded)
	require.Zero(t, rows)
	require.EqualValues(t, 1, hits)
}

type observedCustomScorer struct {
	*IdentityExactScorer
	engine storage.Engine
	calls  int
}

func (s *observedCustomScorer) ScoreCandidates(ctx context.Context, query []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	s.calls++
	node, err := s.engine.GetNode(ctx, "a")
	queryprofile.EntityReadResult(queryprofile.Access(ctx, queryprofile.KindNodeByIDSeek, ""), node, err)
	if err != nil {
		return nil, err
	}
	return s.IdentityExactScorer.ScoreCandidates(ctx, query, candidates)
}
func TestCustomVectorStageStaysOpaqueWhenItsInnerReadIsObserved(t *testing.T) {
	engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "vector_provider")
	_, err := engine.CreateNode(t.Context(), &storage.Node{ID: "a"})
	require.NoError(t, err)
	generator := &recordingCandidateGenerator{candidates: []Candidate{{ID: "a", Score: 1}, {ID: "b", Score: 0.8}}}
	scorer := &observedCustomScorer{IdentityExactScorer: &IdentityExactScorer{}, engine: engine}
	observer := new(fixtureowner.QueryReadObserver)
	ctx := queryprofile.WithObserver(t.Context(), observer)
	result, err := NewVectorSearchPipeline(generator, scorer).Search(ctx, []float32{1, 0}, 2, -1)
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Positive(t, scorer.calls)
	rows, hits, recorded := observer.Reads(queryprofile.KindNodeByIDSeek, "")
	require.True(t, recorded)
	require.EqualValues(t, scorer.calls, rows)
	require.EqualValues(t, scorer.calls, hits)
	var generated, scored int
	for _, callback := range observer.Callbacks() {
		switch callback {
		case "vector candidate generator":
			generated++
		case "vector exact scorer":
			scored++
		default:
			t.Fatalf("unexpected callback %q", callback)
		}
	}
	require.Equal(t, len(generator.limits), generated)
	require.Equal(t, scorer.calls, scored, "embedding an owned scorer does not certify its replacement")
}

func TestCachedSearchReportsNoRepeatedIndexRequest(t *testing.T) {
	service := quantizationCachedService(t)
	options := DefaultSearchOptions()
	options.Limit = 1
	options.SkipMatches = true
	firstObserver := new(fixtureowner.QueryReadObserver)
	first, err := service.Search(queryprofile.WithObserver(t.Context(), firstObserver), "", []float32{1, 1, 0, 0}, options)
	require.NoError(t, err)
	require.Len(t, first.Results, 1)
	_, firstHits, recorded := firstObserver.Reads(profileKindNodeVectorCandidates, "")
	require.True(t, recorded)
	require.Positive(t, firstHits)
	secondObserver := new(fixtureowner.QueryReadObserver)
	second, err := service.Search(queryprofile.WithObserver(t.Context(), secondObserver), "", []float32{1, 1, 0, 0}, options)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.NotSame(t, first, second)
	require.Empty(t, secondObserver.RecordedKinds(), "a real result-cache hit reuses retained candidates without querying indexes")
	require.Empty(t, secondObserver.Callbacks())
}
