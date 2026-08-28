package search

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type overlapBM25Index struct {
	bm25Index
	started       chan<- struct{}
	vectorStarted <-chan struct{}
	release       <-chan struct{}
}

func (i *overlapBM25Index) Search(string, int) []indexResult {
	close(i.started)
	select {
	case <-i.vectorStarted:
	case <-i.release:
	}
	return nil
}

type overlapCandidateGenerator struct {
	started     chan<- struct{}
	bm25Started <-chan struct{}
	release     <-chan struct{}
}

type immediateErrorCandidateGenerator struct {
	started chan<- struct{}
	err     error
}

func (g *immediateErrorCandidateGenerator) SearchCandidates(context.Context, []float32, int, float64) ([]Candidate, error) {
	close(g.started)
	return nil, g.err
}

type cancelableCandidateGenerator struct {
	started chan<- struct{}
}

func (g *cancelableCandidateGenerator) SearchCandidates(ctx context.Context, _ []float32, _ int, _ float64) ([]Candidate, error) {
	close(g.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

type blockingBM25Index struct {
	bm25Index
	started chan<- struct{}
	release <-chan struct{}
}

func (i *blockingBM25Index) Search(string, int) []indexResult {
	close(i.started)
	<-i.release
	return nil
}

func (g *overlapCandidateGenerator) SearchCandidates(context.Context, []float32, int, float64) ([]Candidate, error) {
	close(g.started)
	select {
	case <-g.bm25Started:
	case <-g.release:
	}
	return nil, nil
}

func TestRRFHybridSearch_ParallelRetrievalOverlaps(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	bm25Started := make(chan struct{})
	vectorStarted := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	svc.fulltextIndex = &overlapBM25Index{
		bm25Index:     svc.fulltextIndex,
		started:       bm25Started,
		vectorStarted: vectorStarted,
		release:       release,
	}
	svc.vectorPipeline = NewVectorSearchPipeline(
		&overlapCandidateGenerator{
			started:     vectorStarted,
			bm25Started: bm25Started,
			release:     release,
		},
		&IdentityExactScorer{},
	)

	done := make(chan error, 1)
	go func() {
		opts := DefaultSearchOptions()
		opts.Limit = 10
		_, err := svc.rrfHybridSearch(context.Background(), "parallel retrieval", []float32{1, 0, 0, 0}, opts)
		done <- err
	}()

	select {
	case <-bm25Started:
		select {
		case <-vectorStarted:
		case <-time.After(500 * time.Millisecond):
			close(release)
			t.Fatal("vector retrieval did not overlap BM25 retrieval")
		}
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("BM25 retrieval did not overlap vector retrieval")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("hybrid search did not return after both retrieval branches completed")
	}
}

func TestRRFHybridSearch_ParallelRetrievalPreservesSequentialRanking(t *testing.T) {
	fixture, err := loadDocsCorpusFixture()
	require.NoError(t, err)
	svc := buildDocsSearchFixture(t, fixture.chunks[:1000])
	defer svc.Close()

	query := "configure hybrid vector search and BM25 indexes"
	embedding := docsTextVector(query, docsVectorDims)
	opts := DefaultSearchOptions()
	opts.Limit = 20

	expected, err := sequentialHybridReference(svc, query, embedding, opts)
	require.NoError(t, err)
	actual, err := svc.rrfHybridSearch(context.Background(), query, embedding, opts)
	require.NoError(t, err)
	require.Equal(t, expected.results, actual.Results)
	require.Equal(t, expected.vectorCandidates, actual.Metrics.VectorCandidates)
	require.Equal(t, expected.bm25Candidates, actual.Metrics.BM25Candidates)
	require.Equal(t, expected.fusedCandidates, actual.Metrics.FusedCandidates)
	require.Equal(t, "rrf_hybrid", actual.SearchMethod)
}

type sequentialHybridReferenceResult struct {
	results          []SearchResult
	vectorCandidates int
	bm25Candidates   int
	fusedCandidates  int
}

func sequentialHybridReference(
	svc *Service,
	query string,
	embedding []float32,
	opts *SearchOptions,
) (sequentialHybridReferenceResult, error) {
	ctx := withQueryText(context.Background(), query)
	pipeline, err := svc.getOrCreateVectorPipeline(ctx)
	if err != nil {
		return sequentialHybridReferenceResult{}, err
	}
	seenOrphans := make(map[string]bool)
	vectorResults, _, err := svc.adaptiveVectorSearch(ctx, pipeline, embedding, opts, func(results []indexResult) []indexResult {
		results = svc.filterDecayedCandidates(results)
		if len(opts.Types) > 0 || len(opts.Filters) > 0 {
			results = svc.filterByTypeAndProperties(ctx, results, opts.Types, opts.Filters, seenOrphans)
		}
		return results
	})
	if err != nil {
		return sequentialHybridReferenceResult{}, err
	}

	var bm25Results []indexResult
	if svc.fulltextIndex != nil {
		bm25Results, _, err = svc.adaptiveBM25Search(ctx, svc.fulltextIndex, query, opts, func(results []indexResult) []indexResult {
			results = svc.filterDecayedCandidates(results)
			if len(opts.Types) > 0 || len(opts.Filters) > 0 {
				results = svc.filterByTypeAndProperties(ctx, results, opts.Types, opts.Filters, seenOrphans)
			}
			return results
		})
		if err != nil {
			return sequentialHybridReferenceResult{}, err
		}
	}
	fusedResults := svc.fuseRRF(vectorResults, bm25Results, opts)
	results := svc.enrichResults(ctx, fusedResults, opts.Limit, seenOrphans)
	return sequentialHybridReferenceResult{
		results:          results,
		vectorCandidates: len(vectorResults),
		bm25Candidates:   len(bm25Results),
		fusedCandidates:  len(fusedResults),
	}, nil
}

func TestRRFHybridSearch_ParallelRetrievalJoinsBM25BeforeVectorError(t *testing.T) {
	wantErr := errors.New("vector retrieval failed")
	bm25Started := make(chan struct{})
	vectorStarted := make(chan struct{})
	releaseBM25 := make(chan struct{})
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.fulltextIndex = &blockingBM25Index{
		bm25Index: svc.fulltextIndex,
		started:   bm25Started,
		release:   releaseBM25,
	}
	svc.vectorPipeline = NewVectorSearchPipeline(
		&immediateErrorCandidateGenerator{started: vectorStarted, err: wantErr},
		&IdentityExactScorer{},
	)

	done := make(chan error, 1)
	go func() {
		_, err := svc.rrfHybridSearch(context.Background(), "query", []float32{1, 0, 0, 0}, DefaultSearchOptions())
		done <- err
	}()
	<-bm25Started
	<-vectorStarted
	select {
	case err := <-done:
		t.Fatalf("hybrid search returned %v before BM25 retrieval joined", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseBM25)
	require.ErrorIs(t, <-done, wantErr)
}

func TestRRFHybridSearch_ParallelRetrievalJoinsBM25AfterCancellation(t *testing.T) {
	bm25Started := make(chan struct{})
	vectorStarted := make(chan struct{})
	releaseBM25 := make(chan struct{})
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 4)
	svc.fulltextIndex = &blockingBM25Index{
		bm25Index: svc.fulltextIndex,
		started:   bm25Started,
		release:   releaseBM25,
	}
	svc.vectorPipeline = NewVectorSearchPipeline(
		&cancelableCandidateGenerator{started: vectorStarted},
		&IdentityExactScorer{},
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := svc.rrfHybridSearch(ctx, "query", []float32{1, 0, 0, 0}, DefaultSearchOptions())
		done <- err
	}()
	<-bm25Started
	<-vectorStarted
	cancel()
	select {
	case err := <-done:
		t.Fatalf("hybrid search returned %v before BM25 retrieval joined", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseBM25)
	require.ErrorIs(t, <-done, context.Canceled)
}
