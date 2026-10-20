package search

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/resultstream"
)

// continuationExecution owns the ranked retrieval protocol. Streams request a
// depth; only this owner issues it, records its evidence and decides completion.
// In particular, a stream's requested depth is never evidence of coverage.
type continuationExecution struct {
	mode                         SearchContinuationMode
	cap, maxResults, rankedLimit int
	count, nonGrowing            int
	resultCount                  int
	completed                    continuationStep
	status                       continuationStatus
}

// A receipt binds coverage evidence to the request that actually ran. A failed
// or canceled attempt cannot advance the successfully retrieved depth.
type continuationStep struct{ depth, population int }

type continuationStatus struct {
	terminal           bool
	rankedExhausted    bool
	collectionComplete bool
	reason             string
}

func newContinuationExecution(options SearchOptions, request SearchContinuationRequest) *continuationExecution {
	return &continuationExecution{mode: request.Mode, count: -1, cap: options.MaxCandidateLimit, maxResults: request.MaxResults, rankedLimit: request.RankedLimit}
}

// issue plans the actual request after plateau scheduling and cap enforcement.
// Completion is recorded by observe before the next capacity decision is made.
func (e *continuationExecution) issue(ctx context.Context, requested, population int) (continuationStep, error) {
	if err := ctx.Err(); err != nil {
		return continuationStep{}, err
	}
	if e.completed.depth > 0 {
		if e.cap > 0 && e.completed.depth >= e.cap {
			return continuationStep{}, fmt.Errorf("continuation retrieval depth limit reached: %w", resultstream.ErrCapacity)
		}
		if requested <= e.completed.depth {
			if e.completed.depth > int(^uint(0)>>2) {
				return continuationStep{}, fmt.Errorf("continuation retrieval depth overflow: %w", resultstream.ErrCapacity)
			}
			requested = e.completed.depth * 2
		}
		if e.nonGrowing > 0 && population > requested {
			requested = population
		}
	}
	if e.cap > 0 {
		requested = min(requested, e.cap)
	}
	return continuationStep{depth: requested, population: population}, nil
}

// observe consumes the issued step, including initial and growing full-depth
// steps. Index coverage proves the allowed candidate pool complete; only the
// retriever's own exhaustion evidence proves the eligible ranked population.
func (e *continuationExecution) observe(ctx context.Context, step continuationStep, response *SearchResponse, count int, stepErr error) error {
	if stepErr != nil {
		return stepErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.completed = step
	if e.count >= 0 && count <= e.count {
		e.nonGrowing++
	} else {
		e.nonGrowing = 0
	}
	e.count = count
	e.admit(count)
	retrievalComplete := response.RetrievalExhausted && !response.CandidateBudgetReached
	e.status = continuationStatus{}
	switch {
	case e.mode == SearchContinuationRanked && e.maxResults > 0 && count >= e.maxResults && (e.resultCount < count || !retrievalComplete):
		// Coverage is a retrieval proof, not permission to emit past the caller's
		// ceiling. Equality is collection completion only with genuine proof
		// and no omitted rows; otherwise the caller stopped this stream.
		e.status = continuationStatus{terminal: true, reason: SearchContinuationMaxResultsComplete}
	case retrievalComplete:
		e.status = continuationStatus{terminal: true, rankedExhausted: true, collectionComplete: true, reason: SearchContinuationCollectionComplete}
	case response.CandidateBudgetReached || (step.population > 0 && step.depth >= step.population):
		e.status = continuationStatus{terminal: true, reason: SearchContinuationCandidateComplete}
	case e.mode == SearchContinuationRankedThenID && (e.rankedLimit > 0 || (e.nonGrowing >= continuationNonGrowingExpansionLimit && continuationCanInferCandidateBudget(response.SearchMethod))):
		// This mode has an exact catalog phase, so a bounded ANN prefix can
		// stop on its established plateau policy without losing collection rows.
		e.status = continuationStatus{terminal: true, reason: SearchContinuationCandidateComplete}
	}
	return nil
}

func (e *continuationExecution) response(response *SearchResponse) *SearchResponse {
	metadata := *response
	metadata.RetrievalExhausted = e.status.rankedExhausted
	metadata.CandidateBudgetReached = e.status.terminal && !e.status.rankedExhausted && e.status.reason != SearchContinuationMaxResultsComplete
	return &metadata
}

// admit owns the output window after ranking/deduplication, independently of
// which retrieval proof completes the step. Initial, expanded and catalog
// streams consume this count; no adapter infers admission from a reason string.
func (e *continuationExecution) admit(count int) {
	e.resultCount = count
	if e.maxResults > 0 {
		e.resultCount = min(count, e.maxResults)
	}
}

// catalog reports a different proof: an exact enumeration, not candidate depth.
func (e *continuationExecution) catalog(eligible int) {
	e.admit(eligible)
	if e.mode == SearchContinuationID {
		e.status.rankedExhausted = true
	}
	e.status.terminal = true
	e.status.collectionComplete = e.resultCount == eligible
	e.status.reason = SearchContinuationCollectionComplete
	if !e.status.collectionComplete {
		e.status.reason = SearchContinuationMaxResultsComplete
	}
}

func (s continuationStatus) page(hasMore bool) (ranked, collection bool, reason string) {
	if hasMore {
		return s.rankedExhausted, false, SearchContinuationMoreResults
	}
	return s.rankedExhausted, s.collectionComplete, s.reason
}

func continuationCanInferCandidateBudget(searchMethod string) bool {
	method := strings.ToLower(searchMethod)
	return strings.Contains(method, "rrf_hybrid") || strings.HasPrefix(method, "vector_hnsw") || strings.HasPrefix(method, "vector_clustered") || strings.HasPrefix(method, "vector_ivf_hnsw") || strings.HasPrefix(method, "vector_ivfpq")
}

// searchBatch is the cancellation boundary for retrieval work and all its pool
// producers/consumers. Each loop gets its own counter (chunk searches can run
// concurrently). Reader leases must finish through check before returning rows.
const searchBatchSize = 64

type searchBatch struct {
	ctx     context.Context
	done    <-chan struct{}
	visited uint
}

func newSearchBatch(ctx context.Context) searchBatch { return searchBatch{ctx: ctx, done: ctx.Done()} }

func (b *searchBatch) check() error {
	select {
	case <-b.done:
		return b.ctx.Err()
	default:
		return nil
	}
}

// boundary is deliberately just the inlineable counter. Pool consumers call
// the canonical check only at that boundary, avoiding a function/channel poll
// for every cheap member lookup or copied score.
func (b *searchBatch) boundary() bool {
	boundary := b.visited%searchBatchSize == 0
	b.visited++
	return boundary
}
