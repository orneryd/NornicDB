package search

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"net"
	"regexp"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

const (
	outerRRFK              = 60.0
	minChunkCandidateLimit = 10
	maxChunkCandidateLimit = 100
)

var embeddingHTTPStatusPattern = regexp.MustCompile(`(?i)\b(?:http(?: status)?|status(?: code)?|returned)\D{0,12}([1-5][0-9]{2})\b`)

// ChunkQueryFunc splits a text query into embedding-safe chunks.
type ChunkQueryFunc func(ctx context.Context, query string) ([]string, error)

// EmbedQueryFunc embeds one query chunk. A nil or empty vector is unavailable.
type EmbedQueryFunc func(ctx context.Context, query string) ([]float32, error)

// SearchQueryFunc searches one query and optional vector.
type SearchQueryFunc func(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error)

// ChunkedSearchErrorPolicy identifies adapter-specific errors that must not fall back.
type ChunkedSearchErrorPolicy struct {
	FatalEmbeddingError func(error) bool
	FatalSearchError    func(error) bool
	// Transport identifies the caller in fallback warning logs.
	Transport string
}

// SearchTextChunks applies the canonical text-search semantics used by all transports.
// Multi-chunk queries are searched independently and fused with an outer RRF pass.
func SearchTextChunks(
	ctx context.Context,
	query string,
	opts *SearchOptions,
	chunkQuery ChunkQueryFunc,
	embedQuery EmbedQueryFunc,
	searchQuery SearchQueryFunc,
) (*SearchResponse, error) {
	return SearchTextChunksWithErrorPolicy(ctx, query, opts, chunkQuery, embedQuery, searchQuery, ChunkedSearchErrorPolicy{})
}

// SearchTextChunksWithErrorPolicy applies SearchTextChunks while allowing an adapter
// to designate errors that must be returned instead of triggering BM25 fallback.
func SearchTextChunksWithErrorPolicy(
	ctx context.Context,
	query string,
	opts *SearchOptions,
	chunkQuery ChunkQueryFunc,
	embedQuery EmbedQueryFunc,
	searchQuery SearchQueryFunc,
	errorPolicy ChunkedSearchErrorPolicy,
) (response *SearchResponse, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Every selected search completes here, including lexical callbacks and
	// result fusion. A canceled request/DB child cannot publish success just
	// because its callback returned hits. A live request still permits the
	// existing provider-timeout fallback.
	defer func() {
		if fatal := fatalChunkedSearchError(ctx, errorPolicy.FatalSearchError, retErr); fatal != nil {
			response, retErr = nil, fatal
		}
	}()
	if opts == nil {
		defaults := defaultSearchOptionsValue()
		opts = &defaults
	}
	if embedQuery == nil {
		response, err := searchQuery(ctx, query, nil, opts)
		if !opts.fallbackEnabled() {
			return response, err
		}
		return withSearchFallback(response, SearchFallbackNoEmbedder), err
	}

	chunks := []string{query}
	if chunkQuery != nil {
		var err error
		chunks, err = chunkQuery(ctx, query)
		if err != nil {
			return nil, err
		}
	}
	total := len(chunks)
	chunks, err := LimitQueryChunks(chunks)
	if err != nil {
		return nil, err
	}
	response, searched, err := searchQueryChunks(ctx, query, chunks, opts, embedQuery, searchQuery, errorPolicy)
	if total <= 1 || response == nil {
		return response, err
	}
	// The callback may return a cached response shared with other callers.
	annotated := *response
	annotated.QueryChunksSearched = searched
	annotated.QueryChunksTotal = total
	if len(chunks) < total && annotated.FallbackReason == SearchFallbackNone {
		annotated.FallbackTriggered = true
		annotated.FallbackReason = SearchFallbackQueryChunksDropped
	}
	return &annotated, err
}

// searchQueryChunks searches the kept chunks of query: one chunk as the query
// itself, several independently, fused with an outer RRF pass. searched is
// how many chunks were searched with their embedding: a chunk whose
// embedding failed or is unusable isn't one.
func searchQueryChunks(
	ctx context.Context,
	query string,
	chunks []string,
	opts *SearchOptions,
	embedQuery EmbedQueryFunc,
	searchQuery SearchQueryFunc,
	errorPolicy ChunkedSearchErrorPolicy,
) (*SearchResponse, int, error) {
	if len(chunks) <= 1 {
		response, err := searchSingleQueryChunk(ctx, query, opts, embedQuery, searchQuery, errorPolicy)
		return response, len(chunks), err
	}
	return searchSeveralQueryChunks(ctx, query, chunks, opts, embedQuery, searchQuery, errorPolicy)
}

// searchSingleQueryChunk searches a query of one chunk as the query itself.
func searchSingleQueryChunk(
	ctx context.Context,
	query string,
	opts *SearchOptions,
	embedQuery EmbedQueryFunc,
	searchQuery SearchQueryFunc,
	errorPolicy ChunkedSearchErrorPolicy,
) (*SearchResponse, error) {
	embedding, err := embedQuery(ctx, query)
	if fatal := fatalChunkedSearchError(ctx, errorPolicy.FatalEmbeddingError, err); fatal != nil {
		return nil, fatal
	}
	fallbackReason := SearchFallbackNone
	if err != nil {
		fallbackReason = SearchFallbackQueryEmbeddingFailed
		logQueryEmbeddingFallback(ctx, errorPolicy.Transport, err)
	} else if len(embedding) == 0 {
		fallbackReason = SearchFallbackQueryEmbeddingUnavailable
	}
	if err == nil && len(embedding) > 0 {
		response, searchErr := searchQuery(ctx, query, embedding, opts)
		if searchErr == nil && response != nil {
			// The public completion boundary checks cancellation on success.
			return response, nil
		}
		if fatal := fatalChunkedSearchError(ctx, errorPolicy.FatalSearchError, searchErr); fatal != nil {
			return nil, fatal
		}
		fallbackReason = SearchFallbackNoHybridResults
		if searchErr != nil {
			fallbackReason = SearchFallbackHybridSearchFailed
		}
	}
	response, searchErr := searchQuery(ctx, query, nil, opts)
	return withSearchFallback(response, fallbackReason), searchErr
}

// searchSeveralQueryChunks searches each chunk with its embedding and fuses
// the results with an outer RRF pass; searched counts the chunks searched.
func searchSeveralQueryChunks(
	ctx context.Context,
	query string,
	chunks []string,
	opts *SearchOptions,
	embedQuery EmbedQueryFunc,
	searchQuery SearchQueryFunc,
	errorPolicy ChunkedSearchErrorPolicy,
) (*SearchResponse, int, error) {
	searched := 0
	// vectorRank and bm25Rank are the best (lowest positive) rank any chunk
	// gave the result by that method; 0 means no chunk found it that way.
	// vectorSimilarity is the best similarity of a chunk that found it by
	// vector, valid while vectorRank is positive.
	type fusedResult struct {
		best       *SearchResult
		score      float64
		vectorRank int
		bm25Rank   int

		vectorSimilarity float64
	}

	chunkOpts := *opts
	chunkOpts.Limit = chunkCandidateLimit(opts.Limit)
	if opts.continuation {
		// Continued queries deepen every chunk, rather than repeatedly searching
		// the same one-shot top-100 prefix.
		chunkOpts.Limit = max(chunkOpts.Limit, opts.Limit)
	}
	exhausted := true
	var (
		fusedIndexes     map[string]int
		fused            []fusedResult
		budgetReached    bool
		fallbackReason   SearchFallbackReason
		embeddingErr     error
		rerankApplied    bool
		rerankOrderOwned bool
		rerankExcluded   map[string]struct{}
	)
	// Multi-chunk fusion creates a new candidate set, so it is a separate
	// canonical outcome owner rather than a rerank consumer. Its explicit
	// contract is: outer-RRF owns aggregate order, rerank is applied when any
	// participating chunk applied it, and any participating fallback remains
	// visible with the first reported reason.
	for _, chunk := range chunks {
		embedding, err := embedQuery(ctx, chunk)
		if fatal := fatalChunkedSearchError(ctx, errorPolicy.FatalEmbeddingError, err); fatal != nil {
			return nil, 0, fatal
		}
		if err != nil || len(embedding) == 0 {
			// Unavailable preparation does not participate in the selected search;
			// the canonical fallback (or other successful chunks) owns its result.
			if err != nil {
				fallbackReason = SearchFallbackQueryEmbeddingFailed
				if embeddingErr == nil {
					embeddingErr = err
				}
			} else if !isQueryEmbeddingFallback(fallbackReason) {
				// Reported over an earlier, benign reason: the chunk wasn't
				// searched by meaning.
				fallbackReason = SearchFallbackQueryEmbeddingUnavailable
			}
			continue
		}
		response, err := searchQuery(ctx, chunk, embedding, &chunkOpts)
		if fatal := fatalChunkedSearchError(ctx, errorPolicy.FatalSearchError, err); fatal != nil {
			return nil, 0, fatal
		}
		if err != nil || response == nil {
			if fallbackReason == SearchFallbackNone {
				fallbackReason = SearchFallbackNoHybridResults
				if err != nil {
					fallbackReason = SearchFallbackHybridSearchFailed
				}
			}
			continue
		}
		searched++
		rerankApplied = rerankApplied || response.rerankApplied
		rerankOrderOwned = rerankOrderOwned || response.rerankOrderOwned
		for id := range response.rerankExcluded {
			if rerankExcluded == nil {
				rerankExcluded = make(map[string]struct{})
			}
			rerankExcluded[id] = struct{}{}
		}
		if response.FallbackReason != SearchFallbackNone && fallbackReason == SearchFallbackNone {
			fallbackReason = response.FallbackReason
		}
		budgetReached = budgetReached || response.CandidateBudgetReached
		exhausted = exhausted && response.RetrievalExhausted
		if fusedIndexes == nil && len(response.Results) > 0 {
			candidatesPerChunk := chunkOpts.Limit
			if len(response.Results) > candidatesPerChunk {
				candidatesPerChunk = len(response.Results)
			}
			capacity := candidatesPerChunk * len(chunks)
			fusedIndexes = make(map[string]int, capacity)
			fused = make([]fusedResult, 0, capacity)
		}
		for rank, orderedResult := range response.Results {
			result := &orderedResult
			id := string(result.NodeID)
			if id == "" {
				// A relationship, or a node known by ID only; a graph
				// search's relationship doesn't merge with a node of its ID.
				id = graphResultKey(result)
			}
			index, exists := fusedIndexes[id]
			if !exists {
				fused = append(fused, fusedResult{best: result})
				index = len(fused)
				fusedIndexes[id] = index
			}
			fusedResult := &fused[index-1]
			if exists && result.Score > fusedResult.best.Score {
				fusedResult.best = result
			}
			fusedResult.score += 1.0 / (outerRRFK + float64(rank+1))
			if result.VectorRank > 0 && (fusedResult.vectorRank == 0 || result.Similarity > fusedResult.vectorSimilarity) {
				// This chunk found it by vector, so its Similarity is a vector score.
				fusedResult.vectorSimilarity = result.Similarity
			}
			fusedResult.vectorRank = bestChunkRank(fusedResult.vectorRank, result.VectorRank)
			fusedResult.bm25Rank = bestChunkRank(fusedResult.bm25Rank, result.BM25Rank)
		}
	}
	if embeddingErr != nil {
		logQueryEmbeddingFallback(ctx, errorPolicy.Transport, embeddingErr)
	}

	slices.SortFunc(fused, func(left, right fusedResult) int {
		if order := cmp.Compare(right.score, left.score); order != 0 {
			return order
		}
		return cmp.Compare(searchResultID(*left.best), searchResultID(*right.best))
	})
	if opts.Limit > 0 && len(fused) > opts.Limit {
		exhausted = false
		fused = fused[:opts.Limit]
	}

	response := &SearchResponse{
		RetrievalExhausted:     exhausted && !budgetReached,
		CandidateBudgetReached: budgetReached,
		Status:                 "success",
		Query:                  query,
		Results:                make([]SearchResult, 0, len(fused)),
		TotalCandidates:        len(fusedIndexes),
		Returned:               len(fused),
		SearchMethod:           "chunked_rrf_hybrid",
		rerankApplied:          rerankApplied,
		rerankOrderOwned:       rerankOrderOwned,
		FallbackTriggered:      false,
		FallbackReason:         fallbackReason,
	}
	if rerankApplied {
		response.SearchMethod += "+rerank"
	}
	if fallbackReason != SearchFallbackNone {
		response.FallbackTriggered = true
	}
	for id := range rerankExcluded {
		if _, included := fusedIndexes[id]; included {
			delete(rerankExcluded, id)
		}
	}
	if len(rerankExcluded) > 0 {
		response.rerankExcluded = rerankExcluded
	}
	if len(fused) == 0 {
		if response.selectionIsAuthoritative() {
			return response, searched, nil
		}
		if opts.FallbackEnabled != nil && !*opts.FallbackEnabled {
			response.FallbackTriggered = false
			response.FallbackReason = SearchFallbackNone
			return response, searched, nil
		}
		fallback, err := searchQuery(ctx, query, nil, opts)
		if fallback != nil {
			// The callback may return a cached response shared with other callers.
			copy := *fallback
			copy.CandidateBudgetReached = budgetReached || fallback.CandidateBudgetReached
			copy.RetrievalExhausted = exhausted && fallback.RetrievalExhausted && !copy.CandidateBudgetReached
			fallback = &copy
		}
		return withSearchFallback(fallback, fallbackReason), searched, err
	}
	for _, fusedResult := range fused {
		result := *fusedResult.best
		result.Score = fusedResult.score
		result.RRFScore = fusedResult.score
		// A chunked result's ranks are its best per-chunk ranks, keeping
		// SearchResult's meaning of 0 (not found by that method); the outer
		// fusion score is not derivable from them alone.
		result.VectorRank = fusedResult.vectorRank
		result.BM25Rank = fusedResult.bm25Rank
		if fusedResult.vectorRank > 0 {
			// The representative chunk may have found it by keyword only;
			// a vector-found result's Similarity is always a vector score.
			result.Similarity = fusedResult.vectorSimilarity
		}
		response.Results = append(response.Results, result)
	}
	return response, searched, nil
}

// bestChunkRank is the better of two per-chunk ranks, where 0 means absent.
func bestChunkRank(best, rank int) int {
	if rank > 0 && (best == 0 || rank < best) {
		return rank
	}
	return best
}

func withSearchFallback(response *SearchResponse, reason SearchFallbackReason) *SearchResponse {
	if response == nil || reason == SearchFallbackNone {
		return response
	}
	copy := *response
	copy.FallbackTriggered = true
	copy.FallbackReason = reason
	return &copy
}

func logQueryEmbeddingFallback(ctx context.Context, transport string, err error) {
	if err == nil {
		return
	}
	transport = strings.TrimSpace(transport)
	if transport == "" {
		transport = "unknown"
	}
	logSearchEvent(ctx, nil, nil, slog.LevelWarn,
		localization.SearchQueryEmbeddingFallbackEvent(
			transport,
			string(SearchFallbackQueryEmbeddingFailed),
			sanitizedEmbeddingDiagnostic(err),
		))
}

func sanitizedEmbeddingDiagnostic(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "embedding provider request timed out"
	case errors.Is(err, context.Canceled):
		return "embedding provider request canceled"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return "embedding provider request timed out"
	}
	if match := embeddingHTTPStatusPattern.FindStringSubmatch(err.Error()); len(match) == 2 {
		return "embedding provider returned HTTP status " + match[1]
	}
	return "embedding provider request failed"
}

func chunkCandidateLimit(limit int) int {
	candidateLimit := limit * 3
	if candidateLimit < minChunkCandidateLimit {
		candidateLimit = minChunkCandidateLimit
	}
	if candidateLimit > maxChunkCandidateLimit {
		candidateLimit = maxChunkCandidateLimit
	}
	return candidateLimit
}

func searchResultID(result SearchResult) string {
	if result.NodeID != "" {
		return string(result.NodeID)
	}
	return result.ID
}

// A DB-owned provider can be canceled while its transport's context is live.
// That cancellation is fatal too. A provider timeout with a live request keeps
// the adapter's ordinary outage/fallback policy.
func fatalChunkedSearchError(ctx context.Context, predicate func(error) bool, err error) error {
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if err != nil && (errors.Is(err, context.Canceled) || predicate != nil && predicate(err)) {
		return err
	}
	return nil
}

// isQueryEmbeddingFallback reports whether reason says the query wasn't
// searched by meaning: its embedding failed or was unavailable, or there is
// no embedder. Such a reason is kept over a later, benign one.
func isQueryEmbeddingFallback(reason SearchFallbackReason) bool {
	switch reason {
	case SearchFallbackQueryEmbeddingFailed, SearchFallbackQueryEmbeddingUnavailable, SearchFallbackNoEmbedder:
		return true
	}
	return false
}
