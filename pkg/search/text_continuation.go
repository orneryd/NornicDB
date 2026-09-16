package search

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type SearchContinuationMode string

const (
	SearchContinuationRanked       SearchContinuationMode = "ranked"
	SearchContinuationRankedThenID SearchContinuationMode = "ranked_then_id"
	SearchContinuationID           SearchContinuationMode = "id"

	SearchContinuationRankedPhase  = "ranked"
	SearchContinuationCatalogPhase = "catalog"

	SearchContinuationMoreResults        = "more_results"
	SearchContinuationCandidateComplete  = "candidate_pool_exhausted"
	SearchContinuationCollectionComplete = "eligible_population_exhausted"
	SearchContinuationMaxResultsComplete = "max_results_reached"

	continuationNonGrowingExpansionLimit = 2
)

// NodeAuthorizationFunc decides whether the current caller may read a node.
// Adapters supply this trusted predicate; it is never accepted from the wire.
type NodeAuthorizationFunc func(*storage.Node) (bool, error)

// SearchContinuationRequest controls one start, pull, or discard operation.
// Limit belongs to SearchOptions and is only the initial retrieval depth.
type SearchContinuationRequest struct {
	Owner         string
	Database      string
	QID           string
	N             int
	Discard       bool
	MaxResults    int
	Mode          SearchContinuationMode
	GroupBy       string
	RankedLimit   int
	AuthorizeNode NodeAuthorizationFunc
}

// SearchContinuationPage is the protocol-neutral continued-search response.
type SearchContinuationPage struct {
	response            *SearchResponse
	Results             []SearchResult
	QID                 string
	HasMore             bool
	Position            uint64
	Returned            int
	Discovered          int
	Total               *uint64
	ExpiresAt           time.Time
	Released            bool
	SearchMethod        string
	FallbackTriggered   bool
	FallbackReason      SearchFallbackReason
	Mode                SearchContinuationMode
	RankedCount         int
	EligibleCount       *int
	RankedPoolExhausted bool
	CollectionExhausted bool
	Completion          string
}

type continuationRegistry interface {
	Start(context.Context, resultstream.Scope, resultstream.Stream, int) (*resultstream.Page, error)
	Pull(context.Context, resultstream.Scope, string, int) (*resultstream.Page, error)
	Discard(resultstream.Scope, string) error
	Close()
}

type cachedTextPreparation struct {
	chunkOnce sync.Once
	chunks    []string
	chunkErr  error

	embedMu sync.Mutex
	embeds  map[string]cachedEmbedding
}

type cachedEmbedding struct {
	vector []float32
	err    error
}

type searchContinuationState struct {
	response          *SearchResponse
	mu                sync.RWMutex
	results           []SearchResult
	seen              map[string]struct{}
	plateau           continuationPlateauTracker
	searchMethod      string
	fallbackTriggered bool
	fallbackReason    SearchFallbackReason
	maxResultsReached bool
}

type continuationPlateauTracker struct {
	lastCount  int
	nonGrowing int
}

func newContinuationPlateauTracker(initialCount int) continuationPlateauTracker {
	return continuationPlateauTracker{lastCount: initialCount}
}

func (t *continuationPlateauTracker) budgetReached(response *SearchResponse) bool {
	if response == nil {
		return false
	}
	resultCount := len(response.Results)
	retrievalExhausted := response.RetrievalExhausted
	candidateBudgetReached := response.CandidateBudgetReached
	if retrievalExhausted || candidateBudgetReached {
		t.lastCount = resultCount
		t.nonGrowing = 0
		return candidateBudgetReached
	}
	if !continuationCanInferCandidateBudget(response.SearchMethod) {
		return false
	}
	if resultCount <= t.lastCount {
		t.nonGrowing++
	} else {
		t.nonGrowing = 0
	}
	t.lastCount = resultCount
	return t.nonGrowing >= continuationNonGrowingExpansionLimit
}

func continuationCanInferCandidateBudget(searchMethod string) bool {
	method := strings.ToLower(searchMethod)
	return strings.Contains(method, "rrf_hybrid") ||
		strings.HasPrefix(method, "vector_hnsw") ||
		strings.HasPrefix(method, "vector_clustered") ||
		strings.HasPrefix(method, "vector_ivf_hnsw") ||
		strings.HasPrefix(method, "vector_ivfpq")
}

func markContinuationCandidateBudget(response *SearchResponse) *SearchResponse {
	if response == nil {
		return nil
	}
	copy := *response
	copy.CandidateBudgetReached = true
	copy.RetrievalExhausted = false
	return &copy
}

// SearchTextContinuation starts or resumes a progressively deepened canonical
// text search. Chunking and embedding execute only while starting the stream.
func (s *Service) SearchTextContinuation(
	ctx context.Context,
	query string,
	opts *SearchOptions,
	request SearchContinuationRequest,
	chunkQuery ChunkQueryFunc,
	embedQuery EmbedQueryFunc,
	searchQuery SearchQueryFunc,
	errorPolicy ChunkedSearchErrorPolicy,
) (*SearchContinuationPage, error) {
	registry, err := s.searchContinuationRegistry()
	if err != nil {
		return nil, err
	}
	database := request.Database
	if database == "" {
		database = s.cacheNamespace
	}
	if database == "" {
		database = "default"
	}
	scope := resultstream.Scope{Owner: request.Owner, Database: database}
	if request.Discard {
		if request.QID == "" {
			return nil, resultstream.ErrInvalidQID
		}
		if err := registry.Discard(scope, request.QID); err != nil {
			return nil, err
		}
		return &SearchContinuationPage{Released: true}, nil
	}
	if request.QID != "" {
		page, err := registry.Pull(ctx, scope, request.QID, request.N)
		if err != nil {
			return nil, err
		}
		return searchPageFromResultStream(page)
	}
	if request.Mode == "" {
		request.Mode = SearchContinuationRanked
	}
	if request.Mode == SearchContinuationID {
		if opts == nil {
			defaults := defaultSearchOptionsValue()
			opts = &defaults
		}
		stream, err := s.newIDContinuationStream(ctx, cloneContinuationSearchOptions(opts), request)
		if err != nil {
			return nil, err
		}
		page, err := registry.Start(ctx, scope, stream, request.N)
		if err != nil {
			return nil, err
		}
		return searchPageFromResultStream(page)
	}
	if request.Mode == SearchContinuationRankedThenID {
		if searchQuery == nil {
			return nil, errors.New("ranked_then_id continuation requires a search function")
		}
		if opts == nil {
			defaults := defaultSearchOptionsValue()
			opts = &defaults
		}
		ownedOptions := cloneContinuationSearchOptions(opts)
		ownedOptions.continuation = true
		rankedLimit := request.RankedLimit
		if ownedOptions.Limit <= 0 {
			ownedOptions.Limit = 50
		}
		preparation := &cachedTextPreparation{embeds: make(map[string]cachedEmbedding)}
		cachedChunks := preparation.chunker(chunkQuery)
		cachedEmbeds := preparation.embedder(embedQuery)
		if rankedLimit > 0 {
			ownedOptions.Limit = rankedLimit
		}
		ranked, err := SearchTextChunksWithErrorPolicy(ctx, query, &ownedOptions, cachedChunks, cachedEmbeds, searchQuery, errorPolicy)
		plateau := newContinuationPlateauTracker(0)
		if err == nil && ranked != nil {
			plateau = newContinuationPlateauTracker(len(ranked.Results))
		}
		for err == nil && rankedLimit <= 0 && !ranked.RetrievalExhausted {
			if ranked.CandidateBudgetReached {
				break
			}
			depthLimit := ownedOptions.MaxCandidateLimit
			if depthLimit > 0 && ownedOptions.Limit >= depthLimit {
				return nil, fmt.Errorf("continuation retrieval depth limit reached: %w", resultstream.ErrCapacity)
			}
			nextLimit := ownedOptions.Limit * 2
			if nextLimit <= ownedOptions.Limit {
				return nil, fmt.Errorf("continuation retrieval depth overflow: %w", resultstream.ErrCapacity)
			}
			if depthLimit > 0 && nextLimit > depthLimit {
				nextLimit = depthLimit
			}
			ownedOptions.Limit = nextLimit
			ranked, err = SearchTextChunksWithErrorPolicy(ctx, query, &ownedOptions, cachedChunks, cachedEmbeds, searchQuery, errorPolicy)
			if err == nil && plateau.budgetReached(ranked) {
				ranked = markContinuationCandidateBudget(ranked)
			}
		}
		if err != nil {
			return nil, err
		}
		stream, err := s.newCompleteContinuationStream(ctx, ownedOptions, request, ranked)
		if err != nil {
			return nil, err
		}
		page, err := registry.Start(ctx, scope, stream, request.N)
		if err != nil {
			return nil, err
		}
		return searchPageFromResultStream(page)
	}
	if request.Mode != SearchContinuationRanked {
		return nil, fmt.Errorf("unsupported continuation mode %q", request.Mode)
	}
	if searchQuery == nil {
		return nil, errors.New("search continuation requires a search function")
	}
	if opts == nil {
		defaults := defaultSearchOptionsValue()
		opts = &defaults
	}
	ownedOptions := cloneContinuationSearchOptions(opts)
	ownedOptions.continuation = true
	if ownedOptions.Limit <= 0 {
		ownedOptions.Limit = 50
	}
	if ownedOptions.Limit < request.N {
		ownedOptions.Limit = request.N
	}
	preparation := &cachedTextPreparation{embeds: make(map[string]cachedEmbedding)}
	cachedChunks := preparation.chunker(chunkQuery)
	cachedEmbeds := preparation.embedder(embedQuery)
	initial, err := SearchTextChunksWithErrorPolicy(ctx, query, &ownedOptions, cachedChunks, cachedEmbeds, searchQuery, errorPolicy)
	if err != nil {
		return nil, err
	}
	maxResults := request.MaxResults
	maxResultsReached := continuationMaxResultsReached(maxResults, len(initial.Results), initial.RetrievalExhausted, initial.CandidateBudgetReached)
	if maxResultsReached {
		if len(initial.Results) > maxResults {
			initial.Results = initial.Results[:maxResults]
		}
	}
	compactResults := make([]SearchResult, len(initial.Results))
	for index := range initial.Results {
		compactResults[index] = compactContinuationResult(initial.Results[index])
	}
	state := &searchContinuationState{
		response:          searchResponseMetadata(initial),
		results:           compactResults,
		seen:              make(map[string]struct{}, len(initial.Results)),
		plateau:           newContinuationPlateauTracker(len(initial.Results)),
		searchMethod:      initial.SearchMethod,
		fallbackTriggered: initial.FallbackTriggered,
		fallbackReason:    initial.FallbackReason,
		maxResultsReached: maxResultsReached,
	}
	for index := range state.results {
		state.seen[searchResultID(state.results[index])] = struct{}{}
	}
	initialRows := continuationRows(state.results)
	initialExhausted := initial.RetrievalExhausted ||
		initial.CandidateBudgetReached ||
		maxResultsReached
	depthLimit := ownedOptions.MaxCandidateLimit
	if depthLimit <= 0 {
		depthLimit = int(^uint(0) >> 1)
	}
	lastDepth := ownedOptions.Limit
	expand := func(expandCtx context.Context, depth int) ([][]any, bool, error) {
		// A candidate budget is not evidence that retrieval is exhausted. Fail
		// explicitly when no deeper supported request can be made. max_results
		// limits emitted members, not how deeply we may search to find them.
		if lastDepth >= depthLimit {
			return nil, false, fmt.Errorf("continuation retrieval depth limit reached: %w", resultstream.ErrCapacity)
		}
		depth = min(depth, depthLimit)
		expandedOptions := cloneContinuationSearchOptions(&ownedOptions)
		expandedOptions.Limit = depth
		response, expandErr := SearchTextChunksWithErrorPolicy(expandCtx, query, &expandedOptions, cachedChunks, cachedEmbeds, searchQuery, errorPolicy)
		if expandErr != nil {
			return nil, false, expandErr
		}
		lastDepth = depth
		state.mu.Lock()
		defer state.mu.Unlock()
		candidateBudgetReached := response.CandidateBudgetReached
		if state.plateau.budgetReached(response) {
			response = markContinuationCandidateBudget(response)
			candidateBudgetReached = true
		}
		state.response = searchResponseMetadata(response)
		state.searchMethod = response.SearchMethod
		state.fallbackTriggered = response.FallbackTriggered
		state.fallbackReason = response.FallbackReason
		for index := range response.Results {
			result := response.Results[index]
			id := searchResultID(result)
			if _, exists := state.seen[id]; exists {
				continue
			}
			state.seen[id] = struct{}{}
			state.results = append(state.results, compactContinuationResult(result))
		}
		if continuationMaxResultsReached(maxResults, len(state.results), response.RetrievalExhausted, candidateBudgetReached) {
			if len(state.results) > maxResults {
				state.results = state.results[:maxResults]
			}
			state.maxResultsReached = true
		}
		exhausted := response.RetrievalExhausted ||
			candidateBudgetReached ||
			state.maxResultsReached
		return continuationRows(state.results), exhausted, nil
	}
	stream, err := resultstream.NewProgressive(initialRows, initialExhausted, ownedOptions.Limit, expand)
	if err != nil {
		return nil, err
	}
	page, err := registry.Start(ctx, scope, &searchMetadataStream{
		Stream: stream, state: state, engine: s.engine, authorizeNode: request.AuthorizeNode,
	}, request.N)
	if err != nil {
		return nil, err
	}
	return searchPageFromResultStream(page)
}

func continuationMaxResultsReached(maxResults, resultCount int, retrievalExhausted, candidateBudgetReached bool) bool {
	return maxResults > 0 &&
		resultCount >= maxResults &&
		!retrievalExhausted &&
		!candidateBudgetReached
}

func (s *Service) searchContinuationRegistry() (continuationRegistry, error) {
	s.continuationMu.Lock()
	defer s.continuationMu.Unlock()
	if s.continuationRegistry != nil {
		return s.continuationRegistry, nil
	}
	registry, err := resultstream.NewRegistry(resultstream.Config{})
	if err != nil {
		return nil, err
	}
	s.continuationRegistry = registry
	s.continuationOwned = true
	return registry, nil
}

// SetContinuationRegistry shares one process registry across database-scoped
// search services. The registry remains owned by the caller.
func (s *Service) SetContinuationRegistry(registry *resultstream.Registry) {
	if s == nil || registry == nil {
		return
	}
	s.mu.RLock()
	metrics := s.metrics
	s.mu.RUnlock()
	s.continuationMu.Lock()
	if s.continuationRegistry != nil && s.continuationOwned {
		s.continuationRegistry.Close()
	}
	s.continuationRegistry = registry
	s.continuationOwned = false
	if metrics != nil {
		registry.SetObserver(newCursorObserver(metrics))
	}
	s.continuationMu.Unlock()
}

func (p *cachedTextPreparation) chunker(chunkQuery ChunkQueryFunc) ChunkQueryFunc {
	if chunkQuery == nil {
		return nil
	}
	return func(ctx context.Context, query string) ([]string, error) {
		p.chunkOnce.Do(func() {
			p.chunks, p.chunkErr = chunkQuery(ctx, query)
			p.chunks = append([]string(nil), p.chunks...)
		})
		return p.chunks, p.chunkErr
	}
}

func (p *cachedTextPreparation) embedder(embedQuery EmbedQueryFunc) EmbedQueryFunc {
	if embedQuery == nil {
		return nil
	}
	return func(ctx context.Context, query string) ([]float32, error) {
		p.embedMu.Lock()
		cached, exists := p.embeds[query]
		p.embedMu.Unlock()
		if exists {
			return cached.vector, cached.err
		}
		vector, err := embedQuery(ctx, query)
		vector = append([]float32(nil), vector...)
		p.embedMu.Lock()
		p.embeds[query] = cachedEmbedding{vector: vector, err: err}
		p.embedMu.Unlock()
		return vector, err
	}
}

type searchMetadataStream struct {
	resultstream.Stream
	state         *searchContinuationState
	engine        storage.Engine
	authorizeNode NodeAuthorizationFunc
}

func (s *searchMetadataStream) Pull(ctx context.Context, position uint64, n int) (*resultstream.Page, error) {
	page, err := s.Stream.Pull(ctx, position, n)
	if err != nil {
		return nil, err
	}
	compact := make([]SearchResult, len(page.Rows))
	for index := range page.Rows {
		if len(page.Rows[index]) != 1 {
			return nil, resultstream.ErrInvalidPosition
		}
		result, ok := page.Rows[index][0].(SearchResult)
		if !ok {
			return nil, resultstream.ErrInvalidPosition
		}
		compact[index] = result
	}
	results, err := hydrateContinuationResults(s.engine, compact, s.authorizeNode)
	if err != nil {
		return nil, err
	}
	page.Rows = continuationRows(results)
	s.state.mu.RLock()
	response := s.state.response
	rankedPoolExhausted := response != nil && response.RetrievalExhausted && !response.CandidateBudgetReached
	collectionExhausted := !page.HasMore && rankedPoolExhausted && !s.state.maxResultsReached
	completion := SearchContinuationMoreResults
	if !page.HasMore {
		switch {
		case s.state.maxResultsReached:
			completion = SearchContinuationMaxResultsComplete
		case rankedPoolExhausted:
			completion = SearchContinuationCollectionComplete
		default:
			completion = SearchContinuationCandidateComplete
		}
	}
	page.Metadata = map[string]any{
		"search_method":         s.state.searchMethod,
		"response":              response,
		"fallback_triggered":    s.state.fallbackTriggered,
		"fallback_reason":       s.state.fallbackReason,
		"discovered":            len(s.state.results),
		"mode":                  SearchContinuationRanked,
		"ranked_count":          len(s.state.results),
		"ranked_pool_exhausted": rankedPoolExhausted,
		"collection_exhausted":  collectionExhausted,
		"completion":            completion,
	}
	s.state.mu.RUnlock()
	return page, nil
}

func (s *searchMetadataStream) RetainedBytes() int64 {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	var retainedBytes int64
	for index := range s.state.results {
		retainedBytes += compactContinuationResultBytes(s.state.results[index])
	}
	return retainedBytes
}

func (s *searchMetadataStream) SetRetainedBytesGrowthGuard(growthOK func() bool) {
	if guarded, ok := s.Stream.(resultstream.RetainedBytesGrowthGuard); ok {
		guarded.SetRetainedBytesGrowthGuard(growthOK)
	}
}

func continuationRows(results []SearchResult) [][]any {
	rows := make([][]any, len(results))
	for index := range results {
		rows[index] = []any{results[index]}
	}
	return rows
}

func searchPageFromResultStream(page *resultstream.Page) (*SearchContinuationPage, error) {
	results := make([]SearchResult, len(page.Rows))
	for index := range page.Rows {
		if len(page.Rows[index]) != 1 {
			return nil, resultstream.ErrInvalidPosition
		}
		result, ok := page.Rows[index][0].(SearchResult)
		if !ok {
			return nil, resultstream.ErrInvalidPosition
		}
		results[index] = result
	}
	out := &SearchContinuationPage{
		Results:   results,
		QID:       page.QID,
		HasMore:   page.HasMore,
		Position:  page.Position,
		Returned:  len(results),
		Total:     page.Total,
		ExpiresAt: page.ExpiresAt,
	}
	if page.Metadata != nil {
		out.response, _ = page.Metadata["response"].(*SearchResponse)
		out.SearchMethod, _ = page.Metadata["search_method"].(string)
		out.FallbackTriggered, _ = page.Metadata["fallback_triggered"].(bool)
		if reason, ok := page.Metadata["fallback_reason"].(SearchFallbackReason); ok {
			out.FallbackReason = reason
		} else if reason, ok := page.Metadata["fallback_reason"].(string); ok {
			out.FallbackReason = SearchFallbackReason(reason)
		}
		out.Discovered, _ = page.Metadata["discovered"].(int)
		if mode, ok := page.Metadata["mode"].(SearchContinuationMode); ok {
			out.Mode = mode
		} else if mode, ok := page.Metadata["mode"].(string); ok {
			out.Mode = SearchContinuationMode(mode)
		}
		out.RankedCount, _ = page.Metadata["ranked_count"].(int)
		out.EligibleCount, _ = page.Metadata["eligible_count"].(*int)
		out.RankedPoolExhausted, _ = page.Metadata["ranked_pool_exhausted"].(bool)
		out.CollectionExhausted, _ = page.Metadata["collection_exhausted"].(bool)
		out.Completion, _ = page.Metadata["completion"].(string)
	}
	return out, nil
}

func cloneContinuationSearchOptions(options *SearchOptions) SearchOptions {
	clone := *options
	clone.Types = append([]string(nil), options.Types...)
	if options.MinSimilarity != nil {
		value := *options.MinSimilarity
		clone.MinSimilarity = &value
	}
	if options.FallbackEnabled != nil {
		value := *options.FallbackEnabled
		clone.FallbackEnabled = &value
	}
	if options.Filters != nil {
		clone.Filters = make(map[string][]string, len(options.Filters))
		for key, values := range options.Filters {
			clone.Filters[key] = append([]string(nil), values...)
		}
	}
	return clone
}

// SearchResponse restores the canonical search response with this page's rows.
func (p *SearchContinuationPage) SearchResponse() *SearchResponse {
	response := searchResponseMetadata(p.response)
	response.Results = p.Results
	response.Returned = p.Returned
	response.SearchMethod = p.SearchMethod
	response.FallbackTriggered = p.FallbackTriggered
	response.FallbackReason = p.FallbackReason
	return response
}

func searchResponseMetadata(response *SearchResponse) *SearchResponse {
	if response == nil {
		return &SearchResponse{}
	}
	clone := *response
	clone.Results = nil
	return &clone
}
