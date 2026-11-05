package search

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/localization"
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
	// MaxPageSize is the effective registry limit, advertised on page responses.
	MaxPageSize int

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
	// QueryChunksSearched / QueryChunksTotal are SearchResponse's.
	QueryChunksSearched int
	QueryChunksTotal    int
}

type continuationRegistry interface {
	Start(context.Context, resultstream.Scope, resultstream.Stream, int) (*resultstream.Page, error)
	Pull(context.Context, resultstream.Scope, string, int) (*resultstream.Page, error)
	Discard(resultstream.Scope, string) error
	Close()
	MaxPageSize() int
}

type cachedTextPreparation struct {
	chunkOnce lifecycle.OnceCompletion
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
	searchMethod      string
	fallbackTriggered bool
	fallbackReason    SearchFallbackReason
	execution         *continuationExecution
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
) (out *SearchContinuationPage, err error) {
	registry, err := s.searchContinuationRegistry()
	if err != nil {
		return nil, err
	}
	defer func() {
		if out != nil && !out.Released {
			out.MaxPageSize = registry.MaxPageSize()
		}
	}()
	database := request.Database
	if database == "" {
		database = s.continuationDatabase()
	}
	scope := resultstream.Scope{Owner: request.Owner, Database: database}
	if binder, ok := registry.(interface {
		BindScope(resultstream.Scope) resultstream.Scope
	}); ok {
		scope = binder.BindScope(scope)
	}
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
	// Grouping needs the whole ranked phase before the first page (a later
	// discovery can replace a group's best passage), which the complete
	// modes build; ranked mode pages hits as they are found, so a group_by
	// there is refused, not ignored.
	if request.GroupBy != "" && request.Mode == SearchContinuationRanked {
		return nil, localizedError(localization.SearchGroupByRankedMode(), nil)
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
	if request.Mode != SearchContinuationRanked && request.Mode != SearchContinuationRankedThenID {
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
	if request.Mode == SearchContinuationRanked {
		ownedOptions.Limit = max(ownedOptions.Limit, request.N)
	}
	if request.Mode == SearchContinuationRankedThenID && request.RankedLimit > 0 {
		ownedOptions.Limit = request.RankedLimit
	}
	execution := newContinuationExecution(ownedOptions, request)
	preparation := &cachedTextPreparation{embeds: make(map[string]cachedEmbedding)}
	cachedChunks, cachedEmbeds := preparation.chunker(chunkQuery), preparation.embedder(embedQuery)
	rerankMemo := newRerankMemo()
	// Every initial or expanded search reports its actual issued depth to the
	// same owner. Public adapters only start/pull this service-owned stream.
	searchStep := func(stepCtx context.Context, requested int) (*SearchResponse, continuationStep, error) {
		step, err := execution.issue(stepCtx, requested, s.EmbeddingCount()+s.FulltextDocCount())
		if err != nil {
			return nil, continuationStep{}, err
		}
		stepOptions := cloneContinuationSearchOptions(&ownedOptions)
		stepOptions.Limit = step.depth
		response, err := SearchTextChunksWithErrorPolicy(withRerankMemo(stepCtx, rerankMemo), query, &stepOptions, cachedChunks, cachedEmbeds, searchQuery, errorPolicy)
		if err != nil {
			err = execution.observe(stepCtx, step, nil, 0, err)
		}
		return response, step, err
	}
	initial, initialStep, err := searchStep(ctx, ownedOptions.Limit)
	if err != nil {
		return nil, err
	}
	if err := execution.observe(ctx, initialStep, initial, len(initial.Results), nil); err != nil {
		return nil, err
	}
	if request.Mode == SearchContinuationRankedThenID {
		ranked := initial
		for !execution.status.terminal {
			var step continuationStep
			ranked, step, err = searchStep(ctx, execution.completed.depth)
			if err != nil {
				return nil, err
			}
			if err = execution.observe(ctx, step, ranked, len(ranked.Results), nil); err != nil {
				return nil, err
			}
		}
		stream, err := s.newCompleteContinuationStream(ctx, ownedOptions, request, execution.response(ranked), execution)
		if err != nil {
			return nil, err
		}
		page, err := registry.Start(ctx, scope, stream, request.N)
		if err != nil {
			return nil, err
		}
		return searchPageFromResultStream(page)
	}
	initialResults := initial.Results[:execution.resultCount]
	compactResults := make([]SearchResult, len(initialResults))
	for index := range initialResults {
		compactResults[index] = compactContinuationResult(initialResults[index])
	}
	state := &searchContinuationState{
		response: searchResponseMetadata(execution.response(initial)), results: compactResults,
		seen: make(map[string]struct{}, len(initialResults)), execution: execution,
		searchMethod: initial.SearchMethod, fallbackTriggered: initial.FallbackTriggered, fallbackReason: initial.FallbackReason,
	}
	for index := range state.results {
		state.seen[searchResultID(state.results[index])] = struct{}{}
	}
	initialRows := continuationRows(state.results)
	rows := initialRows
	expand := func(expandCtx context.Context, requested int) ([][]any, bool, error) {
		state.mu.Lock()
		defer state.mu.Unlock()
		response, step, err := searchStep(expandCtx, requested)
		if err != nil {
			return nil, false, err
		}
		work := newSearchBatch(expandCtx)
		for index := range response.Results {
			if work.boundary() {
				if err := work.check(); err != nil {
					return nil, false, err
				}
			}
			result := response.Results[index]
			id := searchResultID(result)
			if _, exists := state.seen[id]; exists {
				continue
			}
			state.seen[id] = struct{}{}
			state.results = append(state.results, compactContinuationResult(result))
			rows = append(rows, []any{state.results[len(state.results)-1]})
		}
		if err := execution.observe(expandCtx, step, response, len(state.results), nil); err != nil {
			return nil, false, err
		}
		state.results, rows = state.results[:execution.resultCount], rows[:execution.resultCount]
		state.response = searchResponseMetadata(execution.response(response))
		state.searchMethod, state.fallbackTriggered, state.fallbackReason = response.SearchMethod, response.FallbackTriggered, response.FallbackReason
		return rows, execution.status.terminal, nil
	}
	stream, err := resultstream.NewProgressive(initialRows, execution.status.terminal, execution.completed.depth, expand)
	if err != nil {
		return nil, err
	}
	page, err := registry.Start(ctx, scope, &searchMetadataStream{
		Stream: stream, state: state, engine: s.engine, authorizeNode: request.AuthorizeNode,
		projection: newResultPropertyProjection(ownedOptions.IncludeProperties, ownedOptions.ExcludeProperties),
	}, request.N)
	if err != nil {
		return nil, err
	}
	return searchPageFromResultStream(page)
}

func (s *Service) searchContinuationRegistry() (continuationRegistry, error) {
	s.continuationMu.Lock()
	defer s.continuationMu.Unlock()
	if s.continuationRegistry != nil {
		return s.continuationRegistry, nil
	}
	registry, err := resultstream.NewRegistry(resultstream.Config{MaxPageSize: s.continuationMaxPageSize})
	if err != nil {
		return nil, err
	}
	s.continuationRegistry = registry.ForDatabase(s.continuationDatabase())
	s.continuationOwned = true
	return s.continuationRegistry, nil
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
	s.continuationRegistry = registry.ForDatabase(s.continuationDatabase())
	s.continuationOwned = false
	if metrics != nil {
		registry.SetObserver(newCursorObserver(metrics))
	}
	s.continuationMu.Unlock()
}

func (s *Service) continuationDatabase() string {
	if s.cacheNamespace != "" {
		return s.cacheNamespace
	}
	return "default"
}

func (p *cachedTextPreparation) chunker(chunkQuery ChunkQueryFunc) ChunkQueryFunc {
	if chunkQuery == nil {
		return nil
	}
	return func(ctx context.Context, query string) ([]string, error) {
		p.chunkOnce.Run(func() {
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
	projection    resultPropertyProjection
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
	results, err := hydrateContinuationResultsContext(ctx, s.engine, compact, s.authorizeNode, s.projection)
	if err != nil {
		return nil, err
	}
	page.Rows = continuationRows(results)
	s.state.mu.RLock()
	response := s.state.response
	rankedPoolExhausted, collectionExhausted, completion := s.state.execution.status.page(page.HasMore)
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
		if out.response != nil {
			out.QueryChunksSearched, out.QueryChunksTotal = out.response.QueryChunksSearched, out.response.QueryChunksTotal
		}
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
	clone.IncludeProperties = append([]string(nil), options.IncludeProperties...)
	clone.ExcludeProperties = append([]string(nil), options.ExcludeProperties...)
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
	response.Results = append([]SearchResult(nil), p.Results...)
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
