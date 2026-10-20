package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) callDbRetrieve(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	req, err := ragProcedureRequest("db.retrieve", args)
	if err != nil {
		return nil, err
	}
	return e.runSearchRequest(ctx, req, false, false)
}

func (e *StorageExecutor) callDbRRetrieve(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	req, err := ragProcedureRequest("db.rretrieve", args)
	if err != nil {
		return nil, err
	}
	return e.runSearchRequest(ctx, req, false, true)
}

func (e *StorageExecutor) callDbRerank(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	req, err := ragProcedureRequest("db.rerank", args)
	if err != nil {
		return nil, err
	}

	query := stringOr(req["query"], stringOr(req["text"], ""))
	candidates, err := parseRerankCandidates(firstPresent(req, "candidates", "rows", "results"))
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, localizedError(localization.CypherSubqueriesRAGCandidatesRequired(), nil)
	}

	opts := search.DefaultSearchOptions()
	if v, ok := toInt(firstPresent(req, "rerankTopK", "rerank_top_k")); ok && v > 0 {
		opts.RerankTopK = v
	}
	if v, ok := ragToFloat64(firstPresent(req, "rerankMinScore", "rerank_min_score")); ok {
		opts.RerankMinScore = v
	}

	svc := e.search()
	if svc == nil {
		svc = search.NewService(e.storage)
		e.keepUnmanagedSearchService(svc)
	}
	reranked, err := svc.RerankCandidates(ctx, query, candidates, opts)
	if err != nil {
		return nil, err
	}

	result := &ExecuteResult{
		Columns: []string{"id", "content", "original_rank", "new_rank", "bi_score", "cross_score", "final_score"},
		Rows:    make([][]interface{}, 0, len(reranked)),
	}
	for _, r := range reranked {
		result.Rows = append(result.Rows, []interface{}{
			r.ID,
			r.Content,
			r.OriginalRank,
			r.NewRank,
			r.BiScore,
			r.CrossScore,
			r.FinalScore,
		})
	}
	return result, nil
}

func (e *StorageExecutor) callDbInfer(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	req, err := ragProcedureRequest("db.infer", args)
	if err != nil {
		return nil, err
	}
	manager := e.GetInferenceManager()
	if manager == nil {
		return nil, localizedError(localization.CypherSubqueriesInferenceManagerUnavailable(), nil)
	}

	start := time.Now()
	params := heimdall.DefaultGenerateParams()
	if v, ok := toInt(req["max_tokens"]); ok && v > 0 {
		params.MaxTokens = v
	}
	if v, ok := toFloat32(req["temperature"]); ok {
		params.Temperature = v
	}
	if v, ok := toFloat32(req["top_p"]); ok {
		params.TopP = v
	}
	if v, ok := toInt(req["top_k"]); ok && v > 0 {
		params.TopK = v
	}
	if stops := toStringSlice(req["stop_tokens"]); len(stops) > 0 {
		params.StopTokens = stops
	}

	var (
		text         string
		model        string
		usage        map[string]interface{}
		finishReason = "stop"
		structured   interface{}
	)

	if messagesRaw, ok := req["messages"]; ok {
		msgs := toChatMessages(messagesRaw)
		if len(msgs) == 0 {
			return nil, localizedError(localization.CypherSubqueriesInferMessagesEmpty(), nil)
		}
		chatReq := heimdall.ChatRequest{
			Model:       stringOr(req["model"], ""),
			Messages:    msgs,
			MaxTokens:   params.MaxTokens,
			Temperature: params.Temperature,
			TopP:        params.TopP,
		}
		resp, chatErr := manager.Chat(ctx, chatReq)
		if chatErr != nil {
			return nil, chatErr
		}
		if resp != nil {
			model = resp.Model
			if len(resp.Choices) > 0 {
				if resp.Choices[0].Message != nil {
					text = resp.Choices[0].Message.Content
				}
				if strings.TrimSpace(resp.Choices[0].FinishReason) != "" {
					finishReason = resp.Choices[0].FinishReason
				}
			}
			if resp.Usage != nil {
				usage = map[string]interface{}{
					"prompt_tokens":     resp.Usage.PromptTokens,
					"completion_tokens": resp.Usage.CompletionTokens,
					"total_tokens":      resp.Usage.TotalTokens,
				}
			}
		}
	} else {
		prompt := stringOr(req["prompt"], stringOr(req["query"], ""))
		if strings.TrimSpace(prompt) == "" {
			return nil, localizedError(localization.CypherSubqueriesInferPromptOrMessagesRequired(), nil)
		}
		text, err = manager.Generate(ctx, prompt, params)
		if err != nil {
			return nil, err
		}
		model = stringOr(req["model"], "")
	}

	if json.Unmarshal([]byte(text), &structured) != nil {
		structured = nil
	}

	return &ExecuteResult{
		Columns: []string{"text", "structured", "model", "usage", "latencyMs", "finishReason"},
		Rows: [][]interface{}{{
			text,
			structured,
			model,
			usage,
			time.Since(start).Milliseconds(),
			finishReason,
		}},
	}, nil
}

func (e *StorageExecutor) runSearchRequest(ctx context.Context, req map[string]interface{}, forceRerank bool, useConfiguredRerank bool) (*ExecuteResult, error) {
	query := stringOr(req["query"], stringOr(req["text"], ""))
	continuation := readRetrievalContinuation(req)
	qid, discard := continuation.qid, continuation.discard
	n, nPresent := continuation.n, continuation.nPresent
	mode, groupBy := continuation.mode, continuation.groupBy
	rankedLimit, rankedLimitPresent := continuation.rankedLimit, continuation.rankedLimitPresent
	continuationRequested := continuation.requested()
	if qid != "" && !nPresent {
		n = 50
	}
	if continuationRequested && !discard && n <= 0 {
		return nil, fmt.Errorf("n must be positive")
	}
	if strings.TrimSpace(query) == "" && qid == "" && mode != search.SearchContinuationID {
		return nil, localizedError(localization.CypherSubqueriesQueryRequired(), nil)
	}

	opts := search.GetAdaptiveRRFConfig(query)
	if limit, ok := toInt(req["limit"]); ok && limit > 0 {
		opts.Limit = limit
	}
	// db.retrieveRelationships searches relationships instead of nodes (#1065).
	if relationships, _ := req[relationshipSearchRequestKey].(bool); relationships {
		if continuationRequested {
			return nil, localizedError(localization.SearchRelationshipsContinuation(), nil)
		}
		opts.Relationships = true
	}
	// db.retrieveGraph ranks nodes and relationships together; n or
	// maxResults ask for one terminal page.
	graphPage := false
	if graph, _ := req[graphSearchRequestKey].(bool); graph {
		if qid != "" || discard || mode != "" || groupBy != "" || rankedLimitPresent {
			return nil, localizedError(localization.SearchGraphContinuation(), nil)
		}
		opts.Graph = true
		opts.RelationshipTypes = toStringSlice(firstPresent(req, "relationshipTypes", "relationship_types"))
		if nPresent {
			graphPage, opts.Limit = true, n
		}
		if maxResults, ok := toInt(firstPresent(req, "maxResults", "max_results")); ok && maxResults > 0 {
			graphPage = true
			if !nPresent || maxResults < opts.Limit {
				opts.Limit = maxResults
			}
		}
		continuationRequested = false
	}
	failClosed, err := applyRetrievalPolicyOptions(opts, req)
	if err != nil {
		return nil, err
	}
	if types := toStringSlice(firstPresent(req, "types", "labels")); len(types) > 0 {
		opts.Types = types
	}
	// where: a Cypher predicate on each candidate, bound as node, with the
	// statement's parameters: a missing property, a negation or a pattern
	// from the node. db.retrieveGraph and db.retrieveRelationships check it on
	// relationship candidates too, bound as relationship with endpoints start
	// and end. It is checked before the
	// limit; later continuation pages check it too, so it keeps the
	// statement's values but not its cancellation.
	if raw, present := req["where"]; present && raw != nil {
		where, isText := raw.(string)
		if !isText {
			return nil, localizedError(localization.CypherSubqueriesWhereFilterInvalid("db.retrieve"), nil)
		}
		if strings.TrimSpace(where) != "" {
			where, err := desugarRetrievalWhere(ctx, where)
			if err != nil {
				return nil, err
			}
			opts.Where = e.compileNodeWhereFilter(context.WithoutCancel(ctx), "node", where)
			if opts.Graph || opts.Relationships {
				// One predicate serves both kinds: a node candidate reads
				// relationship, start and end as null, a relationship
				// candidate reads node as null.
				whereCtx := context.WithoutCancel(ctx)
				nodeScope := valueBindingsLayer(whereCtx, 3)
				nodeScope["relationship"], nodeScope["start"], nodeScope["end"] = nil, nil, nil
				opts.Where = e.compileNodeWhereFilter(withValueBindings(whereCtx, nodeScope), "node", where)
				opts.RelationshipWhere = e.compileRelationshipWhereFilter(whereCtx, where)
			}
		}
	}
	if minSim, ok := ragToFloat64(firstPresent(req, "minSimilarity", "min_similarity")); ok && (!failClosed || isFinite(minSim)) {
		opts.MinSimilarity = &minSim
	} else if failClosed {
		if _, present := policyPresent(req, "minSimilarity", "min_similarity"); present {
			return nil, localizedError(localization.CypherSubqueriesRAGFailClosedInvalid("minSimilarity"), nil)
		}
	}
	if forceRerank {
		opts.RerankEnabled = true
	}
	if v, ok := toInt(firstPresent(req, "rerankTopK", "rerank_top_k")); ok && v > 0 {
		opts.RerankTopK = v
	}
	if v, ok := ragToFloat64(firstPresent(req, "rerankMinScore", "rerank_min_score")); ok {
		opts.RerankMinScore = v
	}

	embedding, suppliedEmbedding, err := resolveSuppliedRetrieveEmbedding(req, failClosed)
	if err != nil {
		return nil, err
	}

	svc := e.search()
	ensureSearchService := func(queryEmbedding []float32) *search.Service {
		if svc == nil {
			dims := search.DefaultVectorDimensions
			if len(queryEmbedding) > 0 {
				dims = len(queryEmbedding)
			}
			svc = search.NewServiceWithDimensions(e.storage, dims)
			e.keepUnmanagedSearchService(svc)
		}
		return svc
	}
	continuationRequest := search.SearchContinuationRequest{
		Owner:    searchContinuationOwner(ctx),
		Database: GetUseDatabaseFromContext(ctx),
		QID:      qid,
		N:        n,
		Discard:  discard,
		Mode:     mode,
		GroupBy:  groupBy,
	}
	if rankedLimit > 0 {
		continuationRequest.RankedLimit = rankedLimit
	}
	if maxResults, ok := toInt(firstPresent(req, "maxResults", "max_results")); ok && maxResults > 0 {
		continuationRequest.MaxResults = maxResults
	}
	if qid != "" {
		page, continuationErr := ensureSearchService(nil).SearchTextContinuation(
			ctx, "", nil, continuationRequest, nil, nil, nil, search.ChunkedSearchErrorPolicy{},
		)
		if continuationErr != nil {
			return nil, continuationErr
		}
		return executeSearchContinuationPage(page), nil
	}
	if mode == search.SearchContinuationID {
		page, continuationErr := ensureSearchService(nil).SearchTextContinuation(
			ctx, "", opts, continuationRequest, nil, nil, nil, search.ChunkedSearchErrorPolicy{},
		)
		if continuationErr != nil {
			return nil, continuationErr
		}
		return executeSearchContinuationPage(page), nil
	}
	if useConfiguredRerank && svc != nil {
		opts.RerankEnabled = svc.RerankerAvailable(ctx)
	}

	var response *search.SearchResponse
	if suppliedEmbedding {
		service := ensureSearchService(embedding)
		if continuationRequested {
			page, continuationErr := service.SearchTextContinuation(
				ctx,
				query,
				opts,
				continuationRequest,
				nil,
				func(context.Context, string) ([]float32, error) { return embedding, nil },
				service.Search,
				search.ChunkedSearchErrorPolicy{},
			)
			if continuationErr != nil {
				return nil, continuationErr
			}
			return executeSearchContinuationPage(page), nil
		}
		response, err = service.Search(ctx, query, embedding, opts)
	} else {
		embedder := e.queryEmbedderForStatement(ctx)
		if embedder != nil {
			var finish func()
			var err error
			ctx, finish, err = e.queryEmbedder.BeginUse(ctx)
			if err != nil {
				return nil, err
			}
			defer finish()
		}
		if embedder == nil && failClosed {
			return nil, failClosedEmbeddingUnavailable(localizedError(localization.CypherCoreEmbedderNotConfigured(), nil))
		}

		var embedQuery search.EmbedQueryFunc
		if embedder != nil {
			embedQuery = func(ctx context.Context, chunk string) ([]float32, error) {
				embedding, embedErr := embedder.Embed(ctx, chunk)
				if embedErr != nil {
					if failClosed {
						return nil, failClosedEmbeddingUnavailable(embedErr)
					}
					return nil, embedErr
				}
				// An empty, non-finite or wrong-dimension vector is a failed
				// embedding (query_embedding_failed), not a missing one.
				if dimensions := queryEmbedderDimensions(embedder); !usableEmbedding(embedding) || dimensions > 0 && len(embedding) != dimensions {
					unusable := localizedError(localization.CypherCoreEmbeddingNoOutput(), nil)
					if failClosed {
						return nil, failClosedEmbeddingUnavailable(unusable)
					}
					return nil, unusable
				}
				return embedding, nil
			}
		}

		searchQuery := func(ctx context.Context, text string, queryEmbedding []float32, searchOpts *search.SearchOptions) (*search.SearchResponse, error) {
			service := ensureSearchService(queryEmbedding)
			if useConfiguredRerank {
				searchOpts.RerankEnabled = service.RerankerAvailable(ctx)
			}
			return service.Search(ctx, text, queryEmbedding, searchOpts)
		}
		chunkQuery := func(_ context.Context, text string) ([]string, error) {
			return embedder.ChunkText(text, 512, 50)
		}
		// failClosed disables every fallback: an embedding failure or a
		// failed hybrid search is the procedure's error, never an empty or
		// text-only result that reads as "nothing matched".
		errorPolicy := search.ChunkedSearchErrorPolicy{
			Transport:           "cypher",
			FatalEmbeddingError: func(error) bool { return failClosed },
			FatalSearchError:    func(error) bool { return failClosed },
		}
		if continuationRequested {
			page, continuationErr := ensureSearchService(embedding).SearchTextContinuation(
				ctx, query, opts, continuationRequest, chunkQuery, embedQuery, searchQuery, errorPolicy,
			)
			if continuationErr != nil {
				return nil, continuationErr
			}
			return executeSearchContinuationPage(page), nil
		}
		response, err = search.SearchTextChunksWithErrorPolicy(
			ctx,
			query,
			opts,
			chunkQuery,
			embedQuery,
			searchQuery,
			errorPolicy,
		)
	}
	if err != nil {
		return nil, err
	}
	if opts.Graph {
		if graphPage {
			return e.graphSearchPage(response), nil
		}
		return e.graphSearchResult(response), nil
	}
	if opts.Relationships {
		return e.relationshipSearchResult(response), nil
	}

	result := &ExecuteResult{
		Columns: []string{"node", "score", "rrf_score", "vector_rank", "bm25_rank", "search_method", "fallback_triggered", "fallback_reason", "query_chunks_searched", "query_chunks_total", "matches"},
		Rows:    make([][]interface{}, 0, len(response.Results)),
	}
	// Of a query split into chunks, how many were searched; 0 and 0 for a
	// query searched whole. Reported whatever the fallback reason (#907).
	chunksSearched, chunksTotal := int64(response.QueryChunksSearched), int64(response.QueryChunksTotal)

	for _, r := range response.Results {
		node := &storage.Node{
			ID:         storage.NodeID(r.ID),
			Labels:     r.Labels,
			Properties: r.Properties,
		}
		result.Rows = append(result.Rows, []interface{}{
			node,
			r.Score,
			r.RRFScore,
			r.VectorRank,
			r.BM25Rank,
			response.SearchMethod,
			response.FallbackTriggered,
			string(response.FallbackReason),
			chunksSearched,
			chunksTotal,
			search.SearchMatchValues(r.Matches),
		})
	}

	return result, nil
}

func executeSearchContinuationPage(page *search.SearchContinuationPage) *ExecuteResult {
	results := make([]interface{}, 0, len(page.Results))
	for index := range page.Results {
		result := page.Results[index]
		mapped := map[string]interface{}{
			"node":  &storage.Node{ID: storage.NodeID(result.ID), Labels: result.Labels, Properties: result.Properties},
			"score": result.Score, "rrf_score": result.RRFScore,
			"vector_rank": int64(result.VectorRank), "bm25_rank": int64(result.BM25Rank),
			"group_key": result.GroupKey, "phase": result.Phase,
			"matches": search.SearchMatchValues(result.Matches),
		}
		for key, value := range continuationResultMetadata(result) {
			mapped[key] = value
		}
		if len(result.Passages) > 0 {
			passages := make([]interface{}, len(result.Passages))
			for passageIndex := range result.Passages {
				passage := result.Passages[passageIndex]
				passages[passageIndex] = map[string]interface{}{
					"node":  &storage.Node{ID: storage.NodeID(passage.ID), Labels: passage.Labels, Properties: passage.Properties},
					"score": passage.Score, "rrf_score": passage.RRFScore,
					"vector_rank": int64(passage.VectorRank), "bm25_rank": int64(passage.BM25Rank),
					"phase":   passage.Phase,
					"matches": search.SearchMatchValues(passage.Matches),
				}
				for key, value := range continuationResultMetadata(passage) {
					passages[passageIndex].(map[string]interface{})[key] = value
				}
			}
			mapped["passages"] = passages
		}
		results = append(results, mapped)
	}
	var total interface{}
	if page.Total != nil {
		total = int64(*page.Total)
	}
	var eligibleCount interface{}
	if page.EligibleCount != nil {
		eligibleCount = int64(*page.EligibleCount)
	}
	pageMap := map[string]interface{}{
		"results": results, "qid": page.QID, "has_more": page.HasMore,
		"position": int64(page.Position), "returned": int64(page.Returned),
		"discovered": int64(page.Discovered), "total": total,
		"expires_at": page.ExpiresAt.UTC().Format(time.RFC3339Nano),
		"released":   page.Released, "search_method": page.SearchMethod,
		"fallback_triggered": page.FallbackTriggered,
		"fallback_reason":    string(page.FallbackReason),
		"mode":               string(page.Mode), "ranked_count": int64(page.RankedCount),
		"eligible_count": eligibleCount, "ranked_pool_exhausted": page.RankedPoolExhausted,
		"collection_exhausted": page.CollectionExhausted, "completion": page.Completion,
		"max_page_size": int64(page.MaxPageSize),
	}
	if page.QueryChunksTotal > 0 {
		pageMap["query_chunks_searched"] = int64(page.QueryChunksSearched)
		pageMap["query_chunks_total"] = int64(page.QueryChunksTotal)
	}
	result := &ExecuteResult{
		Columns: []string{"page"},
		Rows:    [][]interface{}{{pageMap}},
	}
	metadata := continuationResultMetadata(page.SearchResponse())
	for key, value := range metadata {
		result.Rows[0][0].(map[string]interface{})[key] = value
	}
	result.Metadata = metadata
	if page.QID != "" {
		if result.Metadata == nil {
			result.Metadata = make(map[string]interface{})
		}
		result.Metadata["durable_qid"] = page.QID
	}
	return result
}

func searchContinuationOwner(ctx context.Context) string {
	if principal := strings.TrimSpace(GetAuthenticatedPrincipalFromContext(ctx)); principal != "" {
		return principal
	}
	if strings.TrimSpace(GetAuthTokenFromContext(ctx)) != "" {
		return ""
	}
	return "embedded"
}

func applyRetrievalPolicyOptions(opts *search.SearchOptions, req map[string]interface{}) (bool, error) {
	failClosed, err := parseFailClosed(req)
	if err != nil {
		return false, err
	}
	applyAdaptiveCandidateOptions(opts, req)
	// matches: false skips each hit's matches (#1060); they are on by default.
	if value, ok := req["matches"].(bool); ok {
		opts.SkipMatches = !value
	}
	if value, ok := ragToFloat64(firstPresent(req, "rrfK", "rrf_k")); ok && value > 0 && isFinite(value) {
		opts.RRFK = value
	}
	if value, ok := ragToFloat64(firstPresent(req, "vectorWeight", "vector_weight")); ok && value > 0 && isFinite(value) {
		opts.VectorWeight = value
	}
	if value, ok := ragToFloat64(firstPresent(req, "bm25Weight", "bm25_weight")); ok && value > 0 && isFinite(value) {
		opts.BM25Weight = value
	}
	if value, ok := ragToFloat64(firstPresent(req, "minRRFScore", "min_rrf_score")); ok && value >= 0 && isFinite(value) {
		opts.MinRRFScore = value
	}
	if value, ok := toBool(firstPresent(req, "fallbackEnabled", "fallback_enabled")); ok {
		opts.FallbackEnabled = &value
	}
	opts.Filters = parseRetrievalFilters(firstPresent(req, "filters", "propertyFilters", "property_filters"))
	if failClosed {
		if err := validateFailClosedNumericPolicy(req); err != nil {
			return true, err
		}
		disabled := false
		opts.FallbackEnabled = &disabled
	}
	return failClosed, nil
}

func parseFailClosed(req map[string]interface{}) (bool, error) {
	raw, present := policyPresent(req, "failClosed", "fail_closed")
	if !present {
		return false, nil
	}
	value, ok := toBool(raw)
	if !ok {
		return false, localizedError(localization.CypherSubqueriesRAGFailClosedInvalid("failClosed"), nil)
	}
	return value, nil
}

var (
	errRetrieveEmbeddingNotAVector = errors.New("query embedding must be a numeric vector")
	errRetrieveEmbeddingInvalid    = errors.New("query embedding is empty or non-finite")
)

func resolveSuppliedRetrieveEmbedding(req map[string]interface{}, failClosed bool) ([]float32, bool, error) {
	raw, present := policyPresent(req, "embedding", "queryEmbedding", "query_embedding")
	if !present {
		return nil, false, nil
	}
	if _, isString := raw.(string); isString {
		if failClosed {
			return nil, false, failClosedEmbeddingUnavailable(errRetrieveEmbeddingNotAVector)
		}
		return nil, false, nil
	}
	embedding, err := parseRetrieveEmbedding(raw, failClosed)
	if err != nil {
		return nil, false, failClosedEmbeddingUnavailable(err)
	}
	if usableEmbedding(embedding) {
		return embedding, true, nil
	}
	if failClosed {
		return nil, false, failClosedEmbeddingUnavailable(errRetrieveEmbeddingInvalid)
	}
	return nil, false, nil
}

func failClosedEmbeddingUnavailable(cause error) error {
	return localizedError(localization.CypherSubqueriesRAGFailClosedEmbeddingUnavailable(cause), cause)
}

func parseRetrieveEmbedding(raw interface{}, failClosed bool) ([]float32, error) {
	if !failClosed {
		return toFloat32Slice(raw), nil
	}
	return strictNumericEmbedding(raw)
}

func strictNumericEmbedding(raw interface{}) ([]float32, error) {
	list, ok := valueconvert.ReadList(raw)
	if !ok {
		return nil, errRetrieveEmbeddingNotAVector
	}
	values, issue := list.Numbers32()
	if issue != nil {
		return nil, errRetrieveEmbeddingNotAVector
	}
	return values, nil
}

// queryEmbedderDimensions is the embedder's vector dimension when it reports
// one, 0 (unchecked) otherwise.
func queryEmbedderDimensions(embedder QueryEmbedder) int {
	if sized, ok := embedder.(interface{ Dimensions() int }); ok {
		return sized.Dimensions()
	}
	return 0
}

func usableEmbedding(values []float32) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return false
		}
	}
	return true
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func isIntegral(value float64) bool {
	return isFinite(value) && value == math.Trunc(value)
}

func validateFailClosedNumericPolicy(req map[string]interface{}) error {
	inCandidateRange := func(v float64) bool {
		return isIntegral(v) && v >= 1 && v <= float64(int(^uint(0)>>1))
	}
	checks := []struct {
		field string
		keys  []string
		ok    func(float64) bool
	}{
		{"rrfK", []string{"rrfK", "rrf_k"}, func(v float64) bool { return isFinite(v) && v > 0 && v <= 10000 }},
		{"vectorWeight", []string{"vectorWeight", "vector_weight"}, func(v float64) bool { return isFinite(v) && v > 0 && v <= 100 }},
		{"bm25Weight", []string{"bm25Weight", "bm25_weight"}, func(v float64) bool { return isFinite(v) && v > 0 && v <= 100 }},
		{"minRRFScore", []string{"minRRFScore", "min_rrf_score"}, func(v float64) bool { return isFinite(v) && v >= 0 && v <= 1 }},
		{"minSimilarity", []string{"minSimilarity", "min_similarity"}, func(v float64) bool { return isFinite(v) && v >= -1 && v <= 1 }},
		{"initialOverfetchRatio", []string{"initialOverfetchRatio", "initial_overfetch_ratio"}, func(v float64) bool { return isFinite(v) && v >= 1 && v <= 100 }},
		{"maxOverfetchRatio", []string{"maxOverfetchRatio", "max_overfetch_ratio"}, func(v float64) bool { return isFinite(v) && v >= 1 && v <= 100 }},
		{"overfetchGrowthFactor", []string{"overfetchGrowthFactor", "overfetch_growth_factor"}, func(v float64) bool { return isFinite(v) && v > 1 && v <= 100 }},
		{"candidateTarget", []string{"candidateTarget", "candidate_target"}, inCandidateRange},
		{"maxCandidateLimit", []string{"maxCandidateLimit", "max_candidate_limit"}, inCandidateRange},
		{"limit", []string{"limit"}, inCandidateRange},
		{"rerankTopK", []string{"rerankTopK", "rerank_top_k"}, inCandidateRange},
		{"rerankMinScore", []string{"rerankMinScore", "rerank_min_score"}, isFinite},
	}
	for _, check := range checks {
		raw, present := policyPresent(req, check.keys...)
		if !present {
			continue
		}
		value, ok := ragToFloat64(raw)
		if !ok || !check.ok(value) {
			return localizedError(localization.CypherSubqueriesRAGFailClosedInvalid(check.field), nil)
		}
	}
	if raw, present := policyPresent(req, "adaptiveOverfetch", "adaptive_overfetch"); present {
		if _, ok := toBool(raw); !ok {
			return localizedError(localization.CypherSubqueriesRAGFailClosedInvalid("adaptiveOverfetch"), nil)
		}
	}
	if raw, present := policyPresent(req, "fallbackEnabled", "fallback_enabled"); present {
		if _, ok := toBool(raw); !ok {
			return localizedError(localization.CypherSubqueriesRAGFailClosedInvalid("fallbackEnabled"), nil)
		}
	}
	return nil
}

func parseRetrievalFilters(raw interface{}) map[string][]string {
	filters, ok := raw.(map[string]interface{})
	if !ok || len(filters) == 0 {
		return nil
	}
	parsed := make(map[string][]string, len(filters))
	for property, rawValues := range filters {
		if strings.TrimSpace(property) == "" {
			continue
		}
		var values []string
		switch typed := rawValues.(type) {
		case []string:
			values = typed
		case []interface{}:
			values = make([]string, 0, len(typed))
			for _, value := range typed {
				if value != nil {
					values = append(values, fmt.Sprint(value))
				}
			}
		default:
			if rawValues != nil {
				values = []string{fmt.Sprint(rawValues)}
			}
		}
		// An empty list is kept: it matches nothing, as Cypher's IN [] does
		// (search.nodeMatchesFilters, #938), rather than dropping the
		// constraint.
		parsed[property] = values
	}
	if len(parsed) == 0 {
		return nil
	}
	return parsed
}

func applyAdaptiveCandidateOptions(opts *search.SearchOptions, req map[string]interface{}) {
	if value, ok := toBool(firstPresent(req, "adaptiveOverfetch", "adaptive_overfetch")); ok {
		opts.AdaptiveOverfetch = value
	}
	if value, ok := toInt(firstPresent(req, "candidateTarget", "candidate_target")); ok && value > 0 {
		opts.CandidateTarget = value
	}
	if value, ok := ragToFloat64(firstPresent(req, "initialOverfetchRatio", "initial_overfetch_ratio")); ok && value >= 1 && isFinite(value) {
		opts.InitialOverfetchRatio = value
	}
	if value, ok := ragToFloat64(firstPresent(req, "maxOverfetchRatio", "max_overfetch_ratio")); ok && value >= 1 && isFinite(value) {
		opts.MaxOverfetchRatio = value
	}
	if value, ok := ragToFloat64(firstPresent(req, "overfetchGrowthFactor", "overfetch_growth_factor")); ok && value > 1 && isFinite(value) {
		opts.OverfetchGrowthFactor = value
	}
	if value, ok := toInt(firstPresent(req, "maxCandidateLimit", "max_candidate_limit")); ok && value > 0 {
		opts.MaxCandidateLimit = value
	}
}

// ragProcedureRequest is the request of db.retrieve, db.rretrieve,
// db.rerank or db.infer: the call's one evaluated argument, so a map
// literal, a parameter, a variable bound earlier in the statement or any
// expression gives the same request. A STRING is NornicDB's kept short form
// of {query: text}; null is the procedure's own failure.
func ragProcedureRequest(procedure string, args []interface{}) (map[string]interface{}, error) {
	switch value := procedureArgument(args, 0).(type) {
	case nil:
		return nil, procedureArgumentNullError(procedure, "request")
	case string:
		return map[string]interface{}{"query": value}, nil
	}
	return optionalProcedureMap(procedure, args, 0, "request")
}

func toChatMessages(v interface{}) []heimdall.ChatMessage {
	items, ok := v.([]interface{})
	if !ok {
		return nil
	}
	out := make([]heimdall.ChatMessage, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		role := stringOr(m["role"], "")
		content := stringOr(m["content"], "")
		if strings.TrimSpace(role) == "" || content == "" {
			continue
		}
		out = append(out, heimdall.ChatMessage{Role: role, Content: content})
	}
	return out
}

func parseRerankCandidates(raw interface{}) ([]search.RerankCandidate, error) {
	items, ok := raw.([]interface{})
	if !ok {
		return nil, nil
	}
	out := make([]search.RerankCandidate, 0, len(items))
	for _, item := range items {
		row, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		id := stringOr(firstPresent(row, "id", "node_id"), "")
		content := stringOr(firstPresent(row, "content", "text"), "")
		score, _ := ragToFloat64(firstPresent(row, "score", "bi_score", "rrf_score"))
		if strings.TrimSpace(id) == "" {
			return nil, localizedError(localization.CypherSubqueriesRAGCandidateIDRequired(), nil)
		}
		out = append(out, search.RerankCandidate{
			ID:      id,
			Content: content,
			Score:   score,
		})
	}
	return out, nil
}

func continuationResultMetadata(value any) map[string]any {
	if provider, ok := value.(interface{ ResultMetadata() map[string]any }); ok {
		return provider.ResultMetadata()
	}
	return nil
}

// desugarRetrievalWhere rewrites a retrieval's where predicate as a
// statement's text is rewritten when it enters the executor: function aliases,
// then desugarLabelExpressions for a pattern element's own WHERE
// ((m:Coll WHERE m.id IN $ids)), label expressions and the other pattern
// forms every route reads. The predicate is rewritten as the expression of
// RETURN <where> AS …, and taken back from it.
func desugarRetrievalWhere(ctx context.Context, where string) (string, error) {
	where, _ = canonicalizeFunctionAliases(where)
	const prefix, suffix = "RETURN ", " AS __nornic_where"
	rewritten, rewrite, err := desugarLabelExpressions(prefix+where+suffix, getParamsFromContext(ctx), cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return "", err
	}
	if rewrite == nil || !strings.HasPrefix(rewritten, prefix) || !strings.HasSuffix(rewritten, suffix) {
		return where, nil
	}
	return rewritten[len(prefix) : len(rewritten)-len(suffix)], nil
}
