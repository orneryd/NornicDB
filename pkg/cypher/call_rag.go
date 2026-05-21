package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) callDbRetrieve(ctx context.Context, cypher string) (*ExecuteResult, error) {
	req, err := e.parseRagProcedureRequest(ctx, cypher, "DB.RETRIEVE")
	if err != nil {
		return nil, err
	}
	return e.runSearchRequest(ctx, req, false, false)
}

func (e *StorageExecutor) callDbRRetrieve(ctx context.Context, cypher string) (*ExecuteResult, error) {
	req, err := e.parseRagProcedureRequest(ctx, cypher, "DB.RRETRIEVE")
	if err != nil {
		return nil, err
	}
	return e.runSearchRequest(ctx, req, false, true)
}

func (e *StorageExecutor) callDbRerank(ctx context.Context, cypher string) (*ExecuteResult, error) {
	req, err := e.parseRagProcedureRequest(ctx, cypher, "DB.RERANK")
	if err != nil {
		return nil, err
	}

	query := stringOr(req["query"], stringOr(req["text"], ""))
	candidates, err := parseRerankCandidates(firstPresent(req, "candidates", "rows", "results"))
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("db.rerank requires non-empty candidates")
	}

	opts := search.DefaultSearchOptions()
	if v, ok := toInt(firstPresent(req, "rerankTopK", "rerank_top_k")); ok && v > 0 {
		opts.RerankTopK = v
	}
	if v, ok := ragToFloat64(firstPresent(req, "rerankMinScore", "rerank_min_score")); ok {
		opts.RerankMinScore = v
	}

	svc := e.searchService
	if svc == nil {
		svc = search.NewService(e.storage)
		e.searchService = svc
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

func (e *StorageExecutor) callDbInfer(ctx context.Context, cypher string) (*ExecuteResult, error) {
	req, err := e.parseRagProcedureRequest(ctx, cypher, "DB.INFER")
	if err != nil {
		return nil, err
	}
	manager := e.GetInferenceManager()
	if manager == nil {
		return nil, fmt.Errorf("inference manager is not configured")
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
			return nil, fmt.Errorf("db.infer messages cannot be empty")
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
			return nil, fmt.Errorf("db.infer requires prompt or messages")
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
	if strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("query is required")
	}

	opts := search.GetAdaptiveRRFConfig(query)
	if limit, ok := toInt(req["limit"]); ok && limit > 0 {
		opts.Limit = limit
	}
	if types := toStringSlice(firstPresent(req, "types", "labels")); len(types) > 0 {
		opts.Types = types
	}
	if minSim, ok := ragToFloat64(firstPresent(req, "minSimilarity", "min_similarity")); ok {
		opts.MinSimilarity = &minSim
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

	embedding := toFloat32Slice(firstPresent(req, "embedding", "queryEmbedding", "query_embedding"))
	if len(embedding) == 0 && e.embedder != nil {
		embedded, embedErr := embedQueryChunked(ctx, e.embedder, query)
		if embedErr == nil {
			embedding = embedded
		}
	}

	svc := e.searchService
	if svc == nil {
		dims := search.DefaultVectorDimensions
		if len(embedding) > 0 {
			dims = len(embedding)
		}
		svc = search.NewServiceWithDimensions(e.storage, dims)
		e.searchService = svc
	}
	if useConfiguredRerank {
		opts.RerankEnabled = svc.RerankerAvailable(ctx)
	}

	response, err := svc.Search(ctx, query, embedding, opts)
	if err != nil {
		return nil, err
	}

	result := &ExecuteResult{
		Columns: []string{"node", "score", "rrf_score", "vector_rank", "bm25_rank", "search_method", "fallback_triggered"},
		Rows:    make([][]interface{}, 0, len(response.Results)),
	}

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
		})
	}

	return result, nil
}

func (e *StorageExecutor) parseRagProcedureRequest(ctx context.Context, cypher, procName string) (map[string]interface{}, error) {
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, procName)
	if idx == -1 {
		return nil, fmt.Errorf("invalid %s syntax", strings.ToLower(procName))
	}
	parenStart := strings.Index(cypher[idx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("%s requires a request argument", strings.ToLower(procName))
	}
	parenStart += idx
	parenEnd := e.findMatchingParen(cypher, parenStart)
	if parenEnd == -1 {
		return nil, fmt.Errorf("unmatched parenthesis in %s", strings.ToLower(procName))
	}
	rawArg := strings.TrimSpace(cypher[parenStart+1 : parenEnd])
	if rawArg == "" {
		return map[string]interface{}{}, nil
	}

	if strings.HasPrefix(rawArg, "{") && strings.HasSuffix(rawArg, "}") {
		return e.parseMapLiteral(ctx, rawArg), nil
	}
	if strings.HasPrefix(rawArg, "$") {
		name := strings.TrimPrefix(rawArg, "$")
		if params := getParamsFromContext(ctx); params != nil {
			if req, ok := params[name].(map[string]interface{}); ok {
				return req, nil
			}
		}
		return nil, fmt.Errorf("%s parameter %s must be a map", strings.ToLower(procName), rawArg)
	}
	if (strings.HasPrefix(rawArg, "'") && strings.HasSuffix(rawArg, "'")) ||
		(strings.HasPrefix(rawArg, "\"") && strings.HasSuffix(rawArg, "\"")) {
		return map[string]interface{}{"query": strings.Trim(rawArg, "\"'")}, nil
	}
	return nil, fmt.Errorf("%s request must be a map literal", strings.ToLower(procName))
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
			return nil, fmt.Errorf("db.rerank candidate id is required")
		}
		out = append(out, search.RerankCandidate{
			ID:      id,
			Content: content,
			Score:   score,
		})
	}
	return out, nil
}
