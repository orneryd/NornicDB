package search

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// graphCandidate is a node or a relationship in a graph search's fused pool.
type graphCandidate struct {
	result     SearchResult
	vectorRank int
	bm25Rank   int
	rrf        float64
}

func (c *graphCandidate) key() string {
	return graphResultKey(&c.result)
}

// graphResultKey tells a relationship result from a node result with the
// same ID: node and relationship IDs are separate spaces.
func graphResultKey(result *SearchResult) string {
	if result.Relationship != nil {
		return "\x00r" + result.ID
	}
	return result.ID
}

// searchGraph ranks nodes and relationships in one pool (SearchOptions.Graph).
// Each kind is searched to a candidate depth with the request's options (Types
// for node labels, RelationshipTypes for relationship types). The vector hits
// of both kinds are ranked together by similarity, which one embedder makes
// comparable; the keyword hits, whose BM25 scores come from two indexes with
// different statistics, are ranked by interleaving the two rankings (the first
// node hit, the first relationship hit, …). The two rankings are fused by
// reciprocal rank, and a requested rerank orders the fused pool's top
// candidates in one call.
func (s *Service) searchGraph(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 10
	}
	depth := max(limit*5, 50)
	kindOpts := *opts
	kindOpts.Graph = false
	kindOpts.Relationships = false
	kindOpts.Limit = depth
	kindOpts.RerankEnabled = false
	kindOpts.MMREnabled = false
	nodes, err := s.Search(ctx, query, embedding, &kindOpts)
	if err != nil {
		return nil, err
	}
	var relationships []SearchResult
	if s.RelationshipSearchEnabled() {
		relationshipOpts := kindOpts
		relationshipOpts.Relationships = true
		relationshipOpts.Types = opts.RelationshipTypes
		response, err := s.Search(ctx, query, embedding, &relationshipOpts)
		if err != nil {
			return nil, err
		}
		relationships = response.Results
	}

	rrfK := opts.RRFK
	if rrfK <= 0 {
		rrfK = 60
	}
	vectorWeight, bm25Weight := opts.VectorWeight, opts.BM25Weight
	if vectorWeight <= 0 {
		vectorWeight = 1
	}
	if bm25Weight <= 0 {
		bm25Weight = 1
	}
	pool := make([]*graphCandidate, 0, len(nodes.Results)+len(relationships))
	for _, results := range [][]SearchResult{nodes.Results, relationships} {
		for index := range results {
			pool = append(pool, &graphCandidate{result: results[index]})
		}
	}

	// Vector ranking: both kinds by similarity.
	var vectorHits []*graphCandidate
	for _, candidate := range pool {
		if candidate.result.VectorRank > 0 {
			vectorHits = append(vectorHits, candidate)
		}
	}
	sort.SliceStable(vectorHits, func(i, j int) bool {
		if vectorHits[i].result.Similarity != vectorHits[j].result.Similarity {
			return vectorHits[i].result.Similarity > vectorHits[j].result.Similarity
		}
		return vectorHits[i].key() < vectorHits[j].key()
	})
	for rank, candidate := range vectorHits {
		candidate.vectorRank = rank + 1
		candidate.rrf += vectorWeight / (rrfK + float64(rank+1))
	}

	// Keyword ranking: each kind's own ranking, interleaved.
	byKeyword := func(relationship bool) []*graphCandidate {
		var hits []*graphCandidate
		for _, candidate := range pool {
			if candidate.result.BM25Rank > 0 && (candidate.result.Relationship != nil) == relationship {
				hits = append(hits, candidate)
			}
		}
		sort.SliceStable(hits, func(i, j int) bool { return hits[i].result.BM25Rank < hits[j].result.BM25Rank })
		return hits
	}
	nodeHits, relationshipHits := byKeyword(false), byKeyword(true)
	rank := 0
	for i := 0; i < len(nodeHits) || i < len(relationshipHits); i++ {
		for _, hits := range [][]*graphCandidate{nodeHits, relationshipHits} {
			if i < len(hits) {
				rank++
				hits[i].bm25Rank = rank
				hits[i].rrf += bm25Weight / (rrfK + float64(rank))
			}
		}
	}

	sort.SliceStable(pool, func(i, j int) bool {
		if pool[i].rrf != pool[j].rrf {
			return pool[i].rrf > pool[j].rrf
		}
		return pool[i].key() < pool[j].key()
	})
	if minRRF := opts.MinRRFScore; minRRF > 0 {
		kept := pool[:0]
		for _, candidate := range pool {
			if candidate.rrf >= minRRF {
				kept = append(kept, candidate)
			}
		}
		pool = kept
	}

	method := "graph_rrf_hybrid"
	switch {
	case len(embedding) == 0:
		method = "graph_bm25"
	case len(nodeHits) == 0 && len(relationshipHits) == 0:
		method = "graph_vector"
	}
	response := &SearchResponse{
		Status:            "success",
		Query:             query,
		TotalCandidates:   len(pool),
		SearchMethod:      method,
		FallbackTriggered: nodes.FallbackTriggered,
		FallbackReason:    nodes.FallbackReason,
	}
	if nodes.FallbackReason == SearchFallbackNoHybridResults && len(relationships) > 0 {
		// The node side found nothing by hybrid ranking, but the relationship
		// side matched: the graph search as a whole didn't fall back.
		response.FallbackTriggered, response.FallbackReason = false, SearchFallbackNone
	}
	reranked := false
	if opts.RerankEnabled {
		var outcome rerankOutcome
		pool, outcome = s.rerankGraphPool(ctx, query, pool, opts)
		reranked = outcome.Method == rerankMethodApplied
		if outcome.FallbackTriggered {
			response.FallbackTriggered, response.FallbackReason = true, outcome.FallbackReason
		}
		if outcome.Method == rerankMethodApplied {
			response.SearchMethod += "+rerank"
		}
		if len(outcome.excludedIDs) > 0 {
			response.rerankExcluded = make(map[string]struct{}, len(outcome.excludedIDs))
			for _, id := range outcome.excludedIDs {
				response.rerankExcluded[id] = struct{}{}
			}
		}
	}
	response.rerankApplied = reranked
	response.rerankOrderOwned = opts.RerankEnabled
	results := make([]SearchResult, 0, min(limit, len(pool)))
	for _, candidate := range pool {
		if len(results) >= limit {
			break
		}
		result := candidate.result
		result.RRFScore, result.VectorRank, result.BM25Rank = candidate.rrf, candidate.vectorRank, candidate.bm25Rank
		if !reranked {
			result.Score = candidate.rrf
		}
		results = append(results, result)
	}
	response.Results = results
	response.Returned = len(results)
	return response, nil
}

// rerankGraphPool orders a graph search's fused candidates with the
// configured reranker, the top RerankTopK of them in one call, each read as
// its searchable text (a relationship's type and properties, as relationship
// search indexes it). The shared outcome owns the TopK output cap,
// completeness, filtering, order and fallback; this function only maps that
// canonical order to graph hits.
func (s *Service) rerankGraphPool(ctx context.Context, query string, pool []*graphCandidate, opts *SearchOptions) (ordered []*graphCandidate, outcome rerankOutcome) {
	s.mu.RLock()
	reranker := s.reranker
	s.mu.RUnlock()
	// An empty pool has nothing to rerank, and no reason to report: a query
	// chunk that matched nothing doesn't make the search's rerank fail, as
	// in the node rerank (applyStage2Rerank).
	if len(pool) == 0 {
		return pool, s.runRerank(ctx, reranker, query, nil, rerankRunOptions{resultLimit: opts.Limit})
	}
	top := len(pool)
	if opts.RerankTopK > 0 && top > opts.RerankTopK {
		top = opts.RerankTopK
	}
	maxChars := effectiveRerankMaxChars(opts)
	byKey := make(map[string]*graphCandidate, len(pool))
	candidates := make([]RerankCandidate, 0, len(pool))
	for index, candidate := range pool {
		key := candidate.key()
		byKey[key] = candidate
		text := ""
		if index < top {
			text = s.graphRerankContent(&candidate.result, query, maxChars)
		}
		candidates = append(candidates, RerankCandidate{ID: key, Content: text, Score: candidate.rrf})
	}
	outcome = s.runRerank(ctx, reranker, query, candidates, rerankRunOptions{
		providerLimit: top,
		resultLimit:   opts.Limit,
		minScore:      opts.RerankMinScore,
	})
	ordered = make([]*graphCandidate, 0, len(outcome.Results))
	for _, result := range outcome.Results {
		candidate := byKey[result.ID]
		candidate.result.Score = result.FinalScore
		ordered = append(ordered, candidate)
	}
	return ordered, outcome
}

// graphRerankContent is the text a reranker sees for a graph candidate, read
// from storage as the node rerank reads it, and centred on what matched so a
// late passage of a long text isn't cut off (I67):
//   - a node: rerankCandidateContent around its best chunk, or, without the
//     chunk texts kept, a window around that chunk's span;
//   - a relationship: a window around its best chunk's span (or its first
//     term match) in a property on relationship search's list, otherwise its
//     keyword text (RelationshipFulltextProperties); no property outside
//     that list reaches the reranker.
//
// The budget counts characters, not bytes.
func (s *Service) graphRerankContent(result *SearchResult, query string, maxChars int) string {
	match := graphBestMatch(result)
	if relationship := result.Relationship; relationship != nil {
		edge := relationship
		if stored, err := s.engine.GetEdge(relationship.ID); err == nil && stored != nil {
			edge = stored
		}
		if match != nil && s.relationshipPropertyListed(match.Property) {
			if passage := graphPassage(edge.Properties, match, maxChars); passage != "" {
				return passage
			}
		}
		return boundedPrefix(s.relationshipSearchableText(edge), maxChars)
	}
	id := result.NodeID
	if id == "" {
		id = storage.NodeID(result.ID)
	}
	if node, err := s.getNodeWithoutEmbeddings(id); err == nil && node != nil {
		matched := rrfResult{ID: string(id)}
		if match != nil && match.Kind == SearchMatchChunk {
			if len(managedChunkTexts(s.hitEmbedMeta(node))) > match.Chunk {
				matched.MatchID = string(id) + "-chunk-" + strconv.Itoa(match.Chunk)
			} else if passage := graphPassage(node.Properties, match, maxChars); passage != "" {
				return passage
			}
		}
		return s.rerankCandidateContent(node, matched, query, maxChars)
	}
	return boundedPrefix(s.extractSearchableText(graphCandidateTextNode(result)), maxChars)
}

// graphBestMatch is the match a candidate's rerank passage is centred on:
// its best placed chunk match, else its first placed match (a term), else
// its best chunk match without a place (whose chunk text a node may still
// have), else nil; matches come best first.
func graphBestMatch(result *SearchResult) *SearchMatch {
	var unplaced *SearchMatch
	for index := range result.Matches {
		match := &result.Matches[index]
		if match.Kind == SearchMatchChunk {
			if match.Property != "" {
				return match
			}
			if unplaced == nil {
				unplaced = match
			}
		}
	}
	for index := range result.Matches {
		if result.Matches[index].Property != "" {
			return &result.Matches[index]
		}
	}
	return unplaced
}

// graphPassage is the window of properties[match.Property] around the
// match's span, within maxChars characters; "" when the match has no place
// in a string property.
func graphPassage(properties map[string]any, match *SearchMatch, maxChars int) string {
	if match == nil || match.Property == "" || match.End <= match.Start {
		return ""
	}
	value, ok := properties[match.Property].(string)
	if !ok || match.Start >= utf8.RuneCountInString(value) {
		return ""
	}
	return boundedWindow(value, byteOffsetAfterChars(value, 0, match.Start), maxChars)
}

// relationshipPropertyListed reports whether relationship search's keyword
// list admits property (every property when the list is empty).
func (s *Service) relationshipPropertyListed(property string) bool {
	if property == "" {
		return false
	}
	listed := s.RelationshipFulltextProperties()
	if len(listed) == 0 {
		return true
	}
	return slices.Contains(listed, property)
}

// graphCandidateTextNode is the node whose searchable text a result is read
// as: the node itself, or a relationship's type and properties.
func graphCandidateTextNode(result *SearchResult) *storage.Node {
	if relationship := result.Relationship; relationship != nil {
		return &storage.Node{Labels: []string{relationship.Type}, Properties: relationship.Properties}
	}
	return &storage.Node{Labels: result.Labels, Properties: result.Properties}
}
