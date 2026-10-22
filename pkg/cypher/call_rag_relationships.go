package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// relationshipSearchRequestKey marks a db.retrieveRelationships request; a
// Cypher map can't hold the key, so db.retrieve's requests never carry it.
const relationshipSearchRequestKey = "\x00relationships"

// callDbRetrieveRelationships searches relationships as db.retrieve searches
// nodes (#1065), with db.retrieve's request (ragProcedureRequest).
func (e *StorageExecutor) callDbRetrieveRelationships(admission admittedRelationshipStatement, ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	req, err := ragProcedureRequest("db.retrieveRelationships", args)
	if err != nil {
		return nil, err
	}
	// The request may be the caller's parameter map: mark a copy.
	marked := make(map[string]interface{}, len(req)+1)
	for key, value := range req {
		marked[key] = value
	}
	marked[relationshipSearchRequestKey] = true
	return e.runSearchRequest(admission, ctx, marked, false, false)
}

// relationshipSearchResult is a relationship search's rows: each matching
// relationship with its start and end nodes (null when not visible), then
// the same scores and search metadata as node results (#1065).
func (e *StorageExecutor) relationshipSearchResult(response *search.SearchResponse) *ExecuteResult {
	result := &ExecuteResult{
		Columns: []string{"relationship", "start", "end", "score", "rrf_score", "vector_rank", "bm25_rank", "search_method", "fallback_triggered", "fallback_reason"},
		Rows:    make([][]interface{}, 0, len(response.Results)),
	}
	endpoint := func(id storage.NodeID) interface{} {
		if node, err := e.storage.GetNode(id); err == nil && node != nil {
			return node
		}
		return nil
	}
	for _, r := range response.Results {
		relationship := r.Relationship
		result.Rows = append(result.Rows, []interface{}{
			relationship,
			endpoint(relationship.StartNode),
			endpoint(relationship.EndNode),
			r.Score,
			r.RRFScore,
			r.VectorRank,
			r.BM25Rank,
			response.SearchMethod,
			response.FallbackTriggered,
			string(response.FallbackReason),
		})
	}
	return result
}
