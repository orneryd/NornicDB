package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// graphSearchRequestKey marks a db.retrieveGraph request, as
// relationshipSearchRequestKey marks a db.retrieveRelationships one.
const graphSearchRequestKey = "\x00graph"

// callDbRetrieveGraph ranks nodes and relationships in one pool, with
// db.retrieve's request (ragProcedureRequest), plus relationshipTypes for the
// relationships (types filters the nodes' labels). With n or maxResults it
// returns one terminal page instead of rows.
func (e *StorageExecutor) callDbRetrieveGraph(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	return e.runGraphSearchRequest(ctx, "db.retrieveGraph", args, false)
}

// callDbRRetrieveGraph is db.retrieveGraph with the configured reranker, when
// one is available, ordering the fused pool of nodes and relationships in one
// call, as db.rretrieve reranks db.retrieve's nodes.
func (e *StorageExecutor) callDbRRetrieveGraph(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	return e.runGraphSearchRequest(ctx, "db.rretrieveGraph", args, true)
}

func (e *StorageExecutor) runGraphSearchRequest(ctx context.Context, procedure string, args []interface{}, useConfiguredRerank bool) (*ExecuteResult, error) {
	req, err := ragProcedureRequest(procedure, args)
	if err != nil {
		return nil, err
	}
	marked := make(map[string]interface{}, len(req)+1)
	for key, value := range req {
		marked[key] = value
	}
	marked[graphSearchRequestKey] = true
	return e.runSearchRequest(ctx, marked, false, useConfiguredRerank)
}

// graphSearchEntity is one graph search result's kind and entities: a node,
// or a relationship with its start and end nodes (null when not visible).
func (e *StorageExecutor) graphSearchEntity(result *search.SearchResult) (kind string, node, relationship, start, end interface{}) {
	if result.Relationship == nil {
		return "node", &storage.Node{ID: storage.NodeID(result.ID), Database: storage.DatabaseOf(e.storage), Labels: result.Labels, Properties: result.Properties}, nil, nil, nil
	}
	endpoint := func(id storage.NodeID) interface{} {
		if found, err := e.storage.GetNode(id); err == nil && found != nil {
			return found
		}
		return nil
	}
	startID, endID, selected := nativeEdgeStorageEndpoints(e.storage, result.Relationship)
	if !selected {
		return "relationship", nil, result.Relationship, nil, nil
	}
	return "relationship", nil, result.Relationship, endpoint(startID), endpoint(endID)
}

// graphSearchResult is a graph search's rows (db.retrieveGraph's columns),
// each hit with its matches and, as db.retrieve's rows, how many chunks of a
// query split into chunks were searched (0 and 0 for a query searched whole).
func (e *StorageExecutor) graphSearchResult(response *search.SearchResponse) *ExecuteResult {
	result := &ExecuteResult{
		Columns: []string{"kind", "node", "relationship", "start", "end", "score", "rrf_score", "vector_rank", "bm25_rank", "search_method", "fallback_triggered", "fallback_reason", "query_chunks_searched", "query_chunks_total", "matches"},
		Rows:    make([][]interface{}, 0, len(response.Results)),
	}
	chunksSearched, chunksTotal := int64(response.QueryChunksSearched), int64(response.QueryChunksTotal)
	for index := range response.Results {
		r := &response.Results[index]
		kind, node, relationship, start, end := e.graphSearchEntity(r)
		result.Rows = append(result.Rows, []interface{}{
			kind, node, relationship, start, end,
			r.Score, r.RRFScore, r.VectorRank, r.BM25Rank,
			response.SearchMethod, response.FallbackTriggered, string(response.FallbackReason),
			chunksSearched, chunksTotal, search.SearchMatchValues(r.Matches),
		})
	}
	return result
}

// graphSearchPage is a graph search as one terminal page, the form
// db.rretrieve's pages take: results carry kind, node or relationship with
// start and end, scores and matches; the page carries the search method,
// fallback and, for a query split into chunks, how many were searched. It has
// no qid: there is nothing to continue.
func (e *StorageExecutor) graphSearchPage(response *search.SearchResponse) *ExecuteResult {
	results := make([]interface{}, 0, len(response.Results))
	for index := range response.Results {
		r := &response.Results[index]
		kind, node, relationship, start, end := e.graphSearchEntity(r)
		results = append(results, map[string]interface{}{
			"kind": kind, "node": node, "relationship": relationship, "start": start, "end": end,
			"score": r.Score, "rrf_score": r.RRFScore,
			"vector_rank": int64(r.VectorRank), "bm25_rank": int64(r.BM25Rank), "matches": search.SearchMatchValues(r.Matches),
		})
	}
	page := map[string]interface{}{
		"results": results, "qid": "", "has_more": false,
		"position": int64(len(results)), "returned": int64(len(results)), "discovered": int64(response.TotalCandidates),
		"search_method": response.SearchMethod, "fallback_triggered": response.FallbackTriggered,
		"fallback_reason": string(response.FallbackReason), "completion": search.SearchContinuationMaxResultsComplete,
	}
	if response.QueryChunksTotal > 0 {
		page["query_chunks_searched"] = int64(response.QueryChunksSearched)
		page["query_chunks_total"] = int64(response.QueryChunksTotal)
	}
	return &ExecuteResult{Columns: []string{"page"}, Rows: [][]interface{}{{page}}}
}
