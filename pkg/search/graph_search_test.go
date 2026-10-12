package search

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A graph search ranks nodes and relationships in one pool: vector hits of
// both kinds by similarity, keyword hits by interleaving each kind's ranking,
// fused by reciprocal rank; a configured reranker orders the fused pool once,
// and its failure or flat scores keep the fused order.
func TestSearchGraph(t *testing.T) {
	ctx := context.Background()
	engine := relationshipTestEngine(t)
	embed := &countingEmbed{}
	svc := startRelationshipService(t, engine, embed.embed, nil)
	waitRelationshipVectors(t, svc, 2)
	for _, node := range []*storage.Node{
		{ID: "span-node", Labels: []string{"Note"}, Properties: map[string]any{"content": "a span"}, ChunkEmbeddings: [][]float32{{1, 0, 0}}},
		{ID: "river-node", Labels: []string{"Note"}, Properties: map[string]any{"content": "zephyrine river"}, ChunkEmbeddings: [][]float32{{0, 1, 0}}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	graph := func(query string, embedding []float32, configure func(*SearchOptions)) *SearchResponse {
		t.Helper()
		opts := DefaultSearchOptions()
		opts.Graph = true
		opts.Limit = 10
		opts.MinSimilarity = new(float64)
		*opts.MinSimilarity = -1
		if configure != nil {
			configure(opts)
		}
		response, err := svc.Search(ctx, query, embedding, opts)
		require.NoError(t, err)
		return response
	}
	keyOf := func(result SearchResult) string {
		if result.Relationship != nil {
			return "r:" + result.ID
		}
		return "n:" + result.ID
	}

	// Vector: span-node (1.0) before r1 ("bridge … river", 0.7), one ranking.
	response := graph("span", wordVector("span"), nil)
	require.Equal(t, "graph_rrf_hybrid", response.SearchMethod)
	ranks := map[string]int{}
	for _, result := range response.Results {
		ranks[keyOf(result)] = result.VectorRank
	}
	require.Equal(t, 1, ranks["n:span-node"])
	require.Equal(t, 2, ranks["r:r1"])

	// Keyword: the first node hit and the first relationship hit interleave.
	response = graph("zephyrine", nil, nil)
	require.Equal(t, "graph_bm25", response.SearchMethod)
	require.Len(t, response.Results, 2)
	require.Equal(t, "n:river-node", keyOf(response.Results[0]))
	require.Equal(t, 1, response.Results[0].BM25Rank)
	require.Equal(t, "r:r1", keyOf(response.Results[1]))
	require.Equal(t, 2, response.Results[1].BM25Rank)

	// Types filters node labels, RelationshipTypes relationship types.
	response = graph("zephyrine", nil, func(o *SearchOptions) { o.Types = []string{"Doc"} })
	require.Len(t, response.Results, 1)
	require.Equal(t, "r:r1", keyOf(response.Results[0]))
	response = graph("zephyrine", nil, func(o *SearchOptions) { o.RelationshipTypes = []string{"CONTRADICTS"} })
	require.Len(t, response.Results, 1)
	require.Equal(t, "n:river-node", keyOf(response.Results[0]))
	response = graph("zephyrine", nil, func(o *SearchOptions) { o.MinRRFScore = 1 })
	require.Empty(t, response.Results)

	// One rerank over the pool: the relationship first.
	rerank := func(o *SearchOptions) { o.RerankEnabled = true }
	reranker := &coverageReranker{enabled: true, results: []RerankResult{
		{ID: "\x00rr1", FinalScore: 0.9}, {ID: "river-node", FinalScore: 0.2},
	}}
	svc.SetReranker(reranker)
	response = graph("zephyrine", nil, rerank)
	require.Equal(t, "graph_bm25+rerank", response.SearchMethod)
	require.NotContains(t, string(response.FallbackReason), "rerank")
	require.Equal(t, []string{"r:r1", "n:river-node"}, []string{keyOf(response.Results[0]), keyOf(response.Results[1])})
	require.Equal(t, 0.9, response.Results[0].Score)
	require.Len(t, reranker.seen, 2)
	response = graph("zephyrine", nil, func(o *SearchOptions) { o.RerankEnabled, o.RerankMinScore = true, 0.5 })
	require.Len(t, response.Results, 1, "below RerankMinScore")
	reranker.results = []RerankResult{{ID: "\x00rr1", FinalScore: 0.5}, {ID: "river-node", FinalScore: 0.49}}
	response = graph("zephyrine", nil, rerank)
	require.Equal(t, SearchFallbackRerankScoresFlat, response.FallbackReason)
	require.Equal(t, "n:river-node", keyOf(response.Results[0]), "the fused order")
	reranker.err = errors.New("provider down")
	response = graph("zephyrine", nil, rerank)
	require.Equal(t, SearchFallbackRerankFailed, response.FallbackReason)
	reranker.enabled = false
	response = graph("zephyrine", nil, rerank)
	require.NotContains(t, string(response.FallbackReason), "rerank", "no reranker: no rerank, as for nodes")
	require.Equal(t, "graph_bm25", response.SearchMethod)

	require.Equal(t, "x", graphResultKey(&SearchResult{ID: "x"}))
	require.Equal(t, "\x00rx", graphResultKey(&SearchResult{ID: "x", Relationship: &storage.Edge{ID: "x"}}))
}
