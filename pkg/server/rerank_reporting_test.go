package server

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type malformedSearchReranker struct {
	results []search.RerankResult
	err     error
}

type fixedSearchEmbedder struct{}

func (fixedSearchEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}
func (fixedSearchEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for index := range results {
		results[index] = []float32{1, 0}
	}
	return results, nil
}
func (fixedSearchEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (fixedSearchEmbedder) Dimensions() int { return 2 }
func (fixedSearchEmbedder) Model() string   { return "fixed" }
func (fixedSearchEmbedder) Backend() string { return "cpu" }

func (r malformedSearchReranker) Name() string                     { return "malformed" }
func (r malformedSearchReranker) Enabled() bool                    { return true }
func (r malformedSearchReranker) IsAvailable(context.Context) bool { return true }
func (r malformedSearchReranker) Rerank(context.Context, string, []search.RerankCandidate) ([]search.RerankResult, error) {
	return append([]search.RerankResult(nil), r.results...), r.err
}

func TestHTTPSearchMalformedRerankFallsBackTruthfully(t *testing.T) {
	server, authenticator := setupTestServer(t)
	server.db.SetDbConfigResolver(func(string) (int, float64) { return 2, 0 })
	server.db.SetEmbedConfigForDB(nil)
	server.db.SetEmbedder(fixedSearchEmbedder{})
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	for index, id := range []string{"a", "b", "c"} {
		node := &storage.Node{
			ID: storage.NodeID(id), Labels: []string{"Doc"},
			Properties:      map[string]any{"content": "alpha document " + id},
			ChunkEmbeddings: [][]float32{{1, float32(index) * 0.01}},
		}
		_, err = engine.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, server.db.BuildSearchIndexes(context.Background()))
	service, err := server.db.GetOrCreateSearchService(database, engine)
	require.NoError(t, err)
	service.SetSearchResultCachePolicy(0, 0)
	baselineOptions := search.DefaultSearchOptions()
	baselineOptions.Limit = 3
	baseline, err := service.Search(context.Background(), "alpha", []float32{1, 0}, baselineOptions)
	require.NoError(t, err)
	wantIDs := make([]string, len(baseline.Results))
	for index := range baseline.Results {
		wantIDs[index] = baseline.Results[index].ID
	}
	token := getAuthToken(t, authenticator, "admin")
	for name, results := range map[string][]search.RerankResult{
		"duplicate": {{ID: wantIDs[0], OriginalRank: 1, CrossScore: 0.9, FinalScore: 0.9}, {ID: wantIDs[0], OriginalRank: 1, CrossScore: 0.1, FinalScore: 0.1}},
		"negative":  {{OriginalRank: -1, CrossScore: 0.9, FinalScore: 0.9}},
		"nan":       {{ID: wantIDs[0], OriginalRank: 1, CrossScore: math.NaN(), FinalScore: math.NaN()}},
	} {
		t.Run(name, func(t *testing.T) {
			provider := &malformedSearchReranker{results: results}
			server.db.SetRerankerResolver(func(string) search.Reranker { return provider })
			server.db.SetSearchReranker(provider)
			service.SetReranker(provider)
			recorder := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
				"query": "alpha", "limit": 3,
			}, "Bearer "+token)
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, string(search.SearchFallbackRerankFailed), recorder.Header().Get("X-NornicDB-Search-Fallback-Reason"))
			var response []*nornicdb.SearchResult
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			gotIDs := make([]string, len(response))
			for index := range response {
				gotIDs[index] = response[index].Node.ID
			}
			require.Equal(t, wantIDs, gotIDs)
		})
	}
}

func TestHTTPCypherDbRerankProviderFailuresReturnErrors(t *testing.T) {
	server, authenticator := setupTestServer(t)
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	service, err := server.db.GetOrCreateSearchService(database, engine)
	require.NoError(t, err)
	token := getAuthToken(t, authenticator, "admin")
	peer := httptest.NewServer(server.buildRouter())
	t.Cleanup(peer.Close)

	for name, provider := range map[string]*malformedSearchReranker{
		"timeout":   {err: context.DeadlineExceeded},
		"duplicate": {results: []search.RerankResult{{ID: "a", OriginalRank: 1, CrossScore: 0.9, FinalScore: 0.9}, {ID: "a", OriginalRank: 1, CrossScore: 0.8, FinalScore: 0.8}}},
		"nan":       {results: []search.RerankResult{{ID: "a", OriginalRank: 1, CrossScore: math.NaN(), FinalScore: math.NaN()}}},
	} {
		t.Run(name, func(t *testing.T) {
			server.db.SetRerankerResolver(func(string) search.Reranker { return provider })
			server.db.SetSearchReranker(provider)
			service.SetReranker(provider)
			body, err := json.Marshal(map[string]any{"statements": []map[string]any{{
				"statement": "CALL db.rerank({query: 'alpha', candidates: [{id: 'a', content: 'alpha', score: 0.03}, {id: 'b', content: 'beta', score: 0.02}]}) YIELD id RETURN id",
			}}})
			require.NoError(t, err)
			request, err := http.NewRequest(http.MethodPost, peer.URL+"/db/"+database+"/tx/commit", bytes.NewReader(body))
			require.NoError(t, err)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+token)
			response, err := peer.Client().Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusOK, response.StatusCode)
			var payload TransactionResponse
			require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
			require.Len(t, payload.Errors, 1)
			require.Contains(t, payload.Errors[0].Message, "rerank failed")
			require.Len(t, payload.Results, 1)
			require.Empty(t, payload.Results[0].Data)
		})
	}
}
