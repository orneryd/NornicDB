package server

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/testing/search/contract"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestHTTPContinuationResultLimitPublic(t *testing.T) {
	contract.RunResultLimit(t, func(t *testing.T, vector bool) contract.LimitFixture {
		server, authenticator := setupTestServer(t)
		token := getAuthToken(t, authenticator, "admin")
		database := server.dbManager.DefaultDatabaseName()
		engine, err := server.dbManager.GetStorage(database)
		require.NoError(t, err)
		service, err := server.db.GetOrCreateSearchService(database, engine)
		require.NoError(t, err)
		if vector {
			server.db.SetEmbedder(&countingEmbedder{dims: service.VectorIndexDimensions()})
		}
		request := func(ctx context.Context, body map[string]any) (contract.Page, error) {
			body["database"] = database
			wire := makeRequest(t, server, http.MethodPost, "/nornicdb/search", body, "Bearer "+token)
			require.Equal(t, http.StatusOK, wire.Code, wire.Body.String())
			var page struct {
				QID        string `json:"qid"`
				More       bool   `json:"has_more"`
				Completion string `json:"completion"`
				Collection bool   `json:"collection_exhausted"`
				Ranked     bool   `json:"ranked_pool_exhausted"`
				Results    []struct {
					Node struct {
						ID string `json:"id"`
					} `json:"node"`
				} `json:"results"`
			}
			if n, ok := body["n"].(int); ok && n == 0 {
				require.NoError(t, json.NewDecoder(wire.Body).Decode(&page.Results))
			} else {
				require.NoError(t, json.NewDecoder(wire.Body).Decode(&page))
			}
			result := contract.Page{QID: page.QID, More: page.More, Completion: page.Completion, Collection: page.Collection, Ranked: page.Ranked}
			for _, row := range page.Results {
				result.IDs = append(result.IDs, row.Node.ID)
			}
			return result, nil
		}
		return contract.LimitFixture{Fixture: contract.Fixture{Service: service, Engine: engine, Pull: func(ctx context.Context, qid string, n int) (contract.Page, error) {
			return request(ctx, map[string]any{"qid": qid, "n": n})
		}}, Start: func(ctx context.Context, r contract.LimitRequest) (contract.Page, error) {
			return request(ctx, map[string]any{"query": "ceilingtoken", "limit": r.Limit, "n": r.N, "max_results": r.MaxResults, "mode": string(r.Mode)})
		}}
	})
}
