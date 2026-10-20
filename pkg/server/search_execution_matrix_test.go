package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/testing/search/contract"
	"github.com/stretchr/testify/require"
)

func TestHTTPContinuationGeneratedExecutionMatrix(t *testing.T) {
	contract.Run(t, func(t *testing.T, _ int, _ contract.Policy) contract.Fixture {
		server, authenticator := setupTestServer(t)
		token := getAuthToken(t, authenticator, "admin")
		claims, err := authenticator.ValidateToken(token)
		require.NoError(t, err)
		database := server.dbManager.DefaultDatabaseName()
		engine, err := server.dbManager.GetStorage(database)
		require.NoError(t, err)
		service, err := server.db.GetOrCreateSearchService(database, engine)
		require.NoError(t, err)
		return contract.Fixture{Service: service, Engine: engine, Owner: auth.PrincipalID(claims), Database: database,
			Pull: func(ctx context.Context, qid string, n int) (contract.Page, error) {
				body, err := json.Marshal(map[string]any{"database": database, "qid": qid, "n": n})
				require.NoError(t, err)
				req := httptest.NewRequest(http.MethodPost, "/nornicdb/search", bytes.NewReader(body)).WithContext(ctx)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)
				response := httptest.NewRecorder()
				server.buildRouter().ServeHTTP(response, req)
				if response.Code != http.StatusOK && ctx.Err() != nil {

					return contract.Page{}, ctx.Err()
				}
				if response.Code == http.StatusServiceUnavailable {
					return contract.Page{}, resultstream.ErrCapacity
				}
				if response.Code != http.StatusOK {
					return contract.Page{}, fmt.Errorf("HTTP %d: %s", response.Code, response.Body.String())
				}
				var wire struct {
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
				if err := json.NewDecoder(response.Body).Decode(&wire); err != nil {
					return contract.Page{}, err
				}
				page := contract.Page{QID: wire.QID, More: wire.More, Completion: wire.Completion, Collection: wire.Collection, Ranked: wire.Ranked}
				for _, result := range wire.Results {
					page.IDs = append(page.IDs, result.Node.ID)
				}
				return page, nil
			}}
	})
}
