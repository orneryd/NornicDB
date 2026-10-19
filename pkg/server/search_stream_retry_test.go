package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/internal/streamretrytest"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamRetryHTTP(t *testing.T) {
	for _, route := range []string{"search", "autocommit", "tx"} {
		t.Run(route, func(t *testing.T) {
			streamretrytest.Run(t, func(t *testing.T) streamretrytest.Fixture {
				server, authenticator := setupTestServer(t)
				token := getAuthToken(t, authenticator, "admin")
				claims, err := authenticator.ValidateToken(token)
				require.NoError(t, err)
				database := server.dbManager.DefaultDatabaseName()
				engine, err := server.dbManager.GetStorage(database)
				require.NoError(t, err)
				service, err := server.db.GetOrCreateSearchService(database, engine)
				require.NoError(t, err)
				return streamretrytest.Fixture{Service: service, Engine: engine, Owner: auth.PrincipalID(claims), Database: database,
					Pull: func(ctx context.Context, qid string, n int) (streamretrytest.Page, error) {
						request := map[string]any{"database": database, "qid": qid, "n": n}
						path := "/nornicdb/search"
						var payload any = request
						if route != "search" {
							path = "/db/" + database + "/tx"
							if route == "autocommit" {
								path += "/commit"
							}
							payload = map[string]any{"statements": []any{map[string]any{"statement": "CALL db.rretrieve($request)", "parameters": map[string]any{"request": request}}}}
						}
						body, err := json.Marshal(payload)
						require.NoError(t, err)
						req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)).WithContext(ctx)
						req.Header.Set("Content-Type", "application/json")
						req.Header.Set("Authorization", "Bearer "+token)
						response := httptest.NewRecorder()
						server.buildRouter().ServeHTTP(response, req)
						if ctx.Err() != nil {
							return streamretrytest.Page{}, ctx.Err()
						}
						if response.Code != http.StatusOK && response.Code != http.StatusCreated {
							return streamretrytest.Page{}, fmt.Errorf("HTTP %d: %s", response.Code, response.Body.String())
						}
						var value map[string]any
						if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
							return streamretrytest.Page{}, err
						}
						if route != "search" {
							if errors, ok := value["errors"].([]any); ok && len(errors) > 0 {
								return streamretrytest.Page{}, fmt.Errorf("Cypher: %s", response.Body.String())
							}
							result := value["results"].([]any)[0].(map[string]any)
							data := result["data"].([]any)[0].(map[string]any)
							value = data["row"].([]any)[0].(map[string]any)
						}
						page := streamretrytest.Page{More: value["has_more"].(bool)}
						for _, row := range value["results"].([]any) {
							page.IDs = append(page.IDs, row.(map[string]any)["node"].(map[string]any)["id"].(string))
						}
						return page, nil
					}}
			})
		})
	}
}
