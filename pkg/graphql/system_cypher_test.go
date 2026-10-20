package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// Query and mutation share namespace-aware executor setup. System statements
// must reach administration execution without creating a data search service.
func TestSystemCypherSkipsDataSearchSetup(t *testing.T) {
	db := testDB(t)
	manager := testutil.DatabaseManager(t, db)
	handler := NewHandler(db, manager)
	system := manager.SystemDatabaseName()
	var systemSetups atomic.Int64
	db.SetDbConfigResolver(func(database string) (int, float64, string) {
		if database == system {
			systemSetups.Add(1)
		}
		return 0, 0, ""
	})
	admin := cypher.WithPermissionChecker(context.Background(), func(string) bool { return true })
	for _, scoped := range []bool{false, true} {
		for _, mutation := range []bool{false, true} {
			t.Run(fmt.Sprintf("scoped=%t/mutation=%t", scoped, mutation), func(t *testing.T) {
				ctx := admin
				input := map[string]any{"statement": "SHOW DATABASES YIELD name RETURN name"}
				if scoped {
					ctx = auth.WithRequestDatabaseScope(ctx, auth.NewRequestDatabaseScope("", map[string]string{system: system}))
				} else {
					input["database"] = system
				}
				operation, field := "query", "cypher"
				if mutation {
					operation, field = "mutation", "executeCypher"
				}
				document := fmt.Sprintf("%s($input: CypherInput!) { %s(input:$input) { columns rowCount } }", operation, field)
				payload, err := json.Marshal(map[string]any{"query": document, "variables": map[string]any{"input": input}})
				require.NoError(t, err)
				request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload)).WithContext(ctx)
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				require.Equal(t, http.StatusOK, response.Code)
				var result struct {
					Errors []any `json:"errors"`
					Data   map[string]struct {
						Columns  []string `json:"columns"`
						RowCount int      `json:"rowCount"`
					} `json:"data"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				require.Empty(t, result.Errors, response.Body.String())
				require.Contains(t, result.Data[field].Columns, "name")
				require.Positive(t, result.Data[field].RowCount)
				require.Zero(t, systemSetups.Load(), "system administration must not materialize data search state")
			})
		}
	}
}
