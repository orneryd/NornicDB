package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/fabricnametest"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestCompositeKeywordBindingsHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	for _, name := range []string{"one", "two"} {
		require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
	}
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
	}))
	seed := postStatement(t, server, "comp", "USE comp.one CREATE (:KeywordNameSeed {v:7})", token)
	require.Empty(t, seed.Errors)
	for _, endpoint := range []string{"/db/comp/tx/commit", "/db/comp/tx"} {
		for _, sample := range fabricnametest.Cases() {
			// The complete grammar population runs through Execute and Bolt.
			// This transport matrix covers the ticket's names and lookup regressions.
			name, _, _ := strings.Cut(sample.Name, "/")
			switch name {
			case "use", "return", "with", "create", "call", "not", "distinct":
			default:
				continue
			}
			t.Run(endpoint+"/"+sample.Name, func(t *testing.T) {
				response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{
					"statements": []map[string]any{{"statement": sample.Query, "parameters": sample.Params}},
				}, token)
				require.True(t, response.Code == http.StatusOK || response.Code == http.StatusCreated, response.Body.String())
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				if result.Commit != "" {
					commitPath := result.Commit[strings.Index(result.Commit, "/db/"):]
					defer makeRequest(t, server, http.MethodDelete, strings.TrimSuffix(commitPath, "/commit"), nil, token)
				}
				require.Empty(t, result.Errors, sample.Query)
				require.Len(t, result.Results, 1)
				require.Equal(t, []string{"k"}, result.Results[0].Columns)
				require.Len(t, result.Results[0].Data, 1)
				require.Equal(t, []any{float64(7)}, result.Results[0].Data[0].Row)
			})
		}
	}
}
