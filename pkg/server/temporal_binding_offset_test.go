package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/temporaloffset"
	"github.com/stretchr/testify/require"
)

func TestTemporalBindingHTTPTemporalOffsetMutationRoutes(t *testing.T) {
	t.Run("typed byte property", TestBytePropertyHTTPRoundTrip)
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(endpoint, query string, parameters ...map[string]any) TransactionResponse {
		t.Helper()
		statements := []map[string]any{}
		if query != "" {
			statement := map[string]any{"statement": query}
			if len(parameters) > 0 {
				statement["parameters"] = parameters[0]
			}
			statements = append(statements, statement)
		}
		response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": statements}, token)
		require.Contains(t, []int{http.StatusOK, http.StatusCreated}, response.Code)
		var result TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		require.Empty(t, result.Errors)
		return result
	}
	for _, text := range []string{"2026-10-25T01:30:00+01:00[Europe/London]", "2026-10-25T01:30:00.123456789+01:00[Europe/London]"} {
		for _, explicit := range []bool{false, true} {
			for _, c := range temporaloffset.Cases(text) {
				t.Run(fmt.Sprintf("%s/tx=%t/%s", text, explicit, c.Name), func(t *testing.T) {
					post("/db/nornic/tx/commit", "MATCH (n) DETACH DELETE n")
					endpoint, commit := "/db/nornic/tx/commit", ""
					if explicit {
						opened := post("/db/nornic/tx", "")
						commit = opened.Commit
						endpoint = strings.TrimSuffix(commit, "/commit")
						require.NotEmpty(t, commit)
					}
					if c.Setup != "" {
						post(endpoint, c.Setup)
					}
					// HTTP JSON has no temporal type; construct t and m in the row scope.
					query := strings.ReplaceAll(strings.ReplaceAll(c.Query, "$t", "t"), "$m", "m")
					query = "WITH datetime($input.text) AS t WITH t, {t:t, ts:[t]} AS m " + query
					// The WITH/WHERE probes must carry the values to the MERGE.
					query = strings.ReplaceAll(query, "WITH 1 AS x WHERE", "WITH t, m, 1 AS x WHERE")
					check := func(result TransactionResponse) {
						t.Helper()
						require.Len(t, result.Results, 1)
						require.Len(t, result.Results[0].Data, 1)
						require.Equal(t, []any{float64(1792888200), text, float64(1792888200), text}, result.Results[0].Data[0].Row)
					}
					check(post(endpoint, query, map[string]any{"input": map[string]any{"text": text}}))
					if explicit {
						post(commit, "")
					}
					check(post("/db/nornic/tx/commit", c.Read))
				})
			}
		}
	}
}
