package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Exercise the public transaction handlers through an actual runtime arithmetic
// failure; statementFailure must use the same decoded native declaration names.
func TestHTTPNativeYieldFailureDeclarationHeaders(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	seed := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "CREATE (:DeclaredHTTP)"}}}, token)
	var seeded TransactionResponse
	require.NoError(t, json.Unmarshal(seed.Body.Bytes(), &seeded))
	require.Empty(t, seeded.Errors, seed.Body.String())
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, sample := range []struct{ syntax, name string }{
			{"plain", "plain"}, {"`quoted`", "quoted"}, {"`name,part`", "name,part"}, {"`name``part`", "name`part"}, {"```edge```", "`edge`"},
		} {
			body := "CALL db.labels() YIELD label AS " + sample.syntax + " WHERE 1 / $zero = 1 RETURN *"
			for _, query := range []string{
				body,
				"WITH 7 AS value " + body,
				"WITH 7 AS value CALL () { " + body + " } RETURN " + sample.syntax,
				"CALL db.labels() YIELD label AS " + sample.syntax + " WHERE 1 / $zero = 1 RETURN " + sample.syntax + " UNION ALL RETURN 'other' AS " + sample.syntax,
			} {
				for _, endpoint := range []string{"/db/nornic/tx/commit", "/db/nornic/tx"} {
					t.Run(fmt.Sprintf("normalize_%v/%s/%s/%s", normalize, sample.name, endpoint, query), func(t *testing.T) {
						for repeat := 0; repeat < 2; repeat++ {
							rec := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []map[string]any{{"statement": query, "parameters": map[string]any{"zero": 0}}}}, token)
							var response TransactionResponse
							require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), rec.Body.String())
							require.Len(t, response.Errors, 1, rec.Body.String())
							require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", response.Errors[0].Code, rec.Body.String())
							require.Len(t, response.Results, 1, rec.Body.String())
							require.Equal(t, []string{sample.name}, response.Results[0].Columns, rec.Body.String())
							require.Empty(t, response.Results[0].Data)
						}
					})
				}
			}
		}
	}
}
