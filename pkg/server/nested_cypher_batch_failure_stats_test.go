package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/nestedstats"
	"github.com/stretchr/testify/require"
)

func TestHTTPNestedCypherBatchFailureStats(t *testing.T) {
	for _, tc := range nestedstats.BatchFailureCases() {
		t.Run(tc.Name, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			post := func(query string) map[string]any {
				rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": query, "parameters": tc.Params, "includeStats": true}}}, token)
				var raw map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
				return raw
			}
			if tc.Setup != "" {
				require.Empty(t, post(tc.Setup)["errors"])
			}
			failed := post(tc.Query)
			require.NotEmpty(t, failed["errors"])
			for _, result := range failed["results"].([]any) {
				require.NotContains(t, result.(map[string]any), "stats", "a failed statement has no successful summary")
			}
			read := post("MATCH (n:BatchFailureStats) RETURN count(n)")
			require.Empty(t, read["errors"])
			stats := read["results"].([]any)[0].(map[string]any)["stats"].(map[string]any)
			require.Equal(t, false, stats["contains_updates"])
			require.Equal(t, float64(0), stats["nodes_created"])
		})
	}
}
