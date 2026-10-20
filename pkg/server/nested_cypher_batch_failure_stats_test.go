package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/orneryd/nornicdb/testing/cypher/nestedstats"
	"github.com/stretchr/testify/require"
)

func TestHTTPNestedCypherBatchFailureStats(t *testing.T) {
	for _, tc := range nestedstats.BatchFailureCases() {
		t.Run(tc.Name, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			postAt := func(path, query string) map[string]any {
				statements := []map[string]any{}
				if query != "" {
					statements = append(statements, map[string]any{"statement": query, "parameters": tc.Params, "includeStats": true})
				}
				rec := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": statements}, token)
				var raw map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
				return raw
			}
			post := func(query string) map[string]any { return postAt("/db/nornic/tx/commit", query) }
			if tc.Setup != "" {
				require.Empty(t, post(tc.Setup)["errors"])
			}
			// Match the explicit transaction scope of the Bolt counterpart.
			// Autocommit APOC reports failed batches as result rows; explicit
			// failure/commit must not publish a successful statement summary.
			opened := postAt("/db/nornic/tx", "")
			require.Empty(t, opened["errors"])
			commit, err := url.Parse(opened["commit"].(string))
			require.NoError(t, err)
			failed := postAt(commit.Path, tc.Query)
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
