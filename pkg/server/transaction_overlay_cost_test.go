package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionOverlayHTTPRoutes(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		server, auth := setupTestServer(t)
		token := "Bearer " + getAuthToken(t, auth, "admin")
		endpoint := "/db/nornic/tx/commit"
		commit := ""
		if explicit {
			var errs []any
			commit, errs = openHTTPTransaction(t, server, token)
			require.Empty(t, errs)
			endpoint = strings.TrimSuffix(commit, "/commit")
		}
		query := func(statement string, want float64) {
			response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []any{map[string]any{"statement": statement}}}, token)
			var result struct {
				Results []struct {
					Data []struct {
						Row []any `json:"row"`
					} `json:"data"`
				} `json:"results"`
				Errors []any `json:"errors"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
			require.Empty(t, result.Errors, response.Body.String())
			require.Len(t, result.Results, 1)
			require.Len(t, result.Results[0].Data, 1)
			require.Equal(t, []any{want}, result.Results[0].Data[0].Row)
		}
		const create = `UNWIND range(1,64) AS i MERGE (a:Overlay {id:i % 7}) MERGE (b:Overlay {id:(i+1) % 7}) CREATE (a)-[:R {v:i}]->(b) RETURN count(*)`
		query(create, 64)
		query(create, 64)
		query("MATCH (n:Overlay) DETACH DELETE n RETURN count(*)", 7)
		if explicit {
			require.Empty(t, postMerge(t, server, token, commit, "RETURN 1", nil))
			endpoint = "/db/nornic/tx/commit"
		}
		query("MATCH (n:Overlay) RETURN count(n)", 0)
		query("MATCH ()-[r:R]->() RETURN count(r)", 0)
	}
}
