package server

import (
 "encoding/json"
"github.com/orneryd/nornicdb/testing/cypher/nestedstats"
 "net/http"
 "testing"
 "github.com/stretchr/testify/require"
)

func TestHTTPNestedCypherUpdateStats(t *testing.T) {
 server, authenticator := setupTestServer(t)
 token := "Bearer " + getAuthToken(t, authenticator, "admin")
 rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
  "statements": []map[string]any{{"statement": "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", "parameters": map[string]any{"body": "CREATE (:U5HTTPMode) RETURN 1 AS value"}, "includeStats": true}},
 }, token)
 var raw map[string]any
 require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
 require.Empty(t, raw["errors"], rec.Body.String())
 stats := raw["results"].([]any)[0].(map[string]any)["stats"].(map[string]any)
 require.Equal(t, float64(1), stats["nodes_created"], rec.Body.String())
 require.Equal(t, true, stats["contains_updates"], rec.Body.String())
}

func TestHTTPNestedCypherStatsFamily(t *testing.T) {
 for _, tc := range nestedstats.Cases() {
  t.Run(tc.Name,func(t *testing.T) {
   run := func(query string) map[string]any {
    server, authenticator := setupTestServer(t)
    token := "Bearer " + getAuthToken(t,authenticator,"admin")
    post := func(statement string) map[string]any {
     rec := makeRequest(t,server,http.MethodPost,"/db/nornic/tx/commit",map[string]any{"statements":[]map[string]any{{"statement":statement,"parameters":tc.Params,"includeStats":true}}},token)
     var raw map[string]any
     require.NoError(t,json.Unmarshal(rec.Body.Bytes(),&raw))
     require.Empty(t,raw["errors"],rec.Body.String())
     return raw["results"].([]any)[0].(map[string]any)["stats"].(map[string]any)
    }
    if tc.Setup!="" { post(tc.Setup) }
    return post(query)
   }
   require.Equal(t,run(tc.Direct),run(tc.Query))
  })
 }
}
