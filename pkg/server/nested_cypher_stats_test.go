package server

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/testing/cypher/nestedstats"
	"github.com/stretchr/testify/require"
	"net/http"
"net/url"
"strings"
	"testing"
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
		t.Run(tc.Name, func(t *testing.T) {
			run := func(query string) map[string]any {
				server, authenticator := setupTestServer(t)
				token := "Bearer " + getAuthToken(t, authenticator, "admin")
				post := func(statement string) map[string]any {
					rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": tc.Params, "includeStats": true}}}, token)
					var raw map[string]any
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
					require.Empty(t, raw["errors"], rec.Body.String())
					return raw["results"].([]any)[0].(map[string]any)["stats"].(map[string]any)
				}
				if tc.Setup != "" {
					post(tc.Setup)
				}
				return post(query)
			}
			require.Equal(t, run(tc.Direct), run(tc.Query))
		})
	}
}

func TestHTTPNestedCypherRollbackStats(t *testing.T) {
 server,authenticator:=setupTestServer(t)
 token:="Bearer "+getAuthToken(t,authenticator,"admin")
 post:=func(method,path,statement string) map[string]any {
  statements:=[]map[string]any{}
  if statement!="" { statements=append(statements,map[string]any{"statement":statement,"parameters":map[string]any{"body":"CREATE (:NestedRollback) RETURN 1 AS value"},"includeStats":true}) }
  rec:=makeRequest(t,server,method,path,map[string]any{"statements":statements},token)
  var raw map[string]any
  require.NoError(t,json.Unmarshal(rec.Body.Bytes(),&raw),rec.Body.String())
  require.Empty(t,raw["errors"],rec.Body.String())
  return raw
 }
 opened:=post(http.MethodPost,"/db/nornic/tx","")
 commit:=opened["commit"].(string)
 parsed,err:=url.Parse(commit);require.NoError(t,err)
 path:=strings.TrimSuffix(parsed.Path,"/commit")
 written:=post(http.MethodPost,path,"CALL apoc.cypher.doIt($body,{}) YIELD value RETURN value")
 stats:=written["results"].([]any)[0].(map[string]any)["stats"].(map[string]any)
 require.Equal(t,float64(1),stats["nodes_created"])
 rolledBack:=post(http.MethodDelete,path,"")
 require.Empty(t,rolledBack["results"])
 read:=post(http.MethodPost,"/db/nornic/tx/commit","MATCH (n:NestedRollback) RETURN count(n)")
 result:=read["results"].([]any)[0].(map[string]any)
 require.Equal(t,false,result["stats"].(map[string]any)["contains_updates"])
 row:=result["data"].([]any)[0].(map[string]any)["row"].([]any)
 require.Equal(t,float64(0),row[0])
}
