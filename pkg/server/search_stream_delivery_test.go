package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

type pageDeliveryFaultWriter struct {
	*httptest.ResponseRecorder
	point  string
	cancel context.CancelFunc
}

func TestSearchStreamDeliveryGraphQLOwner(t *testing.T) {
 s,users:=setupTestServer(t)
 alice:="Bearer "+getAuthToken(t,users,"admin")
 _,err:=users.CreateUser("delivery-other","password123",[]auth.Role{auth.RoleAdmin});require.NoError(t,err)
 bob:="Bearer "+getAuthToken(t,users,"delivery-other")
 database:=s.dbManager.DefaultDatabaseName()
 store,err:=s.dbManager.GetStorage(database);require.NoError(t,err)
 for _,id:=range []storage.NodeID{"a","b","c"} { _,err=store.CreateNode(&storage.Node{ID:id});require.NoError(t,err) }
 run:=func(token,statement string)map[string]any{
  payload:=map[string]any{"query":"query($statement:String!){ cypher(input:{statement:$statement}){ rows } }",
   "variables":map[string]any{"statement":statement}}
  response:=makeRequest(t,s,http.MethodPost,"/graphql",payload,token)
  var value map[string]any;require.NoError(t,json.Unmarshal(response.Body.Bytes(),&value),response.Body.String())
  return value
 }
 started:=run(alice,"CALL db.retrieve({mode:'id',n:1})")
 require.Empty(t,started["errors"],started)
 qid:=findDeliveredSearchQID(started);require.NotEmpty(t,qid)
 wrong:=run(bob,fmt.Sprintf("CALL db.retrieve({qid:%q,n:1})",qid))
 require.NotEmpty(t,wrong["errors"],"shared registry retains authenticated owner binding")
 // HTTP transactions and GraphQL use the same database registry and owner.
 response:=makeRequest(t,s,http.MethodPost,"/db/"+database+"/tx/commit",
  map[string]any{"statements":[]any{map[string]any{"statement":"CALL db.retrieve($request)","parameters":map[string]any{"request":map[string]any{"qid":qid,"n":1}}}}},alice)
 var pulled TransactionResponse;require.NoError(t,json.Unmarshal(response.Body.Bytes(),&pulled));require.Empty(t,pulled.Errors,response.Body.String())
 last:=run(alice,fmt.Sprintf("CALL db.retrieve({qid:%q,n:1})",qid));require.Empty(t,last["errors"],last)
 for i:=0;i<2;i++ { ended:=run(alice,fmt.Sprintf("CALL db.retrieve({qid:%q,discard:true})",qid));require.Empty(t,ended["errors"],ended) }
}


func (w pageDeliveryFaultWriter) WriteHeader(status int) {
	w.ResponseRecorder.WriteHeader(status)
	if w.point == "headers" {
		w.cancel()
	}
}
func (w pageDeliveryFaultWriter) Write(data []byte) (int, error) {
	_, _ = w.ResponseRecorder.Write(data)
	switch w.point {
	case "write":
		return 0, io.ErrClosedPipe
	case "short":
		return len(data) / 2, nil
	case "body":
		w.cancel()
	}
	return len(data), nil
}

// The page is produced by real public handlers; faults occur at the final
// socket boundary, after any TimeoutHandler response buffer.
func TestSearchStreamDeliveryHTTPRoutes(t *testing.T) {
	for _, route := range []string{"search", "autocommit", "open", "execute", "commit", "graphql_query", "graphql_mutation"} {
		t.Run(route, func(t *testing.T) {
			s, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			database := s.dbManager.DefaultDatabaseName()
			store, err := s.dbManager.GetStorage(database)
			require.NoError(t, err)
			for _, id := range []storage.NodeID{"delivery-a", "delivery-b", "delivery-c"} {
				_, err = store.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			router := s.buildRouter()
			serve := func(request map[string]any, point string) (map[string]any, int) {
				path := "/db/" + database + "/tx/commit"
				statement := "CALL db.retrieve($request)"
				payload := map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": map[string]any{"request": request}}}}
				var rollback string
				switch route {
				case "search":
					path = "/nornicdb/search"
					payload = request
				case "open":
					path = "/db/" + database + "/tx"
				case "execute", "commit":
					opened := makeRequest(t, s, http.MethodPost, "/db/"+database+"/tx", map[string]any{"statements": []any{}}, token)
					var tx TransactionResponse
					require.NoError(t, json.Unmarshal(opened.Body.Bytes(), &tx))
					require.Empty(t, tx.Errors)
					parsed, err := url.Parse(tx.Commit)
					require.NoError(t, err)
					path = strings.TrimSuffix(parsed.Path, "/commit")
					rollback = path
					if route == "commit" {
						path += "/commit"
					}
				case "graphql_query", "graphql_mutation":
					path = "/graphql"
					statement = "CALL db.retrieve({mode:'id',n:1})"
					if qid, ok := request["qid"].(string); ok {
						statement = fmt.Sprintf("CALL db.retrieve({qid:%q,n:1})", qid)
						if request["discard"] == true {
							statement = fmt.Sprintf("CALL db.retrieve({qid:%q,discard:true})", qid)
						}
					}
					op, field := "query", "cypher"
					if route == "graphql_mutation" {
						op, field = "mutation", "executeCypher"
					}
					payload = map[string]any{"query": fmt.Sprintf("%s($input: CypherInput!){ %s(input:$input){ rows columns } }", op, field),
						"variables": map[string]any{"input": map[string]any{"statement": statement, "database": database,
							"parameters": map[string]any{}}}}
				}
				body, err := json.Marshal(payload)
				require.NoError(t, err)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body))).WithContext(ctx)
				req.Header.Set("Authorization", token)
				req.Header.Set("Content-Type", "application/json")
				writer := pageDeliveryFaultWriter{httptest.NewRecorder(), point, cancel}
				router.ServeHTTP(writer, req)
				var value map[string]any
				require.NoError(t, json.Unmarshal(writer.Body.Bytes(), &value), writer.Body.String())
				if route == "open" {
					if commit, ok := value["commit"].(string); ok {
						parsed, err := url.Parse(commit)
						require.NoError(t, err)
						rollback = strings.TrimSuffix(parsed.Path, "/commit")
					}
				}
				if rollback != "" {
					_ = makeRequest(t, s, http.MethodDelete, rollback, nil, token)
				}
				return value, writer.Code
			}
			for _, point := range []string{"success", "write", "short", "headers", "body"} {
				t.Run(point, func(t *testing.T) {
					started, status := serve(map[string]any{"mode": "id", "n": 1}, point)
					require.True(t, status >= 200 && status < 300, started)
					require.Empty(t, started["errors"], started)
					qid := findDeliveredSearchQID(started)
					require.NotEmpty(t, qid, started)
					pulled, status := serve(map[string]any{"qid": qid, "n": 1}, "success")
					if point == "success" {
						require.True(t, status >= 200 && status < 300, pulled)
						require.Empty(t, pulled["errors"], pulled)
						require.NotEmpty(t, findDeliveredSearchQID(pulled), pulled)
					} else if route == "search" {
						require.Equal(t, http.StatusGone, status, pulled)
					} else {
						require.NotEmpty(t, pulled["errors"], "failed page delivery kept cursor alive: %v", pulled)
					}
					// Either live or already-ended tokens remain safe to discard.
					_, _ = serve(map[string]any{"qid": qid, "discard": true}, "success")
				})
			}
		})
	}
}
