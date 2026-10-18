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
 "github.com/stretchr/testify/require"
)

type pageDeliveryFaultWriter struct {
 *httptest.ResponseRecorder
 point string
 cancel context.CancelFunc
}
func (w pageDeliveryFaultWriter) WriteHeader(status int) {
 w.ResponseRecorder.WriteHeader(status)
 if w.point == "headers" { w.cancel() }
}
func (w pageDeliveryFaultWriter) Write(data []byte) (int,error) {
 _,_ = w.ResponseRecorder.Write(data)
 switch w.point {
 case "write": return 0,io.ErrClosedPipe
 case "short": return len(data)/2,nil
 case "body": w.cancel()
 }
 return len(data),nil
}

// The page is produced by real public handlers; faults occur at the final
// socket boundary, after any TimeoutHandler response buffer.
func TestSearchStreamDeliveryHTTPRoutes(t *testing.T) {
 for _, route := range []string{"search","autocommit","open","execute","commit","graphql_query","graphql_mutation"} {
  t.Run(route,func(t *testing.T) {
   s,authenticator:=setupTestServer(t)
   token:="Bearer "+getAuthToken(t,authenticator,"admin")
   database:=s.dbManager.DefaultDatabaseName()
   store,err:=s.dbManager.GetStorage(database);require.NoError(t,err)
   for _,id:=range []storage.NodeID{"delivery-a","delivery-b","delivery-c"} {
    _,err=store.CreateNode(&storage.Node{ID:id});require.NoError(t,err)
   }
   router:=s.buildRouter()
   serve:=func(request map[string]any, point string)(map[string]any,int) {
    path:="/db/"+database+"/tx/commit"
    statement:="CALL db.retrieve($request)"
    payload:=map[string]any{"statements":[]any{map[string]any{"statement":statement,"parameters":map[string]any{"request":request}}}}
    var rollback string
    switch route {
    case "search": path="/nornicdb/search";payload=request
    case "open": path="/db/"+database+"/tx"
    case "execute","commit":
     opened:=makeRequest(t,s,http.MethodPost,"/db/"+database+"/tx",map[string]any{"statements":[]any{}},token)
     var tx TransactionResponse;require.NoError(t,json.Unmarshal(opened.Body.Bytes(),&tx));require.Empty(t,tx.Errors)
     parsed,err:=url.Parse(tx.Commit);require.NoError(t,err)
     path=strings.TrimSuffix(parsed.Path,"/commit")
     rollback=path
     if route=="commit" { path+="/commit" }
    case "graphql_query","graphql_mutation":
     path="/graphql"
     reqJSON,err:=json.Marshal(request);require.NoError(t,err)
     // Variables preserve evaluated integer/map requests in GraphQL's JSON scalar.
     op,field:="query","cypher"
     if route=="graphql_mutation" { op,field="mutation","executeCypher" }
     payload=map[string]any{"query":fmt.Sprintf("%s($input: CypherInput!){ %s(input:$input){ rows columns } }",op,field),
      "variables":map[string]any{"input":map[string]any{"statement":statement,"database":database,
       "parameters":map[string]any{"request":json.RawMessage(reqJSON)}}}}
    }
    body,err:=json.Marshal(payload);require.NoError(t,err)
    ctx,cancel:=context.WithCancel(context.Background());defer cancel()
    req:=httptest.NewRequest(http.MethodPost,path,strings.NewReader(string(body))).WithContext(ctx)
    req.Header.Set("Authorization",token);req.Header.Set("Content-Type","application/json")
    writer:=pageDeliveryFaultWriter{httptest.NewRecorder(),point,cancel}
    router.ServeHTTP(writer,req)
    var value map[string]any;require.NoError(t,json.Unmarshal(writer.Body.Bytes(),&value),writer.Body.String())
    if route=="open" {
     if commit,ok:=value["commit"].(string);ok { parsed,err:=url.Parse(commit);require.NoError(t,err);rollback=strings.TrimSuffix(parsed.Path,"/commit") }
    }
    if rollback!="" { _=makeRequest(t,s,http.MethodDelete,rollback,nil,token) }
    return value,writer.Code
   }
   for _, point:=range []string{"success","write","short","headers","body"} {
    t.Run(point,func(t *testing.T) {
     started,status:=serve(map[string]any{"mode":"id","n":1},point)
     require.True(t,status>=200&&status<300,started)
     require.Empty(t,started["errors"],started)
     qid:=findDeliveredSearchQID(started);require.NotEmpty(t,qid,started)
     pulled,status:=serve(map[string]any{"qid":qid,"n":1},"success")
     if point=="success" {
      require.True(t,status>=200&&status<300,pulled)
      require.Empty(t,pulled["errors"],pulled)
      require.NotEmpty(t,findDeliveredSearchQID(pulled),pulled)
     } else if route=="search" {
      require.Equal(t,http.StatusGone,status,pulled)
     } else {
      require.NotEmpty(t,pulled["errors"],"failed page delivery kept cursor alive: %v",pulled)
     }
     // Either live or already-ended tokens remain safe to discard.
     _,_=serve(map[string]any{"qid":qid,"discard":true},"success")
    })
   }
  })
 }
}

