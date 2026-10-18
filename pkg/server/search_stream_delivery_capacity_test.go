package server

import (
 "encoding/json"
 "fmt"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"

 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestSearchStreamDeliveryCypherFailedDelivery(t *testing.T) {
 s, authenticator := setupTestServer(t)
 token := "Bearer " + getAuthToken(t, authenticator, "admin")
 database := s.dbManager.DefaultDatabaseName()
 engine, err := s.dbManager.GetStorage(database)
 require.NoError(t, err)
 for _, id := range []storage.NodeID{"sol-a", "sol-b", "sol-c"} {
  _, err = engine.CreateNode(&storage.Node{ID:id})
  require.NoError(t, err)
 }
 body := `{"statements":[{"statement":"CALL db.retrieve({mode: 'id', n: 1})"}]}`
 request := httptest.NewRequest(http.MethodPost, "/db/"+database+"/tx/commit", strings.NewReader(body))
 request.Header.Set("Authorization", token)
 request.Header.Set("Content-Type", "application/json")
 writer := disconnectedSearchWriter{httptest.NewRecorder()}
 s.buildRouter().ServeHTTP(writer, request)
 var initial TransactionResponse
 require.NoError(t, json.Unmarshal(writer.Body.Bytes(), &initial))
 require.Empty(t, initial.Errors, writer.Body.String())
 var value any
 require.NoError(t, json.Unmarshal(writer.Body.Bytes(), &value))
 qid := findDeliveredSearchQID(value)
 require.NotEmpty(t, qid, writer.Body.String())
 t.Logf("created cursor through failed Cypher response; checking liveness")
 pull := map[string]any{"statements": []any{map[string]any{
  "statement":"CALL db.retrieve($request)",
  "parameters":map[string]any{"request":map[string]any{"qid":qid,"n":1}},
 }}}
 response := makeRequest(t, s, http.MethodPost, "/db/"+database+"/tx/commit", pull, token)
 var next TransactionResponse
 require.NoError(t, json.Unmarshal(response.Body.Bytes(), &next))
 require.NotEmpty(t, next.Errors, "failed delivery left its cursor alive: %s", response.Body.String())
}

func findDeliveredSearchQID(value any) string {
 switch v:=value.(type) {
 case map[string]any:
  if qid,ok:=v["qid"].(string); ok && qid!="" { return qid }
  for _,child:=range v { if qid:=findDeliveredSearchQID(child);qid!="" {return qid} }
 case []any:
  for _,child:=range v { if qid:=findDeliveredSearchQID(child);qid!="" {return qid} }
 }
 return ""
}

func TestSearchStreamDeliveryCypherFailedDeliveryCapacity(t *testing.T) {
 s, authenticator := setupTestServer(t)
 token := "Bearer " + getAuthToken(t, authenticator, "admin")
 database := s.dbManager.DefaultDatabaseName()
 engine, err := s.dbManager.GetStorage(database)
 require.NoError(t, err)
 for _,id:=range []storage.NodeID{"sol-a","sol-b"} {
  _,err=engine.CreateNode(&storage.Node{ID:id});require.NoError(t,err)
 }
 for i:=0;i<65;i++ {
  body:=fmt.Sprintf(`{"statements":[{"statement":"CALL db.retrieve({mode: 'id', n: 1, repetition: %d})"}]}`,i)
  request:=httptest.NewRequest(http.MethodPost,"/db/"+database+"/tx/commit",strings.NewReader(body))
  request.Header.Set("Authorization",token);request.Header.Set("Content-Type","application/json")
  writer:=disconnectedSearchWriter{httptest.NewRecorder()}
  s.buildRouter().ServeHTTP(writer,request)
  var result TransactionResponse
  require.NoError(t,json.Unmarshal(writer.Body.Bytes(),&result))
  require.Empty(t,result.Errors,"failed delivery %d exhausted registry capacity: %s",i,writer.Body.String())
 }
}

