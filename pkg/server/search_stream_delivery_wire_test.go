package server

import (
 "encoding/json"
 "io"
 "net/http"
 "net/http/httptest"
 "strings"
 "testing"

 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

type finalPageFlushWriter struct{ *httptest.ResponseRecorder }
func (w finalPageFlushWriter) FlushError() error { return io.ErrClosedPipe }

func TestSearchStreamDeliveryHTTPFlush(t *testing.T) {
 s,users:=setupTestServer(t)
 token:="Bearer "+getAuthToken(t,users,"admin")
 database:=s.dbManager.DefaultDatabaseName()
 store,err:=s.dbManager.GetStorage(database);require.NoError(t,err)
 for _,id:=range []storage.NodeID{"a","b","c"} { _,err=store.CreateNode(&storage.Node{ID:id});require.NoError(t,err) }
 request:=httptest.NewRequest(http.MethodPost,"/nornicdb/search",strings.NewReader(`{"mode":"id","n":1}`))
 request.Header.Set("Authorization",token);request.Header.Set("Content-Type","application/json")
 writer:=finalPageFlushWriter{httptest.NewRecorder()}
 s.buildRouter().ServeHTTP(writer,request)
 var page map[string]any;require.NoError(t,json.Unmarshal(writer.Body.Bytes(),&page))
 qid:=findDeliveredSearchQID(page);require.NotEmpty(t,qid)
 response:=makeRequest(t,s,http.MethodPost,"/nornicdb/search",map[string]any{"qid":qid,"n":1},token)
 require.Equal(t,http.StatusGone,response.Code,"failed final flush left cursor alive: %s",response.Body.String())
}

func TestSearchStreamDeliveryGraphQLHiddenPage(t *testing.T) {
 s,users:=setupTestServer(t)
 token:="Bearer "+getAuthToken(t,users,"admin")
 database:=s.dbManager.DefaultDatabaseName()
 store,err:=s.dbManager.GetStorage(database);require.NoError(t,err)
 for _,id:=range []storage.NodeID{"a","b"} { _,err=store.CreateNode(&storage.Node{ID:id});require.NoError(t,err) }
 for i:=0;i<65;i++ {
  payload:=map[string]any{"query":`{cypher(input:{statement:"CALL db.retrieve({mode:'id',n:1})"}){rowCount}}`}
  response:=makeRequest(t,s,http.MethodPost,"/graphql",payload,token)
  var value map[string]any;require.NoError(t,json.Unmarshal(response.Body.Bytes(),&value),response.Body.String())
  require.Empty(t,value["errors"],"response %d: unexposed cursors must not consume owner admission: %v",i,value)
 }
}

