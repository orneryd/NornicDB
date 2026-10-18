package server

import (
 "bytes"
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "testing"

 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func BenchmarkSearchStreamHTTPTransport(b *testing.B) {
 s,users:=setupTestServer(b)
 token:="Bearer "+getAuthToken(b,users,"admin")
 database:=s.dbManager.DefaultDatabaseName()
 store,err:=s.dbManager.GetStorage(database);require.NoError(b,err)
 for _,id:=range []storage.NodeID{"a","b"} { _,err=store.CreateNode(&storage.Node{ID:id});require.NoError(b,err) }
 server:=httptest.NewServer(s.buildRouter());b.Cleanup(server.Close)
 client:=server.Client()
 b.Run("read_to_end",func(b *testing.B) {
  b.ReportAllocs()
  for i:=0;i<b.N;i++ {
   request:=map[string]any{"database":database,"mode":"id","n":1}
   for {
    body,err:=json.Marshal(request);if err!=nil {b.Fatal(err)}
    req,err:=http.NewRequest(http.MethodPost,server.URL+"/nornicdb/search",bytes.NewReader(body));if err!=nil{b.Fatal(err)}
    req.Header.Set("Authorization",token);req.Header.Set("Content-Type","application/json")
    response,err:=client.Do(req);if err!=nil{b.Fatal(err)}
    var page struct {QID string `json:"qid"`;HasMore bool `json:"has_more"`}
    err=json.NewDecoder(response.Body).Decode(&page)
    closeErr:=response.Body.Close()
    if err!=nil||closeErr!=nil||response.StatusCode!=http.StatusOK {b.Fatalf("response: status=%d decode=%v close=%v",response.StatusCode,err,closeErr)}
    if !page.HasMore {break}
    request=map[string]any{"qid":page.QID,"n":1}
   }
  }
 })
}

