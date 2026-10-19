package server

import (
 "context"
 "encoding/json"
 "net/http"
 "net/http/httptest"
 "testing"

 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestRemoteDynamicHTTPModePreservesStandardUpdateCounters(t *testing.T) {
 server,authenticator:=setupTestServer(t)
 token:="Bearer "+getAuthToken(t,authenticator,"admin")
 endpoint:=httptest.NewServer(server.buildRouter()); defer endpoint.Close()
 for _,tc:=range []struct{name,body,mode string; updates bool}{
  {"read","RETURN 1 AS value","r",false},
  {"write","CREATE (:U5HTTPMode) RETURN 1 AS value","w",true},
  {"write without counters","UNWIND [] AS x CREATE (:U5HTTPMode) RETURN count(*) AS value","w",false},
 } { t.Run(tc.name,func(t *testing.T){
  engine,err:=storage.NewRemoteEngine(storage.RemoteEngineConfig{URI:endpoint.URL,Database:"nornic",AuthToken:token}); require.NoError(t,err)
  defer func(){ require.NoError(t,engine.Close()) }()
  tx,err:=engine.BeginCypherTx(context.Background()); require.NoError(t,err)
  query:="CALL apoc.cypher.run($body,{}) YIELD value RETURN value"
  _,_,err=tx.QueryCypher(context.Background(),query,map[string]interface{}{"body":tc.body}); require.NoError(t,err)
  mode,ok:=tx.(storage.RemoteCypherMode); require.True(t,ok)
  write,known:=mode.ResolvedWriteMode(); require.True(t,known); require.Equal(t,tc.mode!="r",write)
  require.NoError(t,tx.Rollback(context.Background()))
  response:=makeRequest(t,server,http.MethodPost,"/db/nornic/tx/commit",map[string]interface{}{"statements":[]interface{}{map[string]interface{}{"statement":query,"parameters":map[string]interface{}{"body":tc.body},"includeStats":true}}},token)
  require.Equal(t,http.StatusOK,response.Code)
  var result TransactionResponse; require.NoError(t,json.Unmarshal(response.Body.Bytes(),&result))
  require.Empty(t,result.Errors); require.Len(t,result.Results,1)
  require.Equal(t,tc.mode,result.Results[0].Stats.QueryType)
  require.Equal(t,tc.updates,result.Results[0].Stats.ContainsUpdates,"mode must not fabricate mutation counters")
 }) }
}