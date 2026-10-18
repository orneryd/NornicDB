package bolt

import (
 "context"
 "testing"
 "github.com/orneryd/nornicdb/pkg/search"
 "github.com/stretchr/testify/require"
)

func TestSearchStreamDeliveryBoltHiddenPage(t *testing.T) {
 session,_,svc:=newPageDeliverySession(t,1)
 for i:=0;i<3;i++ {
  require.NoError(t,session.handleRun(buildRunMessageData("CALL db.retrieve({mode:'id',n:1}) YIELD page RETURN size(page.results) AS count",nil,nil)))
  require.NotNil(t,session.lastResult,"hidden page iteration %d must be admitted",i)
  require.Empty(t,session.lastResult.Metadata["durable_qid"],"the client received no cursor reference in RUN metadata")
  require.NoError(t,session.handlePull(encodePackStreamMap(map[string]any{"n":int64(-1)})))
  replacement,err:=startReplacementPage(svc);require.NoError(t,err,"completed projected result must release an unexposed cursor")
  _,err=svc.SearchTextContinuation(context.Background(),"",nil,search.SearchContinuationRequest{Owner:"replacement",QID:replacement.QID,Discard:true},nil,nil,nil,search.ChunkedSearchErrorPolicy{})
  require.NoError(t,err)
 }
}

