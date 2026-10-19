package cypher

import (
 "context"
 "fmt"
 "testing"
 "time"

 nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

// Counting real graph reads makes cancellation work observable without a
// wall-clock speed assertion or any parser-compilation timing assumption.
type boundedPathReadEngine struct {
 storage.Engine
 tick func()
}
func (store *boundedPathReadEngine) GetInnerEngine() storage.Engine {return store.Engine}
func (store *boundedPathReadEngine) GetOutgoingEdges(id storage.NodeID)([]*storage.Edge,error){store.tick(); return store.Engine.GetOutgoingEdges(id)}
func (store *boundedPathReadEngine) GetIncomingEdges(id storage.NodeID)([]*storage.Edge,error){store.tick(); return store.Engine.GetIncomingEdges(id)}

func TestStatementStopsAPOCPathSearchWithinBoundedWork(t *testing.T) {
 const population,trigger,bound=3000,200,8*pipelineCancellationInterval
 store:=storage.NewNamespacedEngine(newTestMemoryEngine(t),"bounded_paths")
 for index:=0;index<population;index++ {
  _,err:=store.CreateNode(&storage.Node{ID:storage.NodeID(fmt.Sprint(index)),Labels:[]string{"P"},Properties:map[string]interface{}{"v":int64(index)}}); require.NoError(t,err)
  if index>0 {require.NoError(t,store.CreateEdge(&storage.Edge{ID:storage.EdgeID(fmt.Sprint(index)),StartNode:storage.NodeID(fmt.Sprint(index-1)),EndNode:storage.NodeID(fmt.Sprint(index)),Type:"NEXT"}))}
 }
 var clock *boundedWorkClock
 counted:=&boundedPathReadEngine{Engine:store,tick:func(){clock.tick(nil)}}
 exec:=NewStorageExecutor(counted)
 for _,call:=range []string{
  "CALL apoc.algo.dijkstra(a,b,'NEXT>','w') YIELD weight RETURN weight",
  "CALL apoc.algo.aStar(a,b,'NEXT>','w','lat','lon') YIELD weight RETURN weight",
  "CALL apoc.algo.allSimplePaths(a,b,'NEXT>',3000) YIELD path RETURN path",
 } {
  query:="MATCH (a:P {v:0}), (b:P {v:2999}) "+call
  for _,mode:=range []string{"deadline","client_cancel","terminate"} {
   for _,pass:=range []string{"first","compiled"} {
    t.Run(call+"/"+mode+"/"+pass,func(t *testing.T){
     var ctx context.Context; var cancel context.CancelFunc
     if mode=="deadline" {ctx,cancel=context.WithTimeout(context.Background(),2*time.Second)} else {ctx,cancel=context.WithCancel(context.Background())}; defer cancel()
     clock=&boundedWorkClock{ctx:ctx,trigger:trigger,end:func(){switch mode {case "deadline":<-ctx.Done(); case "client_cancel":cancel(); default:terminateStatement(query)}}}
     _,err:=exec.Execute(ctx,query,nil)
     require.Error(t,err)
     switch mode {case "deadline":require.ErrorIs(t,err,context.DeadlineExceeded); case "client_cancel":require.ErrorIs(t,err,context.Canceled); default:code,_:=nornicerrors.Neo4jStatus(err); require.Equal(t,"Neo.ClientError.Transaction.Terminated",code)}
     reads,after:=clock.counts(); require.GreaterOrEqual(t,reads,trigger,"must reach cancellation during graph work"); require.LessOrEqual(t,after,bound,"graph work must stop within the shared probe budget")
    })
   }
  }
 }
}