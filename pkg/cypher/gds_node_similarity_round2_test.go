package cypher

import (
 "context"
 "fmt"
 "math"
 "testing"

 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

type reviewEdge struct { from, to, kind string; props map[string]interface{} }

func reviewGraph(t *testing.T, props []map[string]interface{}, edges []reviewEdge, nodeProjection, relProjection string) *StorageExecutor {
 t.Helper()
 graphProjections = make(map[string]*GraphProjection)
 t.Cleanup(func(){ graphProjections = make(map[string]*GraphProjection) })
 engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "review1197")
 for i,p := range props {
  _,err := engine.CreateNode(&storage.Node{ID:storage.NodeID(fmt.Sprint(i)),Labels:[]string{"N"},Properties:p})
  require.NoError(t,err)
 }
 for i,r := range edges {
  require.NoError(t,engine.CreateEdge(&storage.Edge{ID:storage.EdgeID(fmt.Sprintf("e%03d",i)),StartNode:storage.NodeID(r.from),EndNode:storage.NodeID(r.to),Type:r.kind,Properties:r.props}))
 }
 exec := NewStorageExecutor(engine)
 _,err := exec.Execute(context.Background(),"CALL gds.graph.project('rev',"+nodeProjection+","+relProjection+")",nil)
 require.NoError(t,err)
 return exec
}

func reviewRows(t *testing.T,e *StorageExecutor,config string) [][]interface{} {
 t.Helper()
 result,err := e.Execute(context.Background(),"CALL gds.nodeSimilarity.stream('rev', {"+config+"})",nil)
 require.NoError(t,err)
 t.Logf("config=%s rows=%v",config,result.Rows)
 return result.Rows
}

func TestReviewRound2ParallelAcrossTypes(t *testing.T) {
 e := reviewGraph(t,make([]map[string]interface{},4),[]reviewEdge{
  {"0","2","Z",map[string]interface{}{"w":2.0}},
  {"0","2","A",map[string]interface{}{"w":1.0}},
  {"1","2","A",map[string]interface{}{"w":1.0}},
  {"1","3","A",map[string]interface{}{"w":1.0}},
 },"'N'","{A:{properties:'w'}, Z:{properties:'w'}}")
 for _,selection := range []string{"",",relationshipTypes:['A','Z']",",relationshipTypes:['Z','A']"} {
  t.Run(selection,func(t *testing.T){
   rows:=reviewRows(t,e,"relationshipWeightProperty:'w'"+selection)
   require.Len(t,rows,2)
   require.Equal(t,0.5,rows[0][2],"GDS 2.13.12 selected type A's weight")
  })
 }
}

func TestReviewRound2ComponentPrecision(t *testing.T) {
 e:=reviewGraph(t,[]map[string]interface{}{{"comp":int64(9007199254740992)},{"comp":int64(9007199254740993)},{"comp":int64(5)}},[]reviewEdge{{"0","2","R",nil},{"1","2","R",nil}},"{N:{properties:'comp'}}","'R'")
 require.Empty(t,reviewRows(t,e,"useComponents:'comp'"),"GDS treats these as different Long component IDs")
}

func TestReviewRound2ComponentDefault(t *testing.T) {
 e:=reviewGraph(t,[]map[string]interface{}{nil,{"comp":int64(math.MinInt64)},{"comp":int64(5)}},[]reviewEdge{{"0","2","R",nil},{"1","2","R",nil}},"{N:{properties:'comp'}}","'R'")
 require.Len(t,reviewRows(t,e,"useComponents:'comp'"),2,"GDS's missing Integer property defaults to MinInt64")
}

func TestReviewRound2ComponentType(t *testing.T) {
 e:=reviewGraph(t,[]map[string]interface{}{{"comp":1.2},{"comp":1.2},{"comp":5.5}},[]reviewEdge{{"0","2","R",nil},{"1","2","R",nil}},"{N:{properties:'comp'}}","'R'")
 result,err:=e.Execute(context.Background(),"CALL gds.nodeSimilarity.stream('rev',{useComponents:'comp'})",nil)
 t.Logf("rows=%v error=%v",result,err)
 require.Error(t,err,"GDS refuses DOUBLE components because it reads LONG values")
}

func TestReviewRound2UnprojectedStringID(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},3),[]reviewEdge{{"0","2","R",nil},{"1","2","R",nil}},"'N'","'R'")
 _,err:=e.storage.CreateNode(&storage.Node{ID:"9",Labels:[]string{"Outside"}})
 require.NoError(t,err)
 for _,filter:=range []string{"n","id(n)","[id(n)]"} {
  t.Run(filter,func(t *testing.T){
   result,err:=e.Execute(context.Background(),"MATCH (n:Outside) CALL gds.nodeSimilarity.filtered.stream('rev',{sourceNodeFilter:"+filter+"}) YIELD node1 RETURN node1",nil)
   t.Logf("result=%v error=%v",result,err)
   require.NoError(t,err,"A valid ID outside the projection should admit no nodes")
   require.Empty(t,result.Rows)
  })
 }
}

func TestReviewRound2FilterListMessages(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},3),[]reviewEdge{{"0","2","R",nil},{"1","2","R",nil}},"'N'","'R'")
 for _,tc:=range []struct{filter,want string}{
  {"[true,true]","Invalid types in list. Expected Longs or Nodes but found [Boolean, Boolean]"},
  {"[1,'x',2.5,'y']","Invalid types in list. Expected Longs or Nodes but found [String, Double, String]"},
  {"[{}]","Invalid types in list. Expected Longs or Nodes but found [HashMap]"},
 } {
  t.Run(tc.filter,func(t *testing.T){
   _,err:=e.Execute(context.Background(),"CALL gds.nodeSimilarity.filtered.stream('rev',{sourceNodeFilter:"+tc.filter+"})",nil)
   t.Logf("error=%v",err)
   require.ErrorContains(t,err,tc.want)
  })
 }
}

func TestReviewRound2MissingWeights(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},4),[]reviewEdge{
  {"0","2","R",map[string]interface{}{"w":1.0}},
  {"0","3","R",nil},
  {"1","2","R",map[string]interface{}{"w":1.0}},
 },"'N'","{R:{properties:'w'}}")
 for _,metric:=range []string{"JACCARD","OVERLAP","COSINE"} {
  t.Run(metric,func(t *testing.T){require.Empty(t,reviewRows(t,e,"relationshipWeightProperty:'w',similarityMetric:'"+metric+"',similarityCutoff:0"),"GDS retains missing weight as NaN, so the score is undefined")})
 }
}

func TestReviewRound2ConfirmedFixVariants(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},4),[]reviewEdge{{"0","0","R",nil},{"0","2","R",nil},{"1","0","R",nil},{"1","2","R",nil}},"'N'","'R'")
 require.Len(t,reviewRows(t,e,""),2)
 require.Equal(t,0.5,reviewRows(t,e,"")[0][2])
 require.Empty(t,reviewRows(t,e,"degreeCutoff:2"))
 _,err:=readNodeSimilarityConfig(&GraphProjection{},map[string]interface{}{"similarityCutoff":math.NaN()},false)
 require.ErrorContains(t,err,"NaN")
 for _,cutoff:=range []float64{math.Inf(1),math.Inf(-1),-0.1,1.1} {
  _,err=readNodeSimilarityConfig(&GraphProjection{},map[string]interface{}{"similarityCutoff":cutoff},false)
  require.Error(t,err)
 }
 for _,value:=range []interface{}{nil,[]interface{}{},[]int64{}} {
  filter,err:=readGDSNodeFilter(&GraphProjection{},value)
  require.NoError(t,err)
  require.True(t,filter.admits(&GraphProjection{},"0"))
 }
 for _,config:=range []map[string]interface{}{{"topK":int64(10),"bottomK":int64(1)},{"topK":int64(1),"bottomK":int64(10)},{"topK":int64(10),"bottomK":int64(10)},{"topN":int64(0),"bottomN":int64(1)},{"topN":int64(1),"bottomN":int64(0)}} {
  _,err=readNodeSimilarityConfig(&GraphProjection{},config,false)
  require.NoError(t,err)
 }
}

func TestReviewRound2ParallelAcrossTypeBoundary(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},4),[]reviewEdge{
  {"0","2","A",map[string]interface{}{"w":1.0}},
  {"0","3","A",map[string]interface{}{"w":1.0}},
  {"0","2","Z",map[string]interface{}{"w":2.0}},
  {"1","2","A",map[string]interface{}{"w":1.0}},
  {"1","3","A",map[string]interface{}{"w":1.0}},
 },"'N'","{A:{properties:'w'},Z:{properties:'w'}}")
 t.Run("unweighted",func(t *testing.T){require.InDelta(t,2.0/3,reviewRows(t,e,"")[0][2],1e-12,"GDS retains non-consecutive repetitions between relationship-type graphs")})
 t.Run("weighted",func(t *testing.T){require.Equal(t,0.5,reviewRows(t,e,"relationshipWeightProperty:'w'")[0][2])})
 t.Run("degree",func(t *testing.T){require.Empty(t,reviewRows(t,e,"upperDegreeCutoff:2"),"GDS gives node 0 degree 3 in this union graph")})
}

func TestReviewRound2InfiniteOverlapScore(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},5),[]reviewEdge{
  {"0","2","R",map[string]interface{}{"w":1.0}},
  {"0","3","R",map[string]interface{}{"w":-1.0}},
  {"1","2","R",map[string]interface{}{"w":1.0}},
  {"1","4","R",map[string]interface{}{"w":-1.0}},
 },"'N'","{R:{properties:'w'}}")
 rows:=reviewRows(t,e,"relationshipWeightProperty:'w',similarityMetric:'OVERLAP'")
 require.Len(t,rows,2,"GDS emits the positive infinite score")
 require.True(t,math.IsInf(rows[0][2].(float64),1))
}

func TestReviewRound2ZeroInfiniteWeightFixes(t *testing.T) {
 for _,w:=range []float64{0,math.Inf(1)} {
  t.Run(fmt.Sprint(w),func(t *testing.T){
   e:=reviewGraph(t,make([]map[string]interface{},3),[]reviewEdge{{"0","2","R",map[string]interface{}{"w":w}},{"1","2","R",map[string]interface{}{"w":w}}},"'N'","{R:{properties:'w'}}")
   for _,metric:=range []string{"JACCARD","OVERLAP","COSINE"} {
    for _,cutoff:=range []string{"",",similarityCutoff:0"} {
     require.Empty(t,reviewRows(t,e,"relationshipWeightProperty:'w',similarityMetric:'"+metric+"'"+cutoff))
    }
   }
  })
 }
}

func TestReviewRound2NumericStringIDFix(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},3),[]reviewEdge{{"0","2","R",nil},{"1","2","R",nil}},"'N'","'R'")
 for _,config:=range []string{"sourceNodeFilter:'0'","sourceNodeFilter:['0']","targetNodeFilter:'1'","targetNodeFilter:['1']","sourceNodeFilter:['0'],targetNodeFilter:['1']"} {
  result,err:=e.Execute(context.Background(),"CALL gds.nodeSimilarity.filtered.stream('rev',{"+config+"})",nil)
  require.NoError(t,err)
  require.Len(t,result.Rows,1)
  require.Equal(t,"0",result.Rows[0][0])
  require.Equal(t,"1",result.Rows[0][1])
 }
}

func TestReviewRound2EmptyWeightProperty(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},3),[]reviewEdge{{"0","2","R",map[string]interface{}{"w":1.0}},{"1","2","R",map[string]interface{}{"w":2.0}}},"'N'","{R:{properties:'w'}}")
 result,err:=e.Execute(context.Background(),"CALL gds.nodeSimilarity.stream('rev',{relationshipWeightProperty:''})",nil)
 t.Logf("result=%v error=%v",result,err)
 require.Error(t,err,"GDS reports that the empty weight property is not projected")
}

func TestReviewRound2SelfLoopMissingWeight(t *testing.T) {
 e:=reviewGraph(t,make([]map[string]interface{},3),[]reviewEdge{{"0","0","R",nil},{"0","2","R",map[string]interface{}{"w":1.0}},{"1","2","R",map[string]interface{}{"w":1.0}}},"'N'","{R:{properties:'w'}}")
 require.Len(t,reviewRows(t,e,"relationshipWeightProperty:'w'"),2,"GDS ignores the self-loop before reading its missing weight")
}
