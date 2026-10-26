// Author: Lane #1644 (Codex). Public transport request, with a complete
// healthy result and an unrecovered required related-node read afterwards.
package mcp
import("context";"errors";"sync/atomic";"testing";"github.com/orneryd/nornicdb/pkg/nornicdb";"github.com/orneryd/nornicdb/pkg/storage";"github.com/stretchr/testify/require";"github.com/orneryd/nornicdb/pkg/auth")
type foundationRelatedFault struct {storage.Engine; active atomic.Bool; visits atomic.Int64; cause error}
func(v *foundationRelatedFault)Namespace()string{return storage.DatabaseOf(v.Engine)}
func(v *foundationRelatedFault)GetOutgoingEdges(id storage.NodeID)([]*storage.Edge,error){if v.active.Load(){v.visits.Add(1);return nil,v.cause};return v.Engine.GetOutgoingEdges(id)}
func(v *foundationRelatedFault)GetIncomingEdges(id storage.NodeID)([]*storage.Edge,error){if v.active.Load(){v.visits.Add(1);return nil,v.cause};return v.Engine.GetIncomingEdges(id)}
func TestFoundationPublicRelatedOutcome(t *testing.T){
db,err:=nornicdb.Open("",nil);require.NoError(t,err);defer db.Close()
engine:=db.GetStorage()
for _,id:=range []storage.NodeID{"a","b"}{_,err:=engine.CreateNode(&storage.Node{ID:id,Labels:[]string{"Memory"},Properties:map[string]any{"title":"foundationzephyr","content":"foundationzephyr unique content"}});require.NoError(t,err)}
require.NoError(t,engine.CreateEdge(&storage.Edge{ID:"ab",StartNode:"a",EndNode:"b",Type:"R"}))
require.NoError(t,db.BuildSearchIndexes(context.Background()))

view:=&foundationRelatedFault{Engine:engine,cause:errors.New("required related-node read failed")}
router:=NewServer(db,nil);router.SetDatabaseScopedStorage(func(string)(storage.Engine,error){return view,nil})
ctx:=ContextWithDatabase(auth.WithRequestAuthenticationDisabled(context.Background()),"nornic")
request:=map[string]interface{}{"query":"foundationzephyr","depth":2,"limit":5}
healthy,err:=router.CallTool(ctx,ToolDiscover,request);require.NoError(t,err);good:=healthy.(DiscoverResult);require.NotEmpty(t,good.Results);require.NotEmpty(t,good.Results[0].Related)
view.active.Store(true)
result,err:=router.CallTool(ctx,ToolDiscover,request)
require.Positive(t,view.visits.Load());require.ErrorIs(t,err,view.cause,"public MCP discover must not return a successful partial graph: %#v",result)
}
