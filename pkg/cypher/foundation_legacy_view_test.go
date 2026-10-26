// Author: Lane #1644 (Codex). The compatibility exception concerns error
// transport; every exported read still consumes its issued transaction view.
package cypher_test
import("context";"testing";"github.com/orneryd/nornicdb/pkg/cypher";"github.com/orneryd/nornicdb/pkg/storage";"github.com/stretchr/testify/require")
func TestFoundationPublicLegacyIssuedView(t *testing.T){
 base:=storage.NewMemoryEngine();defer base.Close();tenant:=storage.NewNamespacedEngine(base,"tenant");owner:=cypher.NewStorageExecutor(tenant);ctx:=context.Background()
 _,err:=owner.Execute(ctx,"BEGIN",nil);require.NoError(t,err);defer owner.Execute(ctx,"ROLLBACK",nil)
 _,err=owner.Execute(ctx,"CREATE (a:Doc {id:'a'})-[:R]->(b:Doc {id:'b'})",nil);require.NoError(t,err)
 issued,err:=owner.FunctionStorage("");require.NoError(t,err)
 nodes:=issued.GetAllNodes();require.Len(t,nodes,2)
 var a,b storage.NodeID
 for _,node:=range nodes{if node.Properties["id"]=="a"{a=node.ID};if node.Properties["id"]=="b"{b=node.ID}}
 require.NotEmpty(t,a);require.NotEmpty(t,b)
 require.NotNil(t,issued.GetEdgeBetween(a,b,"R"))
 require.Equal(t,1,issued.GetOutDegree(a),"legacy omission is not permission to read the committed base during BEGIN")
 require.Equal(t,1,issued.GetInDegree(b))
 require.Zero(t,tenant.GetOutDegree(a),"committed control remains empty")
}
