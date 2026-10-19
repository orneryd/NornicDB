package storage

import (
    "testing"

    "github.com/stretchr/testify/require"
)

func TestCompositeNativeIdentityWriteBoundary(t *testing.T) {
    for _, scoped := range []bool{false, true} {
        for _, opaque := range []string{"raw:same", "4:one:same"} {
            t.Run(opaque+map[bool]string{false:"/raw",true:"/namespaced"}[scoped],func(t *testing.T){
                stores := map[string]Engine{}
                for _, database := range []string{"one","two"} {
                    base := NewMemoryEngine()
                    t.Cleanup(func(){_ = base.Close()})
                    var engine Engine = base
                    if scoped { engine = NewNamespacedEngine(base,database) }
                    stores[database] = engine
                    _,err := engine.CreateNode(&Node{ID:NodeID(opaque),Labels:[]string{"P"},Properties:map[string]interface{}{"v":int64(7)}})
                    require.NoError(t,err)
                    _,err = engine.CreateNode(&Node{ID:"raw:target",Labels:[]string{"Q"}})
                    require.NoError(t,err)
                    require.NoError(t,engine.CreateEdge(&Edge{ID:"raw:rel",StartNode:NodeID(opaque),EndNode:"raw:target",Type:"R"}))
                }
                composite := NewCompositeEngine(stores,map[string]string{"one":"one","two":"two"},map[string]string{"one":"read_write","two":"read_write"})
                composite.SetCompositeName("comp")
                node,err := composite.GetNode(NodeID("4:two:"+opaque))
                require.NoError(t,err)
                require.Equal(t,"two",node.Database)
                node.Properties["mark"] = int64(99)
                require.NoError(t,composite.UpdateNode(node))
                first,err := stores["one"].GetNode(NodeID(opaque));require.NoError(t,err)
                second,err := stores["two"].GetNode(NodeID(opaque));require.NoError(t,err)
                require.Nil(t,first.Properties["mark"])
                require.Equal(t,int64(99),second.Properties["mark"])
                edge,err := composite.GetEdge("5:two:raw:rel");require.NoError(t,err)
                edge.Properties = map[string]interface{}{"mark":int64(99)}
                require.NoError(t,composite.UpdateEdge(edge))
                firstEdge,err := stores["one"].GetEdge("raw:rel");require.NoError(t,err)
                secondEdge,err := stores["two"].GetEdge("raw:rel");require.NoError(t,err)
                require.Nil(t,firstEdge.Properties["mark"])
                require.Equal(t,int64(99),secondEdge.Properties["mark"])
                target,err := composite.GetNode("4:two:raw:target");require.NoError(t,err)
                created := &Edge{ID:"raw:new",Type:"S"}
                require.NoError(t,BindRelationshipEndpoints(created,node,target))
                require.NoError(t,composite.BulkCreateEdges([]*Edge{created}))
                _,err = stores["one"].GetEdge(created.ID);require.ErrorIs(t,err,ErrNotFound)
                persisted,err := stores["two"].GetEdge(created.ID);require.NoError(t,err)
                require.Equal(t,NodeID(opaque),persisted.StartNode)
                foreign,err := composite.GetNode(NodeID("4:one:"+opaque));require.NoError(t,err)
                require.Error(t,BindRelationshipEndpoints(&Edge{ID:"raw:cross",Type:"CROSS"},node,foreign))
                require.NoError(t,composite.DeleteEdge("5:two:raw:rel"))
                _,err = stores["one"].GetEdge("raw:rel");require.NoError(t,err)
                _,err = stores["two"].GetEdge("raw:rel");require.ErrorIs(t,err,ErrNotFound)
                require.NoError(t,composite.BulkDeleteEdges([]EdgeID{"5:two:raw:new"}))
                require.NoError(t,composite.BulkDeleteNodes([]NodeID{NodeID("4:two:"+opaque)}))
                _,err = stores["one"].GetNode(NodeID(opaque));require.NoError(t,err)
                _,err = stores["two"].GetNode(NodeID(opaque));require.ErrorIs(t,err,ErrNotFound)
            })
        }
    }
}