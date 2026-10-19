package storage

import (
    "testing"
    "strings"

    "github.com/stretchr/testify/require"
)

func TestCompositeNativeIdentityWriteBoundary(t *testing.T) {
    for _, scoped := range []bool{false, true} {
        for _, opaque := range []string{"raw:same", "4:one:same"} {
            t.Run(opaque+map[bool]string{false:"/raw",true:"/namespaced"}[scoped],func(t *testing.T){
                prefix := strings.SplitN(opaque,":",2)[0]
                targetID := NodeID(prefix+":target")
                edgeID := EdgeID(prefix+":rel")
                newID := EdgeID(prefix+":new")
                stores := map[string]Engine{}
                for _, database := range []string{"one","two"} {
                    base := NewMemoryEngine()
                    t.Cleanup(func(){_ = base.Close()})
                    var engine Engine = base
                    if scoped { engine = NewNamespacedEngine(base,database) }
                    stores[database] = engine
                    _,err := engine.CreateNode(&Node{ID:NodeID(opaque),Labels:[]string{"P"},Properties:map[string]interface{}{"v":int64(7)}})
                    require.NoError(t,err)
                    _,err = engine.CreateNode(&Node{ID:targetID,Labels:[]string{"Q"}})
                    require.NoError(t,err)
                    require.NoError(t,engine.CreateEdge(&Edge{ID:edgeID,StartNode:NodeID(opaque),EndNode:targetID,Type:"R"}))
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
                edge,err := composite.GetEdge(EdgeID("5:two:"+string(edgeID)));require.NoError(t,err)
                edge.Properties = map[string]interface{}{"mark":int64(99)}
                require.NoError(t,composite.UpdateEdge(edge))
                firstEdge,err := stores["one"].GetEdge(edgeID);require.NoError(t,err)
                secondEdge,err := stores["two"].GetEdge(edgeID);require.NoError(t,err)
                require.Nil(t,firstEdge.Properties["mark"])
                require.Equal(t,int64(99),secondEdge.Properties["mark"])
                target,err := composite.GetNode(NodeID("4:two:"+string(targetID)));require.NoError(t,err)
                created := &Edge{ID:newID,Type:"S"}
                require.NoError(t,BindRelationshipEndpoints(created,node,target))
                require.NoError(t,composite.BulkCreateEdges([]*Edge{created}))
                _,err = stores["one"].GetEdge(created.ID);require.ErrorIs(t,err,ErrNotFound)
                persisted,err := stores["two"].GetEdge(created.ID);require.NoError(t,err)
                require.Equal(t,NodeID(opaque),persisted.StartNode)
                foreign,err := composite.GetNode(NodeID("4:one:"+opaque));require.NoError(t,err)
                require.Error(t,BindRelationshipEndpoints(&Edge{ID:EdgeID(prefix+":cross"),Type:"CROSS"},node,foreign))
                require.NoError(t,composite.DeleteEdge(EdgeID("5:two:"+string(edgeID))))
                _,err = stores["one"].GetEdge(edgeID);require.NoError(t,err)
                _,err = stores["two"].GetEdge(edgeID);require.ErrorIs(t,err,ErrNotFound)
                require.NoError(t,composite.BulkDeleteEdges([]EdgeID{EdgeID("5:two:"+string(newID))}))
                require.NoError(t,composite.BulkDeleteNodes([]NodeID{NodeID("4:two:"+opaque)}))
                _,err = stores["one"].GetNode(NodeID(opaque));require.NoError(t,err)
                _,err = stores["two"].GetNode(NodeID(opaque));require.ErrorIs(t,err,ErrNotFound)
            })
        }
    }
}