package cypher

import (
    "context"
    "fmt"
    "testing"

    "github.com/orneryd/nornicdb/pkg/storage"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

// The same local IDs exist in both sources. Every operation binds native values
// from both, then mutates only the second source; row refresh and deleted views
// must retain that choice just as storage reads and relationship endpoints do.
func TestCompositeMutationIdentityMatrix(t *testing.T) {
    cases := []struct {
        name, clause string
        rows [][]interface{}
        nodeDeleted, edgeDeleted, edgeCreated bool
        nodeProperty, edgeProperty string
        removed bool
    }{
        {name:"set node",clause:"SET m.changed = 42 RETURN n.changed,m.changed",rows:[][]interface{}{{nil,int64(42)}},nodeProperty:"changed"},
        {name:"remove node property",clause:"REMOVE m.v RETURN n.v,m.v",rows:[][]interface{}{{int64(7),nil}},nodeProperty:"v",removed:true},
        {name:"set relationship",clause:"SET s.changed = 42 RETURN r.changed,s.changed",rows:[][]interface{}{{nil,int64(42)}},edgeProperty:"changed"},
        {name:"delete relationship",clause:"DELETE s RETURN r.marker,elementId(s)",rows:[][]interface{}{{"one","5:two:same-rel"}},edgeDeleted:true},
        {name:"detach delete node",clause:"DETACH DELETE m RETURN n.v,properties(m)",rows:[][]interface{}{{int64(7),map[string]interface{}{}}},nodeDeleted:true,edgeDeleted:true},
        {name:"create relationship",clause:"CREATE (m)-[made:MADE]->(y) RETURN elementId(startNode(made)),elementId(endNode(made))",rows:[][]interface{}{{"4:two:same","4:two:target"}},edgeCreated:true},
        {name:"merge relationship",clause:"MERGE (m)-[made:MADE]->(y) RETURN elementId(startNode(made)),elementId(endNode(made))",rows:[][]interface{}{{"4:two:same","4:two:target"}},edgeCreated:true},
    }
    for _, testCase := range cases {
        for _, mode := range []string{"autocommit","commit","rollback"} {
            t.Run(fmt.Sprintf("%s/%s",testCase.name,mode),func(t *testing.T){
                exec,mgr:=newCompositeIdentityExecutor(t,false)
                for _, database:=range []string{"one","two"} {
                    store,err:=mgr.GetStorage(database)
                    require.NoError(t,err)
                    edge,err:=store.GetEdge("same-rel")
                    require.NoError(t,err)
                    edge.Properties=map[string]interface{}{"marker":database}
                    require.NoError(t,store.UpdateEdge(edge))
                }
                ctx:=context.Background()
                if mode!="autocommit" {
                    store,err:=mgr.GetStorage("comp")
                    require.NoError(t,err)
                    exec=NewStorageExecutor(store)
                    exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager:mgr})
                    _,err=exec.Execute(ctx,"BEGIN",nil)
                    require.NoError(t,err)
                    t.Cleanup(func(){_,_=exec.Execute(ctx,"ROLLBACK",nil)})
                }
                result,err:=exec.Execute(ctx,compositeIdentityQueryPrefix+testCase.clause,nil)
                require.NoError(t,err)
                assert.Equal(t,testCase.rows,result.Rows)
                if mode!="autocommit" {
                    terminal:="COMMIT"
                    if mode=="rollback" {terminal="ROLLBACK"}
                    _,err=exec.Execute(ctx,terminal,nil)
                    require.NoError(t,err)
                }
                for _, database:=range []string{"one","two"} {
                    store,err:=mgr.GetStorage(database)
                    require.NoError(t,err)
                    changed:=database=="two" && mode!="rollback"
                    node,err:=store.GetNode("same")
                    if changed && testCase.nodeDeleted {require.ErrorIs(t,err,storage.ErrNotFound)} else {
                        require.NoError(t,err)
                        if testCase.nodeProperty!="" {
                            expected:=interface{}(nil)
                            if testCase.nodeProperty=="v" {expected=int64(7)}
                            if changed {if testCase.removed {expected=nil} else {expected=int64(42)}}
                            require.Equal(t,expected,node.Properties[testCase.nodeProperty])
                        }
                    }
                    edge,err:=store.GetEdge("same-rel")
                    if changed && testCase.edgeDeleted {require.ErrorIs(t,err,storage.ErrNotFound)} else {
                        require.NoError(t,err)
                        require.Equal(t,database,edge.Properties["marker"])
                        if testCase.edgeProperty!="" {
                            expected:=interface{}(nil)
                            if changed {expected=int64(42)}
                            require.Equal(t,expected,edge.Properties[testCase.edgeProperty])
                        }
                    }
                    made,err:=store.GetEdgesByType("MADE")
                    require.NoError(t,err)
                    want:=0
                    if changed && testCase.edgeCreated {want=1}
                    require.Len(t,made,want)
                    if want>0 {require.Equal(t,storage.NodeID("same"),made[0].StartNode);require.Equal(t,storage.NodeID("target"),made[0].EndNode)}
                }
            })
        }
    }
}
