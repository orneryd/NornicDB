package storage

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeRemoteDatabaseNamesRetainPhysicalSource(t *testing.T) {
	sources:=map[string]Engine{}
	for _,alias:=range []string{"left","right"}{
		sources[alias]=makeEngineWithTransport(t,"http://"+alias+".example",func(*http.Request)(*http.Response,error){
			return &http.Response{StatusCode:http.StatusOK,Header:make(http.Header),Body:makeTXResponse([][]interface{}{{map[string]interface{}{
				"id":"shared", "labels":[]interface{}{"N"}, "properties":map[string]interface{}{"id":"shared","source":alias},
			}}},nil)},nil
		})
	}
	composite:=NewCompositeEngine(sources,map[string]string{"left":"remote_db","right":"remote_db"},map[string]string{"left":"read_write","right":"read_write"})
	composite.SetCompositeName("comp")
	t.Cleanup(func(){require.NoError(t,composite.Close())})
	nodes:=composite.GetAllNodes()
	require.Len(t,nodes,2)
	identities:=map[string]bool{}
	for _,node:=range nodes{identities[node.IdentityKey()]=true}
	require.Len(t,identities,2,"independent remotes may have the same database name and stored ID")
	for _,node:=range nodes{
		reloaded,err:=composite.GetNode(NodeID(node.ElementID("")))
		require.NoError(t,err)
		require.Equal(t,node.Properties["source"],reloaded.Properties["source"])
	}
}
