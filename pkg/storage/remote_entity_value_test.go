package storage

import (
	"encoding/json"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/require"
)

func TestRemoteBoltEntityProvenance(t *testing.T) {
	node := dbtype.Node{ElementId: "4:remote:n", Labels: []string{"Doc"}, Props: map[string]interface{}{"id": "node-id", "name": "Remote"}}
	edge := dbtype.Relationship{ElementId: "5:remote:r", Type: "LINKS", StartElementId: node.ElementId, EndElementId: "4:remote:other", Props: map[string]interface{}{"id": "rel-id", "weight": 2}}
	plain := map[string]interface{}{"elementId": "forged", "labels": []interface{}{"Fake"}, "properties": map[string]interface{}{"name": "ordinary"}}
	result := normalizeBoltValue(map[string]interface{}{"nested": []interface{}{node, edge, plain}}).(map[string]interface{})["nested"].([]interface{})
	remoteNode := result[0].(RemoteEntityValue)
	remoteEdge := result[1].(RemoteEntityValue)
	require.Equal(t, node.ElementId, remoteNode["elementId"])
	require.Equal(t, edge.ElementId, remoteEdge["elementId"])
	require.IsType(t, map[string]interface{}{}, result[2])
	convertedNode, err := valueToNode(remoteNode)
	require.NoError(t, err)
	require.Equal(t, NodeID("node-id"), convertedNode.ID)
	require.Equal(t, node.Props, convertedNode.Properties)
	convertedEdge, err := valueToEdge(remoteEdge)
	require.NoError(t, err)
	require.Equal(t, EdgeID("rel-id"), convertedEdge.ID)
	require.Equal(t, NodeID(node.ElementId), convertedEdge.StartNode)
	wire, err := json.Marshal(remoteNode)
	require.NoError(t, err)
	require.JSONEq(t, `{"elementId":"4:remote:n","id":"node-id","labels":["Doc"],"properties":{"id":"node-id","name":"Remote"}}`, string(wire))
}
