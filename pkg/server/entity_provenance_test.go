package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPEntityProvenance(t *testing.T) {
	server := &Server{}
	graph := &GraphResult{}
	state := &transactionHTTPValueState{graph: graph}
	for _, database := range []string{"one", "two"} {
		node := &storage.Node{ID: "same", Database: database, Properties: map[string]interface{}{"db": database}}
		edge := &storage.Edge{ID: "rel", Database: database, StartNode: "same", EndNode: "target", Type: "R", Properties: map[string]interface{}{}}
		_, meta := server.transactionHTTPValue(node, "comp", state)
		require.Equal(t, "4:"+database+":same", meta[0].(map[string]interface{})["elementId"])
		_, meta = server.transactionHTTPValue(edge, "comp", state)
		require.Equal(t, "5:"+database+":rel", meta[0].(map[string]interface{})["elementId"])
	}
	require.Len(t, graph.Nodes, 2)
	require.Len(t, graph.Relationships, 2)
	require.Equal(t, "4:one:same", graph.Relationships[0].StartNode)
	require.Equal(t, "4:two:target", graph.Relationships[1].EndNode)
}
