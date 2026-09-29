package server

// gh745_element_id_test.go — regression tests for #745 §2: HTTP row values
// and row meta must name the database the entity actually lives in, not a
// fixed "nornicdb".

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGh745_HTTPElementIDUsesRequestDatabase(t *testing.T) {
	s := &Server{}
	node := &storage.Node{ID: "n1", Labels: []string{"EI"}, Properties: map[string]interface{}{"k": int64(1)}}
	edge := &storage.Edge{ID: "e1", Type: "R", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{}}

	converted := s.convertRowToNeo4jFormat([]interface{}{node, edge}, "otherdb")
	require.Len(t, converted, 2)

	nodeMap, ok := converted[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "4:otherdb:n1", nodeMap["elementId"])

	edgeMap, ok := converted[1].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "5:otherdb:e1", edgeMap["elementId"])
	require.Equal(t, "4:otherdb:n1", edgeMap["startNodeElementId"])
	require.Equal(t, "4:otherdb:n2", edgeMap["endNodeElementId"])

	// The row meta echoes the same canonical element id.
	meta := s.generateRowMeta(converted)
	require.Len(t, meta, 2)
	nodeMeta, ok := meta[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "4:otherdb:n1", nodeMeta["elementId"])
	require.Equal(t, "node", nodeMeta["type"])
	edgeMeta, ok := meta[1].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "5:otherdb:e1", edgeMeta["elementId"])
	require.Equal(t, "relationship", edgeMeta["type"])
}
