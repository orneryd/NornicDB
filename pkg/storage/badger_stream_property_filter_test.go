package storage

// A projected node scan with a PropertyFilter skips nodes whose projected
// properties the filter rejects, before decoding the rest of them (#824).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerStreamNodesPropertyFilter(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, node := range []*Node{
		{ID: "ns:a", Labels: []string{"Code"}, Properties: map[string]interface{}{"id": "a", "body": "x"}},
		{ID: "ns:b", Labels: []string{"Code"}, Properties: map[string]interface{}{"id": "b", "body": "y"}},
		{ID: "ns:c", Labels: []string{"Other"}, Properties: map[string]interface{}{"body": "z"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	stream := func(opts StreamNodesOptions) []*Node {
		t.Helper()
		var visited []*Node
		require.NoError(t, engine.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
			visited = append(visited, node)
			return nil
		}))
		return visited
	}
	wantB := func(props map[string]interface{}) bool { return props["id"] == "b" }

	visited := stream(StreamNodesOptions{Projection: []string{"id"}, PropertyFilter: wantB})
	require.Len(t, visited, 1)
	require.Equal(t, NodeID("ns:b"), visited[0].ID)
	require.Equal(t, []string{"Code"}, visited[0].Labels)
	require.Equal(t, map[string]interface{}{"id": "b"}, visited[0].Properties)

	// The prefix scan applies it too; without a filter every node is visited.
	require.Len(t, stream(StreamNodesOptions{Prefix: "ns:", Projection: []string{"id"}, PropertyFilter: wantB}), 1)
	require.Len(t, stream(StreamNodesOptions{Projection: []string{"id"}}), 3)
}

func TestDecodeNodeRejectsMalformedBodies(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	scan := newProjectedNodeDecoder(engine, []string{"id"}, nil)
	for name, data := range map[string][]byte{
		"empty":          {},
		"format byte":    {0x01},
		"length varint":  {nodeFormatTokenizedV1, 0xff},
		"truncated":      {nodeFormatTokenizedV1, 0x05, 0x01},
		"bad properties": {nodeFormatTokenizedV1, 0x02, 0xff, 0xff},
		"bad body":       {nodeFormatTokenizedV1, 0x01, 0x00, 0xc1},
	} {
		node, err := engine.decodeNodeProjected("ns", data, nil)
		require.Error(t, err, name)
		require.Nil(t, node, name)
		node, err = scan.decode([]byte("ns:n1"), data)
		require.Error(t, err, name)
		require.Nil(t, node, name)
	}
}
