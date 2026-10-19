package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A listener may write synchronously as soon as replay completes. Every read
// source must then use the new committed state, including stored reads and
// property indexes, rather than the generation that caused the notification.
func TestWriteBehindReplayListenerRetiresEveryReadSource(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		for _, deleted := range []bool{false, true} {
			name := kind + "/update"
			if deleted {
				name = kind + "/delete"
			}
			t.Run(name, func(t *testing.T) {
				b := newWriteBehindTestEngine(t, time.Hour)
				b.writeBehind.Close()
				for _, id := range []NodeID{"test:a", "test:b"} {
					_, err := b.CreateNode(&Node{ID: id})
					require.NoError(t, err)
				}
				schema := b.GetSchemaForNamespace("test")
				require.NoError(t, schema.AddPropertyIndex("retired_v", "Identity", []string{"v"}))
				called := false
				check := func(id string, properties map[string]any, err error, found bool, source string) {
					t.Helper()
					if deleted {
						assert.False(t, found, "%s retained %s", source, id)
						assert.True(t, err == nil || errors.Is(err, ErrNotFound), "%s: %v", source, err)
					} else if assert.NoError(t, err, source) && assert.True(t, found, source) {
						assert.Equal(t, "new", properties["v"], source)
					}
				}
				if kind == "node" {
					b.OnNodeCreated(func(node *Node) {
						if node.ID != "test:own" {
							return
						}
						called = true
						if deleted {
							if !assert.NoError(t, b.DeleteNode(node.ID)) {
								return
							}
						} else {
							next := CopyNode(node)
							next.Properties = map[string]any{"v": "new"}
							if !assert.NoError(t, b.UpdateNode(next)) {
								return
							}
						}
						for source, get := range map[string]func(NodeID) (*Node, error){
							"point": b.GetNode, "stored": b.GetStoredNode,
							"projected": func(id NodeID) (*Node, error) { return b.GetNodeProjected(id, []string{"v"}) },
						} {
							n, err := get(node.ID)
							var properties map[string]any
							if n != nil {
								properties = n.Properties
							}
							check(string(node.ID), properties, err, n != nil, source)
						}
						for source, get := range map[string]func(string) ([]*Node, error){"label": b.GetNodesByLabel, "stored-label": b.GetStoredNodesByLabel} {
							nodes, err := get("Identity")
							assert.NoError(t, err, source)
							assert.Len(t, nodes, map[bool]int{false: 1, true: 0}[deleted], source)
							for _, n := range nodes {
								check(string(n.ID), n.Properties, nil, true, source)
							}
						}
						assert.Empty(t, schema.PropertyIndexLookup("Identity", "v", "old"))
						ids := schema.PropertyIndexLookup("Identity", "v", "new")
						if deleted {
							assert.Empty(t, ids)
						} else {
							assert.Equal(t, []NodeID{node.ID}, ids)
						}
					})
					bufferedCreate(t, b, &Node{ID: "test:own", Labels: []string{"Identity"}, Properties: map[string]any{"v": "old"}})
				} else {
					b.OnEdgeCreated(func(edge *Edge) {
						called = true
						if deleted {
							if !assert.NoError(t, b.DeleteEdge(edge.ID)) {
								return
							}
						} else {
							next := CopyEdge(edge)
							next.Properties = map[string]any{"v": "new"}
							if !assert.NoError(t, b.UpdateEdge(next)) {
								return
							}
						}
						for source, get := range map[string]func(EdgeID) (*Edge, error){"point": b.GetEdge, "stored": b.GetStoredEdge} {
							e, err := get(edge.ID)
							var properties map[string]any
							if e != nil {
								properties = e.Properties
							}
							check(string(edge.ID), properties, err, e != nil, source)
						}
						for source, get := range map[string]func() ([]*Edge, error){
							"type": func() ([]*Edge, error) { return b.GetEdgesByType("LINK") },
							"stored-type": func() ([]*Edge, error) { return b.GetStoredEdgesByType("LINK") },
							"outgoing": func() ([]*Edge, error) { return b.GetOutgoingEdges("test:a") },
							"stored-outgoing": func() ([]*Edge, error) { return b.GetStoredOutgoingEdges("test:a") },
							"incoming": func() ([]*Edge, error) { return b.GetIncomingEdges("test:b") },
							"stored-incoming": func() ([]*Edge, error) { return b.GetStoredIncomingEdges("test:b") },
							"between": func() ([]*Edge, error) { return b.GetEdgesBetween("test:a", "test:b") },
						} {
							edges, err := get()
							assert.NoError(t, err, source)
							assert.Len(t, edges, map[bool]int{false: 1, true: 0}[deleted], source)
							for _, e := range edges {
								check(string(e.ID), e.Properties, nil, true, source)
							}
						}
					})
					bufferedCreateEdge(t, b, &Edge{ID: "test:own", StartNode: "test:a", EndNode: "test:b", Type: "LINK", Properties: map[string]any{"v": "old"}})
				}
				require.NoError(t, b.FlushWriteBehind())
				require.True(t, called)
			})
		}
	}
}
