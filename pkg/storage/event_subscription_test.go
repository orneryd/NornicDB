package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// subscriptionRecorder is an event-publishing engine that records the
// handlers subscribed to it.
type subscriptionRecorder struct {
	Engine
	handlers []EventHandlers
}

func (r *subscriptionRecorder) SubscribeEvents(handlers EventHandlers) error {
	r.handlers = append(r.handlers, handlers)
	return nil
}

// A subscription reaches the publishing engine through each storage wrapper;
// the namespaced view delivers its own namespace only, with stripped IDs, and
// the resets that cover it. An engine that cannot deliver events is an error.
func TestEventSubscriptionThroughStorageWrappers(t *testing.T) {
	for name, wrap := range map[string]func(Engine) Engine{
		"wal":    func(inner Engine) Engine { return &WALEngine{engine: inner} },
		"traced": func(inner Engine) Engine { return &TracedEngine{Engine: inner} },
	} {
		t.Run(name, func(t *testing.T) {
			rec := &subscriptionRecorder{}
			require.NoError(t, SubscribeEvents(wrap(rec), EventHandlers{NodeCreated: func(*Node) {}}))
			require.Len(t, rec.handlers, 1, "the wrapper did not forward the subscription")
			require.ErrorIs(t, SubscribeEvents(wrap(struct{ Engine }{}), EventHandlers{}), ErrEventSubscriptionUnsupported)
		})
	}

	rec := &subscriptionRecorder{}
	var nodes []NodeID
	var updatedNodes []NodeID
	var edgeBodies []*Edge
	var edges []EdgeID
	var resets []string
	require.NoError(t, SubscribeEvents(NewNamespacedEngine(rec, "tenant"), EventHandlers{
		NodeCreated: func(node *Node) { nodes = append(nodes, node.ID) },
		NodeUpdated: func(node *Node) {
			if node.ID == "panic" {
				panic("subscriber panic")
			}
			updatedNodes = append(updatedNodes, node.ID)
		},
		EdgeCreated:    func(edge *Edge) { edgeBodies = append(edgeBodies, edge) },
		EdgeUpdated:    func(edge *Edge) { edgeBodies = append(edgeBodies, edge) },
		NodeDeleted:    func(id NodeID) { nodes = append(nodes, id) },
		EdgeDeleted:    func(id EdgeID) { edges = append(edges, id) },
		NamespaceReset: func(prefix string) { resets = append(resets, prefix) },
	}))
	require.Len(t, rec.handlers, 1)
	inner := rec.handlers[0]
	inner.NodeCreated(&Node{ID: "tenant:a"})
	inner.NodeCreated(&Node{ID: "other:a"})
	inner.NodeUpdated(nil)
	inner.NodeUpdated(&Node{ID: "tenant:changed"})
	inner.NodeUpdated(&Node{ID: "other:changed"})
	source := &Edge{ID: "tenant:edge", StartNode: "tenant:start", EndNode: "tenant:end", Type: "REL"}
	inner.EdgeCreated(nil)
	inner.EdgeUpdated(nil)
	inner.EdgeCreated(source)
	inner.EdgeUpdated(source)
	inner.EdgeCreated(&Edge{ID: "other:edge"})
	inner.EdgeUpdated(&Edge{ID: "other:edge"})
	require.Equal(t, EdgeID("tenant:edge"), source.ID, "projection never mutates publisher's body")
	require.Len(t, edgeBodies, 2)
	for _, edge := range edgeBodies {
		require.Equal(t, EdgeID("edge"), edge.ID)
		require.Equal(t, NodeID("start"), edge.StartNode)
		require.Equal(t, NodeID("end"), edge.EndNode)
		require.Equal(t, "REL", edge.Type)
	}
	require.Equal(t, []NodeID{"changed"}, updatedNodes)
	require.PanicsWithValue(t, "subscriber panic", func() { inner.NodeUpdated(&Node{ID: "tenant:panic"}) }, "projection preserves subscriber panic propagation")

	inner.NodeDeleted("tenant:b")
	inner.EdgeDeleted("tenant:r")
	inner.EdgeDeleted("other:r")
	inner.NamespaceReset("tenant:")
	inner.NamespaceReset("tenant:x")
	inner.NamespaceReset("other:")
	require.Equal(t, []NodeID{"a", "b"}, nodes)
	require.Equal(t, []EdgeID{"r"}, edges)
	require.Equal(t, []string{"", "x"}, resets)
}
