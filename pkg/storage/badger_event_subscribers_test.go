package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Every registered event callback runs, in registration order: a second
// subscriber (the GraphQL subscriptions) doesn't replace the first (the
// search indexes). A nil callback is ignored.
func TestBadgerEventCallbacksFanOut(t *testing.T) {
	engine := createTestBadgerEngine(t)
	var events []string
	for _, subscriber := range []string{"first", "second"} {
		engine.OnNodeCreated(func(node *Node) { events = append(events, subscriber+" created "+string(node.ID)) })
		engine.OnNodeUpdated(func(node *Node) { events = append(events, subscriber+" updated "+string(node.ID)) })
		engine.OnNodeDeleted(func(id NodeID) { events = append(events, subscriber+" deleted "+string(id)) })
		engine.OnEdgeCreated(func(edge *Edge) { events = append(events, subscriber+" created "+string(edge.ID)) })
		engine.OnEdgeUpdated(func(edge *Edge) { events = append(events, subscriber+" updated "+string(edge.ID)) })
		engine.OnEdgeDeleted(func(id EdgeID) { events = append(events, subscriber+" deleted "+string(id)) })
	}
	engine.OnNodeCreated(nil)
	engine.OnNodeUpdated(nil)
	engine.OnNodeDeleted(nil)
	engine.OnEdgeCreated(nil)
	engine.OnEdgeUpdated(nil)
	engine.OnEdgeDeleted(nil)

	engine.queueCommitOperations(nil, &Operation{Type: OpCreateNode, Node: &Node{ID: "test:n"}}, notificationImmediate)
	engine.queueCommitOperations(nil, &Operation{Type: OpUpdateNode, Node: &Node{ID: "test:n"}}, notificationImmediate)
	engine.queueCommitOperations(nil, &Operation{Type: OpDeleteNode, NodeID: "test:n"}, notificationImmediate)
	engine.queueCommitOperations(nil, &Operation{Type: OpCreateEdge, Edge: &Edge{ID: "test:r"}}, notificationImmediate)
	engine.queueCommitOperations(nil, &Operation{Type: OpUpdateEdge, Edge: &Edge{ID: "test:r"}}, notificationImmediate)
	engine.queueCommitOperations(nil, &Operation{Type: OpDeleteEdge, EdgeID: "test:r"}, notificationImmediate)
	require.Equal(t, []string{
		"first created test:n", "second created test:n",
		"first updated test:n", "second updated test:n",
		"first deleted test:n", "second deleted test:n",
		"first created test:r", "second created test:r",
		"first updated test:r", "second updated test:r",
		"first deleted test:r", "second deleted test:r",
	}, events)
}
