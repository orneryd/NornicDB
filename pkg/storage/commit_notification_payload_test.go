package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNotificationPayloadOwnsDeliveredImagesOnly(t *testing.T) {
	node := &Node{ID: "review:node", Properties: map[string]any{"value": "accepted"}}
	edge := &Edge{ID: "review:edge", Properties: map[string]any{"value": "accepted"}}
	oldNode := &Node{ID: node.ID, Properties: map[string]any{"value": "before"}}
	oldEdge := &Edge{ID: edge.ID, Properties: map[string]any{"value": "before"}}
	source := []Operation{{Type: OpUpdateNode, Node: node, OldNode: oldNode}, {Type: OpUpdateEdge, Edge: edge, OldEdge: oldEdge}, {Type: OpDeleteNode, NodeID: node.ID, OldNode: oldNode, DeletedEdgeIDs: []EdgeID{edge.ID}, DeletedEdgeFacts: []AffectedFact{{}}}}
	notification := &commitNotification{}
	capturePublicationOperations(notification, source)
	node.Properties["value"] = "changed"
	edge.Properties["value"] = "changed"
	source[2].DeletedEdgeIDs[0] = "review:other"
	require.Equal(t, "accepted", notification.ops[0].Node.Properties["value"])
	require.Equal(t, "accepted", notification.ops[1].Edge.Properties["value"])
	require.Equal(t, []EdgeID{"review:edge"}, notification.ops[2].DeletedEdgeIDs)
	for _, operation := range notification.ops {
		require.Nil(t, operation.OldNode, "rollback images do not belong to listener delivery")
		require.Nil(t, operation.OldEdge)
		require.Nil(t, operation.DeletedEdgeFacts)
	}
	require.Same(t, oldNode, source[0].OldNode, "publication capture must not change the commit frame")
	require.Same(t, oldEdge, source[1].OldEdge)
	require.Len(t, source[2].DeletedEdgeFacts, 1)
}

func BenchmarkNotificationDeletePayload(b *testing.B) {
	operations := make([]Operation, 2000)
	for i := range operations {
		operations[i] = Operation{Type: OpDeleteNode, OldNode: &Node{Properties: map[string]any{"value": i}}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		notification := &commitNotification{}
		capturePublicationOperations(notification, operations)
		if len(notification.ops) != len(operations) {
			b.Fatal("lost operations")
		}
	}
}
