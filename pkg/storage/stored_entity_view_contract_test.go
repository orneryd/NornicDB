package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStoredEntityViewUsesStoredInclusiveTypedOverlay(t *testing.T) {
	b := newTestEngine(t)
	const id = "test:same"
	_, err := b.CreateNode(&Node{ID: NodeID(id), Properties: map[string]any{"value": "physical"}})
	require.NoError(t, err)
	require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(id), StartNode: NodeID(id), EndNode: NodeID(id), Type: "R", Properties: map[string]any{"value": "physical"}}))
	b.markManualSuppression(manualSuppressionNode, id, true)
	b.markManualSuppression(manualSuppressionEdge, id, true)
	w := NewWriteBehindBuffer(time.Hour, 1000, func(*CommitBuffer) error { return nil })
	t.Cleanup(func() { w.Close() })
	require.NoError(t, w.Append(BufferedMutation{Kind: BufferedOpNode, Node: &Node{runtime: &nodeRuntime{metadata: NodeRuntimeMetadata{VisibilitySuppressed: true}}, ID: NodeID(id), Properties: map[string]any{"value": "acknowledged"}, ChunkEmbeddings: [][]float32{{1, 2}}}}))
	require.NoError(t, w.Append(BufferedMutation{Kind: BufferedOpEdge, Edge: &Edge{ID: EdgeID(id), StartNode: NodeID(id), EndNode: NodeID(id), Type: "R", VisibilitySuppressed: true, Properties: map[string]any{"value": "acknowledged"}}}))
	read, err := b.captureReadView(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(read.Close)
	view := storedEntityView{read: embeddingRecordReader{txn: read.txn}, acknowledged: w}
	w.mu.RLock()
	hiddenNode := w.entityStateLocked(bufferedNodeEntity, id, bufferedReadVisibility{engine: b, visibility: visibleRead})
	hiddenEdge := w.entityStateLocked(bufferedEdgeEntity, id, bufferedReadVisibility{engine: b, visibility: visibleRead})
	w.mu.RUnlock()
	require.True(t, hiddenNode.deleted)
	require.True(t, hiddenEdge.deleted, "control proves visible mode hides both bodies")
	node, err := view.node(b, NodeID(id))
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Equal(t, "acknowledged", node.Properties["value"])
	require.True(t, node.RuntimeMetadata().VisibilitySuppressed)
	require.Equal(t, [][]float32{{1, 2}}, node.ChunkEmbeddings)
	edge, err := view.edge(b, EdgeID(id))
	require.NoError(t, err)
	require.NotNil(t, edge)
	require.Equal(t, "acknowledged", edge.Properties["value"])
	require.True(t, edge.VisibilitySuppressed)
	node.Properties["value"] = "caller"
	node.ChunkEmbeddings[0][0] = 99
	edge.Properties["value"] = "caller"
	again, err := view.node(b, NodeID(id))
	require.NoError(t, err)
	require.Equal(t, "acknowledged", again.Properties["value"])
	require.Equal(t, float32(1), again.ChunkEmbeddings[0][0])
	againEdge, err := view.edge(b, EdgeID(id))
	require.NoError(t, err)
	require.Equal(t, "acknowledged", againEdge.Properties["value"])
	require.NoError(t, w.Append(BufferedMutation{Kind: BufferedOpDelete, ID: id}))
	node, err = view.node(b, NodeID(id))
	require.NoError(t, err)
	require.Nil(t, node)
	edge, err = view.edge(b, EdgeID(id))
	require.NoError(t, err)
	require.Nil(t, edge)
	physical, err := b.GetStoredNode(NodeID(id))
	require.NoError(t, err)
	require.Equal(t, "physical", physical.Properties["value"], "typed overlay tombstone masks pinned physical predecessor")
}
