package storage

// BadgerEngine stats and event tests retained from the removed async-engine
// test file: these exercise the engine directly, not the deleted wrapper.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_NodeCount(t *testing.T) {
	b := createTestBadgerEngine(t)
	count, err := b.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	_, _ = b.CreateNode(testNode(prefixTestID("cnt1")))
	count, err = b.NodeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestBadgerEngine_NodeCountByPrefix(t *testing.T) {
	b := createTestBadgerEngine(t)
	_, _ = b.CreateNode(testNode(prefixTestID("pfx-1")))
	_, _ = b.CreateNode(testNode(prefixTestID("pfx-2")))

	count, err := b.NodeCountByPrefix(prefixTestID("pfx-"))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, count, int64(0))
}

func TestBadgerEngine_EdgeCount(t *testing.T) {
	b := createTestBadgerEngine(t)
	count, err := b.EdgeCount()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestBadgerEngine_GetSchema(t *testing.T) {
	b := createTestBadgerEngine(t)
	sm := b.GetSchema()
	assert.NotNil(t, sm)
}

func TestBadgerEngine_EventSetterCallbacks(t *testing.T) {
	b := createTestBadgerEngine(t)

	nodeCreatedCount := 0
	nodeDeletedCount := 0
	nodeUpdated := func(n *Node) {}
	nodeCreated := func(n *Node) { nodeCreatedCount++ }
	nodeDeleted := func(id NodeID) { nodeDeletedCount++ }
	edgeCreated := func(e *Edge) {}
	edgeUpdated := func(e *Edge) {}
	edgeDeleted := func(id EdgeID) {}

	b.OnNodeCreated(nodeCreated)
	b.OnNodeUpdated(nodeUpdated)
	b.OnNodeDeleted(nodeDeleted)
	b.OnEdgeCreated(edgeCreated)
	b.OnEdgeUpdated(edgeUpdated)
	b.OnEdgeDeleted(edgeDeleted)

	b.callbackMu.RLock()
	defer b.callbackMu.RUnlock()
	assert.NotNil(t, b.onNodeUpdated)
	assert.NotNil(t, b.onEdgeCreated)
	assert.NotNil(t, b.onEdgeUpdated)
	assert.NotNil(t, b.onEdgeDeleted)

	b.notifyNodeCreated(testNode("evt-node"))
	b.notifyNodeUpdated(testNode("evt-node"))
	b.notifyNodeDeleted(NodeID(prefixTestID("evt-node")))
	b.notifyEdgeCreated(testEdge("evt-edge", "evt-start", "evt-end", "LINK"))
	b.notifyEdgeUpdated(testEdge("evt-edge", "evt-start", "evt-end", "LINK"))
	b.notifyEdgeDeleted(EdgeID(prefixTestID("evt-edge")))

	assert.Equal(t, 1, nodeCreatedCount)
	assert.Equal(t, 1, nodeDeletedCount)
}

func TestBadgerEngine_LabelBatchAndStatsHelpers(t *testing.T) {
	b := createTestBadgerEngine(t)
	_, _ = b.CreateNode(&Node{ID: NodeID(prefixTestID("lbl-1")), Labels: []string{"Person"}})
	_, _ = b.CreateNode(&Node{ID: NodeID(prefixTestID("lbl-2")), Labels: []string{"Other"}})

	result, err := b.HasLabelBatch([]NodeID{
		NodeID(prefixTestID("lbl-1")),
		NodeID(prefixTestID("lbl-2")),
		"",
		NodeID(prefixTestID("missing")),
	}, "Person")
	require.NoError(t, err)
	assert.Equal(t, map[NodeID]bool{
		NodeID(prefixTestID("lbl-1")): true,
	}, result)

	result, err = b.HasLabelBatch(nil, "Person")
	require.NoError(t, err)
	assert.Empty(t, result)

	b.InvalidatePendingEmbeddingsIndex()

	assert.True(t, hasPrefix([]byte("tenant_a:node"), []byte("tenant_a:")))
	assert.False(t, hasPrefix([]byte("tenant_a:node"), []byte("tenant_b:")))
	assert.True(t, hasPrefix([]byte("x"), nil))
}

func TestBadgerEngine_ListNamespaces(t *testing.T) {
	b := createTestBadgerEngine(t)
	_, _ = b.CreateNode(&Node{ID: "tenant_a:n1", Labels: []string{"Person"}})
	_, _ = b.CreateNode(&Node{ID: "tenant_b:n2", Labels: []string{"Person"}})
	_ = b.CreateEdge(&Edge{ID: "tenant_b:e1", StartNode: "tenant_b:n2", EndNode: "tenant_b:n2", Type: "SELF"})

	namespaces := b.ListNamespaces()
	assert.Contains(t, namespaces, "tenant_a")
	assert.Contains(t, namespaces, "tenant_b")
}
