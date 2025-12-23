package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAsyncEngine_UpdateNodeEmbedding_DoesNotChangeNodeCount(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	async := NewAsyncEngine(base, DefaultAsyncEngineConfig())

	node := &Node{
		ID:         NodeID("nornic:node-1"),
		Labels:     []string{"Test"},
		Properties: map[string]any{"k": "v"},
	}
	_, err := base.CreateNode(node)
	require.NoError(t, err)

	count1, err := async.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count1)

	node.ChunkEmbeddings = [][]float32{{0.1, 0.2, 0.3}}
	node.Properties["has_embedding"] = true
	err = async.UpdateNodeEmbedding(node)
	require.NoError(t, err)

	count2, err := async.NodeCount()
	require.NoError(t, err)
	require.Equal(t, count1, count2, "embedding update must not affect NodeCount")
}

func TestAsyncEngine_UpdateNodeEmbedding_InFlight_DoesNotChangeNodeCount(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })

	async := NewAsyncEngine(base, DefaultAsyncEngineConfig())

	node := &Node{
		ID:         NodeID("nornic:node-1"),
		Labels:     []string{"Test"},
		Properties: map[string]any{"k": "v"},
	}
	_, err := base.CreateNode(node)
	require.NoError(t, err)

	// Simulate an in-flight state (e.g., node being flushed) and ensure embedding updates
	// are still treated as updates (not creates) for NodeCount purposes.
	async.mu.Lock()
	async.inFlightNodes[node.ID] = true
	async.mu.Unlock()

	count1, err := async.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count1)

	node.ChunkEmbeddings = [][]float32{{0.1, 0.2, 0.3}}
	node.Properties["has_embedding"] = true
	err = async.UpdateNodeEmbedding(node)
	require.NoError(t, err)

	count2, err := async.NodeCount()
	require.NoError(t, err)
	require.Equal(t, count1, count2, "embedding update must not affect NodeCount even when inFlight")
}

