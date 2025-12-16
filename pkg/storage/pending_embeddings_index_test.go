package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_PendingEmbeddingsIndex(t *testing.T) {
	t.Run("new node without embedding is added to pending index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node without embedding
		node := &Node{
			ID:         "pending-1",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Alice"},
			Embedding:  nil,
		}
		err := engine.CreateNode(node)
		require.NoError(t, err)

		// Check pending count
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "node without embedding should be in pending index")

		// FindNodeNeedingEmbedding should return this node
		found := engine.FindNodeNeedingEmbedding()
		require.NotNil(t, found)
		assert.Equal(t, "pending-1", string(found.ID))
	})

	t.Run("node with embedding is NOT added to pending index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node WITH embedding
		node := &Node{
			ID:         "embedded-1",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Bob"},
			Embedding:  []float32{0.1, 0.2, 0.3},
		}
		err := engine.CreateNode(node)
		require.NoError(t, err)

		// Check pending count - should be 0
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 0, count, "node with embedding should NOT be in pending index")

		// FindNodeNeedingEmbedding should return nil
		found := engine.FindNodeNeedingEmbedding()
		assert.Nil(t, found)
	})

	t.Run("MarkNodeEmbedded removes node from pending index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node without embedding
		node := &Node{
			ID:         "mark-1",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "Charlie"},
			Embedding:  nil,
		}
		err := engine.CreateNode(node)
		require.NoError(t, err)

		// Verify it's in pending
		assert.Equal(t, 1, engine.PendingEmbeddingsCount())

		// Mark as embedded
		engine.MarkNodeEmbedded("mark-1")

		// Should no longer be in pending
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
	})

	t.Run("RefreshPendingEmbeddingsIndex adds missing nodes", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create nodes without embedding
		for i := 0; i < 5; i++ {
			node := &Node{
				ID:         NodeID("refresh-" + string(rune('a'+i))),
				Labels:     []string{"Item"},
				Properties: map[string]interface{}{"index": i},
				Embedding:  nil,
			}
			err := engine.CreateNode(node)
			require.NoError(t, err)
		}

		// All should be in pending
		assert.Equal(t, 5, engine.PendingEmbeddingsCount())

		// Manually clear the pending index (simulating corruption or bug)
		for i := 0; i < 5; i++ {
			engine.MarkNodeEmbedded(NodeID("refresh-" + string(rune('a'+i))))
		}
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())

		// Refresh should re-add them
		added := engine.RefreshPendingEmbeddingsIndex()
		assert.Equal(t, 5, added)
		assert.Equal(t, 5, engine.PendingEmbeddingsCount())
	})

	t.Run("ClearAllEmbeddings refreshes pending index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create nodes WITH embeddings
		for i := 0; i < 3; i++ {
			node := &Node{
				ID:         NodeID("clear-" + string(rune('a'+i))),
				Labels:     []string{"Memory"},
				Properties: map[string]interface{}{"content": "test content"},
				Embedding:  []float32{0.1, 0.2, 0.3},
			}
			err := engine.CreateNode(node)
			require.NoError(t, err)
		}

		// No nodes should be pending (they all have embeddings)
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())

		// Clear all embeddings
		cleared, err := engine.ClearAllEmbeddings()
		require.NoError(t, err)
		assert.Equal(t, 3, cleared)

		// Now all nodes should be in pending index
		assert.Equal(t, 3, engine.PendingEmbeddingsCount())

		// FindNodeNeedingEmbedding should find them
		found := engine.FindNodeNeedingEmbedding()
		require.NotNil(t, found)
	})

	t.Run("REGRESSION: node with has_embedding=true but no array is found", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node that has has_embedding=true but no actual embedding
		// This was the bug - such nodes were being skipped
		node := &Node{
			ID:     "regression-1",
			Labels: []string{"File"},
			Properties: map[string]interface{}{
				"name":          "test.txt",
				"has_embedding": true, // Property says true
				"has_chunks":    true,
			},
			Embedding: nil, // But no actual embedding
		}
		err := engine.CreateNode(node)
		require.NoError(t, err)

		// Should be in pending index because Embedding array is nil
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "node with has_embedding=true but no embedding array should be pending")

		// FindNodeNeedingEmbedding should return this node
		found := engine.FindNodeNeedingEmbedding()
		require.NotNil(t, found, "should find node that needs embedding")
		assert.Equal(t, "regression-1", string(found.ID))
	})

	t.Run("internal nodes are not added to pending index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create an internal node (label starts with _)
		node := &Node{
			ID:         "internal-1",
			Labels:     []string{"_SystemNode"},
			Properties: map[string]interface{}{"data": "internal"},
			Embedding:  nil,
		}
		err := engine.CreateNode(node)
		require.NoError(t, err)

		// Should NOT be in pending index
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 0, count, "internal nodes should not be in pending index")
	})

	t.Run("multiple nodes are processed in order", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create multiple nodes
		nodeIDs := []string{"multi-a", "multi-b", "multi-c"}
		for _, id := range nodeIDs {
			node := &Node{
				ID:         NodeID(id),
				Labels:     []string{"Item"},
				Properties: map[string]interface{}{"id": id},
				Embedding:  nil,
			}
			err := engine.CreateNode(node)
			require.NoError(t, err)
		}

		assert.Equal(t, 3, engine.PendingEmbeddingsCount())

		// Process each one
		processed := make(map[string]bool)
		for i := 0; i < 3; i++ {
			found := engine.FindNodeNeedingEmbedding()
			require.NotNil(t, found)
			processed[string(found.ID)] = true

			// Mark as embedded
			engine.MarkNodeEmbedded(found.ID)
		}

		// All should have been processed
		for _, id := range nodeIDs {
			assert.True(t, processed[id], "node %s should have been processed", id)
		}

		// No more pending
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
		assert.Nil(t, engine.FindNodeNeedingEmbedding())
	})
}

// newTestBadgerEngineForPending creates a BadgerEngine for pending embeddings tests
func newTestBadgerEngineForPending(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { engine.Close() })
	return engine
}
