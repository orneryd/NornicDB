package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_PendingEmbeddingsIndex(t *testing.T) {
	t.Run("new node without embedding is added to pending index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node without embedding
		node := &Node{
			ID:              NodeID(prefixTestID("pending-1")),
			Labels:          []string{"Person"},
			Properties:      map[string]interface{}{"name": "Alice"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Check pending count
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "node without embedding should be in pending index")

		// FindNodeNeedingEmbedding should return this node
		found := engine.FindNodeNeedingEmbedding()
		require.NotNil(t, found)
		assert.Equal(t, prefixTestID("pending-1"), string(found.ID))
	})

	t.Run("node with embedding is NOT added to pending index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node WITH embedding
		node := &Node{
			ID:              NodeID(prefixTestID("embedded-1")),
			Labels:          []string{"Person"},
			Properties:      map[string]interface{}{"name": "Bob"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		}
		_, err := engine.CreateNode(node)
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
			ID:              NodeID(prefixTestID("mark-1")),
			Labels:          []string{"Person"},
			Properties:      map[string]interface{}{"name": "Charlie"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Verify it's in pending
		assert.Equal(t, 1, engine.PendingEmbeddingsCount())

		// Mark as embedded
		engine.MarkNodeEmbedded(NodeID(prefixTestID("mark-1")))

		// Should no longer be in pending
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
	})

	t.Run("RefreshPendingEmbeddingsIndex adds missing nodes", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create nodes without embedding
		for i := 0; i < 5; i++ {
			node := &Node{
				ID:              NodeID(prefixTestID("refresh-" + string(rune('a'+i)))),
				Labels:          []string{"Item"},
				Properties:      map[string]interface{}{"index": i},
				ChunkEmbeddings: nil,
			}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
		}

		// All should be in pending
		assert.Equal(t, 5, engine.PendingEmbeddingsCount())

		// Manually clear the pending index (simulating corruption or bug)
		for i := 0; i < 5; i++ {
			engine.MarkNodeEmbedded(NodeID(prefixTestID("refresh-" + string(rune('a'+i)))))
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
				ID:              NodeID(prefixTestID("clear-" + string(rune('a'+i)))),
				Labels:          []string{"Memory"},
				Properties:      map[string]interface{}{"content": "test content"},
				ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
			}
			_, err := engine.CreateNode(node)
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
			ID:     NodeID(prefixTestID("regression-1")),
			Labels: []string{"File"},
			Properties: map[string]interface{}{
				"name":          "test.txt",
				"has_embedding": true, // Property says true
				"has_chunks":    true,
			},
			ChunkEmbeddings: nil, // But no actual embedding
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Should be in pending index because Embedding array is nil
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "node with has_embedding=true but no embedding array should be pending")

		// FindNodeNeedingEmbedding should return this node
		found := engine.FindNodeNeedingEmbedding()
		require.NotNil(t, found, "should find node that needs embedding")
		assert.Equal(t, prefixTestID("regression-1"), string(found.ID))
	})

	t.Run("RefreshPendingEmbeddingsIndex_removes_stale_entries", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node without embedding
		node := &Node{
			ID:              NodeID(prefixTestID("stale-test")),
			Labels:          []string{"Test"},
			Properties:      map[string]interface{}{"content": "Test content"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Verify it's in the pending index
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "node should be in pending index")

		// Manually add a stale entry to the index (simulating a deleted node)
		// This simulates the bug where deleted nodes remain in the index
		err = engine.db.Update(func(txn *badger.Txn) error {
			return txn.Set(pendingEmbedKey(NodeID(prefixTestID("deleted-node"))), []byte{})
		})
		require.NoError(t, err)

		// Verify stale entry is in index
		count = engine.PendingEmbeddingsCount()
		assert.Equal(t, 2, count, "should have 2 entries (1 real + 1 stale)")

		// Refresh the index - should remove stale entry
		added := engine.RefreshPendingEmbeddingsIndex()
		assert.Equal(t, 0, added, "should not add any new nodes")

		// Verify stale entry was removed
		count = engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "stale entry should be removed, only real node remains")
	})

	t.Run("RefreshPendingEmbeddingsIndex_removes_entries_for_nodes_with_embeddings", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node without embedding
		node := &Node{
			ID:              NodeID(prefixTestID("needs-embed")),
			Labels:          []string{"Test"},
			Properties:      map[string]interface{}{"content": "Test content"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Verify it's in the pending index
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "node should be in pending index")

		// Add embedding to the node
		node.ChunkEmbeddings = [][]float32{{0.1, 0.2, 0.3}}
		err = engine.UpdateNode(node)
		require.NoError(t, err)

		// Manually add it back to pending index (simulating stale state)
		err = engine.db.Update(func(txn *badger.Txn) error {
			return txn.Set(pendingEmbedKey(node.ID), []byte{})
		})
		require.NoError(t, err)

		// Verify it's still in index (stale)
		count = engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "stale entry should still be in index")

		// Refresh the index - should remove entry since node has embedding
		added := engine.RefreshPendingEmbeddingsIndex()
		assert.Equal(t, 0, added, "should not add any new nodes")

		// Verify stale entry was removed
		count = engine.PendingEmbeddingsCount()
		assert.Equal(t, 0, count, "entry should be removed since node has embedding")
	})

	t.Run("internal nodes are not added to pending index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create an internal node (label starts with _)
		node := &Node{
			ID:              NodeID(prefixTestID("internal-1")),
			Labels:          []string{"_SystemNode"},
			Properties:      map[string]interface{}{"data": "internal"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
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
				ID:              NodeID(prefixTestID(id)),
				Labels:          []string{"Item"},
				Properties:      map[string]interface{}{"id": id},
				ChunkEmbeddings: nil,
			}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
		}

		assert.Equal(t, 3, engine.PendingEmbeddingsCount())

		// Process each one
		processed := make(map[string]bool)
		for i := 0; i < 3; i++ {
			found := engine.FindNodeNeedingEmbedding()
			require.NotNil(t, found)
			// found.ID is prefixed (e.g., "test:multi-a"), unprefix it for comparison
			unprefixedID := string(found.ID)
			if strings.HasPrefix(unprefixedID, "test:") {
				unprefixedID = unprefixedID[5:] // Remove "test:" prefix
			}
			processed[unprefixedID] = true

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

	t.Run("FindNodeNeedingEmbedding_skips_stale_entries", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a valid node without embedding
		node := &Node{
			ID:              NodeID(prefixTestID("valid-node")),
			Labels:          []string{"Test"},
			Properties:      map[string]interface{}{"content": "Test content"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Manually add stale entries to the pending index (simulating deleted nodes)
		staleIDs := []NodeID{"stale-1", "stale-2", "stale-3"}
		for _, staleID := range staleIDs {
			err = engine.db.Update(func(txn *badger.Txn) error {
				return txn.Set(pendingEmbedKey(staleID), []byte{})
			})
			require.NoError(t, err)
		}

		// Verify we have 4 entries (1 valid + 3 stale)
		count := engine.PendingEmbeddingsCount()
		assert.Equal(t, 4, count, "should have 4 entries in pending index")

		// FindNodeNeedingEmbedding should skip stale entries and find the valid one
		// It should handle up to 100 stale entries before giving up
		found := engine.FindNodeNeedingEmbedding()
		require.NotNil(t, found, "should find valid node despite stale entries")
		assert.Equal(t, prefixTestID("valid-node"), string(found.ID), "should find the valid node")

		// Verify stale entries were removed from index
		count = engine.PendingEmbeddingsCount()
		assert.Equal(t, 1, count, "stale entries should be removed, only valid node remains")
	})

	t.Run("FindNodeNeedingEmbedding_handles_many_stale_entries", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a valid node
		node := &Node{
			ID:              NodeID(prefixTestID("valid-node")),
			Labels:          []string{"Test"},
			Properties:      map[string]interface{}{"content": "Test content"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Add 50 stale entries (less than the 100 max attempts)
		for i := 0; i < 50; i++ {
			staleID := NodeID(prefixTestID(fmt.Sprintf("stale-%d", i)))
			err = engine.db.Update(func(txn *badger.Txn) error {
				return txn.Set(pendingEmbedKey(staleID), []byte{})
			})
			require.NoError(t, err)
		}

		// FindNodeNeedingEmbedding should skip all stale entries and find the valid one
		found := engine.FindNodeNeedingEmbedding()
		require.NotNil(t, found, "should find valid node after skipping 50 stale entries")
		assert.Equal(t, prefixTestID("valid-node"), string(found.ID))
	})

	t.Run("FindNodeNeedingEmbedding_returns_nil_after_max_attempts", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Add 101 stale entries (more than the 100 max attempts)
		for i := 0; i < 101; i++ {
			staleID := NodeID(prefixTestID(fmt.Sprintf("stale-%d", i)))
			err := engine.db.Update(func(txn *badger.Txn) error {
				return txn.Set(pendingEmbedKey(staleID), []byte{})
			})
			require.NoError(t, err)
		}

		// FindNodeNeedingEmbedding should give up after 100 attempts
		found := engine.FindNodeNeedingEmbedding()
		assert.Nil(t, found, "should return nil after max attempts to prevent infinite loop")
	})

	t.Run("UpdateNodeEmbedding_only_updates_existing_nodes", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node without embedding
		node := &Node{
			ID:              NodeID(prefixTestID("test-node")),
			Labels:          []string{"Test"},
			Properties:      map[string]interface{}{"content": "Test content"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Add embedding to the node
		node.ChunkEmbeddings = [][]float32{{0.1, 0.2, 0.3, 0.4}}
		node.Properties["embedding_model"] = "test-model"
		node.Properties["embedding_dimensions"] = 4
		node.Properties["has_embedding"] = true

		// UpdateNodeEmbedding should succeed for existing node
		err = engine.UpdateNodeEmbedding(node)
		require.NoError(t, err, "UpdateNodeEmbedding should succeed for existing node")

		// Verify embedding was saved
		updated, err := engine.GetNode(node.ID)
		require.NoError(t, err)
		assert.Equal(t, [][]float32{{0.1, 0.2, 0.3, 0.4}}, updated.ChunkEmbeddings)
		assert.Equal(t, "test-model", updated.Properties["embedding_model"])

		// Try to update a non-existent node - should return ErrNotFound
		nonExistent := &Node{
			ID:              NodeID(prefixTestID("non-existent")),
			ChunkEmbeddings: [][]float32{{0.5, 0.6}},
		}
		err = engine.UpdateNodeEmbedding(nonExistent)
		assert.Equal(t, ErrNotFound, err, "UpdateNodeEmbedding should return ErrNotFound for non-existent node")

		// Verify node was NOT created
		_, err = engine.GetNode(NodeID(prefixTestID("non-existent")))
		assert.Equal(t, ErrNotFound, err, "node should not have been created")
	})

	t.Run("UpdateNodeEmbedding_preserves_other_properties", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node with properties
		node := &Node{
			ID:     NodeID(prefixTestID("test-node")),
			Labels: []string{"Test"},
			Properties: map[string]interface{}{
				"content": "Original content",
				"title":   "Original title",
			},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Update only embedding-related fields
		node.ChunkEmbeddings = [][]float32{{0.1, 0.2, 0.3}}
		node.Properties["embedding_model"] = "test-model"
		node.Properties["embedding_dimensions"] = 3
		node.Properties["has_embedding"] = true

		err = engine.UpdateNodeEmbedding(node)
		require.NoError(t, err)

		// Verify embedding was updated but other properties preserved
		updated, err := engine.GetNode(node.ID)
		require.NoError(t, err)
		assert.Equal(t, [][]float32{{0.1, 0.2, 0.3}}, updated.ChunkEmbeddings)
		assert.Equal(t, "Original content", updated.Properties["content"], "non-embedding properties should be preserved")
		assert.Equal(t, "Original title", updated.Properties["title"], "non-embedding properties should be preserved")
	})

	t.Run("UpdateNodeEmbedding_removes_from_pending_index", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		// Create a node without embedding (should be in pending index)
		node := &Node{
			ID:              NodeID(prefixTestID("test-node")),
			Labels:          []string{"Test"},
			Properties:      map[string]interface{}{"content": "Test content"},
			ChunkEmbeddings: nil,
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		// Verify it's in pending index
		assert.Equal(t, 1, engine.PendingEmbeddingsCount())

		// Add embedding and update
		node.ChunkEmbeddings = [][]float32{{0.1, 0.2, 0.3}}
		node.Properties["embedding_model"] = "test-model"
		node.Properties["embedding_dimensions"] = 3
		node.Properties["has_embedding"] = true

		err = engine.UpdateNodeEmbedding(node)
		require.NoError(t, err)

		// Should be removed from pending index
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
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
