package storage

import (
	"context"
	"errors"
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
		staleIDs := []NodeID{
			NodeID(prefixTestID("stale-1")),
			NodeID(prefixTestID("stale-2")),
			NodeID(prefixTestID("stale-3")),
		}
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

		// Add stale entries with no corresponding nodes.
		for i := 0; i < 101; i++ {
			staleID := NodeID(prefixTestID(fmt.Sprintf("stale-%d", i)))
			err := engine.db.Update(func(txn *badger.Txn) error {
				return txn.Set(pendingEmbedKey(staleID), []byte{})
			})
			require.NoError(t, err)
		}

		// FindNodeNeedingEmbedding should clean stale entries and return nil.
		found := engine.FindNodeNeedingEmbedding()
		assert.Nil(t, found, "should return nil when only stale entries exist")
		assert.Equal(t, 0, engine.PendingEmbeddingsCount(), "stale entries should be cleaned up")
	})

	t.Run("FindNodeNeedingEmbedding_handles_malformed_pending_keys_and_decode_failures", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)
		badID := NodeID(prefixTestID("decode-bad"))
		require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
			// malformed pending key with no node id (len<=1 path)
			if err := txn.Set([]byte{prefixPendingEmbed}, []byte{}); err != nil {
				return err
			}
			// valid pending key but corrupt node payload (decode error path)
			if err := txn.Set(pendingEmbedKey(badID), []byte{}); err != nil {
				return err
			}
			return txn.Set(nodeKey(badID), []byte("not-a-node"))
		}))

		found := engine.FindNodeNeedingEmbedding()
		assert.Nil(t, found)

		// Corrupt entry should be removed; malformed key remains ignored.
		require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(pendingEmbedKey(badID))
			assert.ErrorIs(t, err, badger.ErrKeyNotFound)
			_, err = txn.Get([]byte{prefixPendingEmbed})
			require.NoError(t, err)
			return nil
		}))
	})

	t.Run("FindNodeNeedingEmbedding_removes_nodes_that_no_longer_need_embedding", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)
		node := &Node{
			ID:              NodeID(prefixTestID("no-embed-needed")),
			Labels:          []string{"Test"},
			Properties:      map[string]interface{}{"name": "already-clean"},
			ChunkEmbeddings: [][]float32{{0.1, 0.2}},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		engine.AddToPendingEmbeddings(node.ID)
		assert.Equal(t, 1, engine.PendingEmbeddingsCount())
		found := engine.FindNodeNeedingEmbedding()
		assert.Nil(t, found)
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
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
		node.EmbedMeta = map[string]any{
			"embedding_model":      "test-model",
			"embedding_dimensions": 4,
			"has_embedding":        true,
		}

		// UpdateNodeEmbedding should succeed for existing node
		err = engine.UpdateNodeEmbedding(node)
		require.NoError(t, err, "UpdateNodeEmbedding should succeed for existing node")

		// Verify embedding was saved
		updated, err := engine.GetNode(node.ID)
		require.NoError(t, err)
		assert.Equal(t, [][]float32{{0.1, 0.2, 0.3, 0.4}}, updated.ChunkEmbeddings)
		assert.Equal(t, "test-model", updated.EmbedMeta["embedding_model"])

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
		node.EmbedMeta = map[string]any{
			"embedding_model":      "test-model",
			"embedding_dimensions": 3,
			"has_embedding":        true,
		}

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
		node.EmbedMeta = map[string]any{
			"embedding_model":      "test-model",
			"embedding_dimensions": 3,
			"has_embedding":        true,
		}

		err = engine.UpdateNodeEmbedding(node)
		require.NoError(t, err)

		// Should be removed from pending index
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
	})

	t.Run("RefreshPendingEmbeddingsIndex_removes_manual_system_namespace_entries", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		err := engine.db.Update(func(txn *badger.Txn) error {
			return txn.Set(pendingEmbedKey(NodeID("system:settings")), []byte{})
		})
		require.NoError(t, err)
		assert.Equal(t, 1, engine.PendingEmbeddingsCount())

		added := engine.RefreshPendingEmbeddingsIndex()
		assert.Equal(t, 0, added)
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
		assert.Nil(t, engine.FindNodeNeedingEmbedding())
	})

	t.Run("StreamNodeChunks covers default size stop and cancellation", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		for _, id := range []string{"chunk-a", "chunk-b", "chunk-c"} {
			_, err := engine.CreateNode(&Node{
				ID:              NodeID(prefixTestID(id)),
				Labels:          []string{"Chunked"},
				Properties:      map[string]interface{}{"name": id},
				ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
			})
			require.NoError(t, err)
		}

		chunksSeen := 0
		err := engine.StreamNodeChunks(context.Background(), 0, func(nodes []*Node) error {
			chunksSeen++
			require.NotEmpty(t, nodes)
			return ErrIterationStopped
		})
		require.NoError(t, err)
		assert.Equal(t, 1, chunksSeen)

		errBoom := errors.New("stop on visitor error")
		err = engine.StreamNodeChunks(context.Background(), 2, func(nodes []*Node) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = engine.StreamNodeChunks(ctx, 2, func(nodes []*Node) error { return nil })
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("ClearAllEmbeddingsForPrefix only clears matching namespace", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)

		_, err := engine.CreateNode(&Node{
			ID:              "tenant_a:doc-1",
			Labels:          []string{"Doc"},
			Properties:      map[string]interface{}{"content": "a"},
			ChunkEmbeddings: [][]float32{{1, 2, 3}},
		})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{
			ID:              "tenant_b:doc-1",
			Labels:          []string{"Doc"},
			Properties:      map[string]interface{}{"content": "b"},
			ChunkEmbeddings: [][]float32{{4, 5, 6}},
		})
		require.NoError(t, err)

		cleared, err := engine.ClearAllEmbeddingsForPrefix("tenant_a:")
		require.NoError(t, err)
		assert.Equal(t, 1, cleared)
		assert.Equal(t, 1, engine.PendingEmbeddingsCount())

		tenantA, err := engine.GetNode("tenant_a:doc-1")
		require.NoError(t, err)
		assert.Nil(t, tenantA.ChunkEmbeddings)

		tenantB, err := engine.GetNode("tenant_b:doc-1")
		require.NoError(t, err)
		require.Len(t, tenantB.ChunkEmbeddings, 1)
		require.Len(t, tenantB.ChunkEmbeddings[0], 3)
	})

	t.Run("closed engine helpers return safe results", func(t *testing.T) {
		engine := newTestBadgerEngineForPending(t)
		require.NoError(t, engine.Close())

		require.Error(t, engine.Sync())
		require.Error(t, engine.RunGC())
		assert.Equal(t, 0, engine.PendingEmbeddingsCount())
		assert.Equal(t, 0, engine.RefreshPendingEmbeddingsIndex())
		assert.Nil(t, engine.FindNodeNeedingEmbedding())

		lsm, vlog := engine.Size()
		assert.Equal(t, int64(0), lsm)
		assert.Equal(t, int64(0), vlog)

		_, err := engine.ClearAllEmbeddingsForPrefix("tenant_a:")
		require.Error(t, err)

		err = engine.StreamNodeChunks(context.Background(), 2, func(nodes []*Node) error { return nil })
		require.Error(t, err)

		engine.InvalidatePendingEmbeddingsIndex()
	})
}

// newTestBadgerEngineForPending creates a BadgerEngine for pending embeddings tests
func newTestBadgerEngineForPending(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { engine.Close() })
	// These tests are specifically exercising the pending-embed index. The
	// engine defaults the flag to false so servers with embeddings disabled
	// pay zero write amplification; flip it on for the suite.
	engine.SetEmbeddingsEnabled(true)
	return engine
}

func TestBadgerEngine_InvalidatePendingEmbeddingsIndex(t *testing.T) {
	engine := newTestBadgerEngineForPending(t)

	// InvalidatePendingEmbeddingsIndex is documented as a no-op for Badger.
	// Verify it doesn't panic and the index remains functional.
	node := &Node{
		ID:         NodeID(prefixTestID("inv-n1")),
		Labels:     []string{"TestNode"},
		Properties: map[string]interface{}{"name": "test"},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	// Call the no-op — should not panic or error
	engine.InvalidatePendingEmbeddingsIndex()

	// Index should still be functional after "invalidation"
	count := engine.RefreshPendingEmbeddingsIndex()
	// count may be 0 or positive depending on whether the node needs embeddings;
	// the point is it doesn't error
	_ = count
}
