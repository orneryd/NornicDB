// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// Stats and Lifecycle
// ============================================================================

// NodeCount returns the total number of valid, decodable nodes.
// This is consistent with AllNodes() - only counts nodes that can be successfully decoded.
// Corrupted or incompatible node entries are not counted.
// initializeCounts scans the database once to initialize the cached node and edge counts.
// This is called only on engine startup to enable O(1) stats lookups.
func (b *BadgerEngine) initializeCounts() error {
	var nodeCount, edgeCount int64
	namespaceNodeCounts := make(map[string]int64)
	namespaceEdgeCounts := make(map[string]int64)

	err := b.db.View(func(txn *badger.Txn) error {
		// Count nodes
		nodeOpts := badger.DefaultIteratorOptions
		nodeOpts.PrefetchValues = false // Only need keys for counting
		nodeIt := txn.NewIterator(nodeOpts)
		defer nodeIt.Close()

		nodePrefix := []byte{prefixNode}
		for nodeIt.Seek(nodePrefix); nodeIt.ValidForPrefix(nodePrefix); nodeIt.Next() {
			nodeCount++
			key := nodeIt.Item().Key()
			if len(key) <= 1 {
				continue
			}
			if ns, ok := namespacePrefixFromID(string(key[1:])); ok {
				namespaceNodeCounts[ns]++
			}
		}

		// Count edges
		edgeOpts := badger.DefaultIteratorOptions
		edgeOpts.PrefetchValues = false // Only need keys for counting
		edgeIt := txn.NewIterator(edgeOpts)
		defer edgeIt.Close()

		edgePrefix := []byte{prefixEdge}
		for edgeIt.Seek(edgePrefix); edgeIt.ValidForPrefix(edgePrefix); edgeIt.Next() {
			edgeCount++
			key := edgeIt.Item().Key()
			if len(key) <= 1 {
				continue
			}
			if ns, ok := namespacePrefixFromID(string(key[1:])); ok {
				namespaceEdgeCounts[ns]++
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Initialize atomic counters
	b.nodeCount.Store(nodeCount)
	b.edgeCount.Store(edgeCount)
	b.namespaceCountsMu.Lock()
	b.namespaceNodeCounts = namespaceNodeCounts
	b.namespaceEdgeCounts = namespaceEdgeCounts
	b.namespaceCountsMu.Unlock()

	return nil
}

func (b *BadgerEngine) NodeCount() (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}

	return b.nodeCount.Load(), nil
}

// NodeCountByPrefix counts nodes whose NodeID begins with the provided prefix.
// The prefix refers to the NodeID string prefix (e.g., database namespace "nornic:").
//
// This is an optional fast-path used by NamespacedEngine to provide accurate
// per-database counts without decoding values.
func (b *BadgerEngine) NodeCountByPrefix(prefix string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}

	b.namespaceCountsMu.RLock()
	if count, ok := b.namespaceNodeCounts[prefix]; ok {
		b.namespaceCountsMu.RUnlock()
		return count, nil
	}
	b.namespaceCountsMu.RUnlock()

	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		keyPrefix := make([]byte, 0, 1+len(prefix))
		keyPrefix = append(keyPrefix, prefixNode)
		keyPrefix = append(keyPrefix, []byte(prefix)...)

		it := txn.NewIterator(badgerIterOptsKeyOnly(keyPrefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// EdgeCount returns the total number of valid, decodable edges.
// This is consistent with AllEdges() - only counts edges that can be successfully decoded.
func (b *BadgerEngine) EdgeCount() (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}

	return b.edgeCount.Load(), nil
}

// EdgeCountByPrefix counts edges whose EdgeID begins with the provided prefix.
// The prefix refers to the EdgeID string prefix (e.g., database namespace "nornic:").
func (b *BadgerEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}

	b.namespaceCountsMu.RLock()
	if count, ok := b.namespaceEdgeCounts[prefix]; ok {
		b.namespaceCountsMu.RUnlock()
		return count, nil
	}
	b.namespaceCountsMu.RUnlock()

	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		keyPrefix := make([]byte, 0, 1+len(prefix))
		keyPrefix = append(keyPrefix, prefixEdge)
		keyPrefix = append(keyPrefix, []byte(prefix)...)

		it := txn.NewIterator(badgerIterOptsKeyOnly(keyPrefix))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// GetSchema returns the schema manager.
func (b *BadgerEngine) GetSchema() *SchemaManager {
	// Backwards-compat: callers without an explicit namespace get the default DB schema.
	// Most production callers should use NamespacedEngine which routes correctly.
	return b.GetSchemaForNamespace("nornic")
}

// Close closes the BadgerDB database.
func (b *BadgerEngine) Close() error {
	b.stopEdgeBetweenIndexBackfill()

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	if b.lifecycleController != nil {
		b.lifecycleController.StopLifecycle()
	}

	b.closed = true
	return b.db.Close()
}

// Sync forces a sync of all data to disk.
// This is useful for ensuring durability before a crash.
func (b *BadgerEngine) Sync() error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.db.Sync()
}

// RunGC runs garbage collection on the BadgerDB value log.
// Should be called periodically for long-running applications.
func (b *BadgerEngine) RunGC() error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.db.RunValueLogGC(0.5)
}

// Size returns the approximate size of the database in bytes.
func (b *BadgerEngine) Size() (lsm, vlog int64) {
	if b.ensureOpen() != nil {
		return 0, 0
	}

	return b.db.Size()
}

// FindNodeNeedingEmbedding returns a node that needs embedding.
// Uses Badger's secondary index (prefixPendingEmbed) for O(1) lookup.
//
// This is highly optimized:
// - O(1) to find next node (just seek to prefix and get first key)
// - No in-memory index needed
// - Persistent across restarts
// - Atomic with node operations
//
// CRITICAL: This method aggressively cleans up stale entries to prevent
// processing non-existent nodes. It will skip up to 100 stale entries
// before giving up to prevent infinite loops.
func (b *BadgerEngine) FindNodeNeedingEmbedding() *Node {
	if b.ensureOpen() != nil {
		return nil
	}

	var found *Node
	removedStale := 0
	removedNoLongerNeeds := 0

	_ = b.withUpdate(func(txn *badger.Txn) error {
		prefix := []byte{prefixPendingEmbed}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 10))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])

			// Never embed the system database; remove any stale/system entries.
			if isSystemNamespaceID(string(nodeID)) {
				_ = txn.Delete(pendingEmbedKey(nodeID))
				removedNoLongerNeeds++
				continue
			}

			// Verify node exists in the same transaction for consistency.
			item, err := txn.Get(nodeKey(nodeID))
			if err == badger.ErrKeyNotFound {
				_ = txn.Delete(pendingEmbedKey(nodeID))
				removedStale++
				continue
			}
			if err != nil {
				// Conservatively skip on unexpected errors.
				continue
			}

			var node *Node
			if err := item.Value(func(val []byte) error {
				var decErr error
				node, decErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decErr
			}); err != nil || node == nil {
				_ = txn.Delete(pendingEmbedKey(nodeID))
				removedStale++
				continue
			}

			// If node no longer needs embedding, remove it from the pending index.
			if (len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0) || !NodeNeedsEmbedding(node) {
				_ = txn.Delete(pendingEmbedKey(nodeID))
				removedNoLongerNeeds++
				continue
			}

			found = node
			break
		}
		return nil
	})

	// Log at most once per call (avoid per-entry spam on large stale indexes).
	if removedStale > 0 || removedNoLongerNeeds > 0 {
		b.log.Info("pending embeddings cleanup",
			"subsystem", "embeddings_index",
			"removed_stale", removedStale,
			"removed_no_longer_needed", removedNoLongerNeeds,
		)
	}

	return found
}

// MarkNodeEmbedded removes a node from the pending embeddings index.
// Call this after successfully embedding a node.
func (b *BadgerEngine) MarkNodeEmbedded(nodeID NodeID) {
	_ = b.withUpdate(func(txn *badger.Txn) error {
		return txn.Delete(pendingEmbedKey(nodeID))
	})
}

// AddToPendingEmbeddings adds a node to the pending embeddings index.
// Call this when creating a node that needs embedding.
func (b *BadgerEngine) AddToPendingEmbeddings(nodeID NodeID) {
	_ = b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(pendingEmbedKey(nodeID), []byte{})
	})
}

// PendingEmbeddingsCount returns the number of nodes waiting for embedding.
// Note: This requires a scan of the pending index, so use sparingly.
func (b *BadgerEngine) PendingEmbeddingsCount() int {
	count := 0
	_ = b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixPendingEmbed}
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count
}

// InvalidatePendingEmbeddingsIndex is a no-op for Badger index.
// The index is persistent and doesn't need invalidation.
func (b *BadgerEngine) InvalidatePendingEmbeddingsIndex() {
	// No-op - Badger index is persistent and self-maintaining
}

// RefreshPendingEmbeddingsIndex rebuilds the pending embeddings index.
// This scans all nodes and adds any missing ones to the index.
// It also removes stale entries for nodes that no longer exist or already have embeddings.
// Use this on startup or after bulk imports.
func (b *BadgerEngine) RefreshPendingEmbeddingsIndex() int {
	added := 0
	removed := 0

	// First pass: Clean up stale entries in the pending index
	// Remove entries for nodes that don't exist or already have embeddings
	_ = b.withUpdate(func(txn *badger.Txn) error {
		pendingPrefix := []byte{prefixPendingEmbed}
		it := txn.NewIterator(badgerIterOptsKeyOnly(pendingPrefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			// Extract nodeID from key (skip prefix byte)
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])

			// Never embed the system database; remove from pending index if present.
			if isSystemNamespaceID(string(nodeID)) {
				txn.Delete(key)
				removed++
				continue
			}

			// Check if node exists and still needs embedding
			// ALL node IDs must be prefixed - no unprefixed IDs allowed
			keyToCheck := nodeKey(nodeID)
			item, err := txn.Get(keyToCheck)

			if err == badger.ErrKeyNotFound {
				// Node doesn't exist - remove from pending index
				// This is a stale entry - log it for debugging.
				// Cap the per-call log volume so a corrupted/oversized
				// pending index does not flood the log pipeline.
				if removed < 10 {
					b.log.Debug("refresh pending embeddings: removing stale entry",
						"subsystem", "embeddings_index",
						"reason", "node_not_found",
						"node_id", string(nodeID),
					)
				}
				txn.Delete(key)
				removed++
				continue
			}
			if err != nil {
				// Error reading - mark as stale.
				if removed < 10 {
					b.log.Debug("refresh pending embeddings: removing corrupted entry",
						"subsystem", "embeddings_index",
						"reason", "read_error",
						"node_id", string(nodeID),
						slog.Any("error", err),
					)
				}
				txn.Delete(key)
				removed++
				continue
			}

			// Check if node already has embedding
			// nodeID is already extracted above from the pending embed key
			// item already contains the node data from above
			var node *Node
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decodeErr
			}); err != nil {
				// Can't decode - remove stale entry
				txn.Delete(key)
				removed++
				continue
			}

			// Remove from index if node already has embedding or doesn't need one
			if (len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0) || !NodeNeedsEmbedding(node) {
				txn.Delete(key)
				removed++
			}
		}
		return nil
	})

	// Second pass: Add missing nodes to the index
	_ = b.withUpdate(func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 100))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			// Extract nodeID from key (skip prefix byte)
			key := item.Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])
			if isSystemNamespaceID(string(nodeID)) {
				continue
			}

			item.Value(func(val []byte) error {
				node, err := b.decodeNodeWithEmbeddings(txn, val, nodeID)
				if err != nil {
					return nil
				}

				// Skip internal nodes
				for _, label := range node.Labels {
					if len(label) > 0 && label[0] == '_' {
						return nil
					}
				}

				// Check if needs embedding and not already in index
				if (len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0) && NodeNeedsEmbedding(node) {
					// Check if already in pending index
					_, err := txn.Get(pendingEmbedKey(node.ID))
					if err == badger.ErrKeyNotFound {
						txn.Set(pendingEmbedKey(node.ID), []byte{})
						added++
					}
				}
				return nil
			})
		}
		return nil
	})

	// Always log if there were changes, or if we're cleaning up stale entries.
	if added > 0 || removed > 0 {
		b.log.Info("pending embeddings index refreshed",
			"subsystem", "embeddings_index",
			"added", added,
			"removed_stale", removed,
		)
	}
	// No changes case: previously a commented-out verbose-mode print; left
	// silent so steady-state refreshes do not contribute log volume.
	return added
}

// IterateNodes iterates through all nodes one at a time without loading all into memory.
// The callback returns true to continue, false to stop.
func (b *BadgerEngine) IterateNodes(fn func(*Node) bool) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	return b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 10))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			// Extract nodeID from key (skip prefix byte)
			key := item.Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])

			var node *Node
			err := item.Value(func(val []byte) error {
				var decErr error
				node, decErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decErr
			})
			if err != nil {
				continue // Skip invalid nodes
			}
			if !fn(node) {
				break // Callback requested stop
			}
		}
		return nil
	})
}

// StreamNodes implements StreamingEngine.StreamNodes for memory-efficient iteration.
// Iterates through all nodes one at a time without loading all into memory.
func (b *BadgerEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	return b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 10))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			var node *Node
			err := item.Value(func(val []byte) error {
				key := item.Key()
				if len(key) <= 1 {
					return nil
				}
				nodeID := NodeID(key[1:])
				var decErr error
				node, decErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decErr
			})
			if err != nil {
				continue // Skip invalid nodes
			}
			if err := fn(node); err != nil {
				if err == ErrIterationStopped {
					return nil // Normal stop
				}
				return err
			}
		}
		return nil
	})
}

// StreamNodesByPrefix streams nodes whose IDs start with prefix.
// This is significantly faster than full StreamNodes + callback filtering when
// tenants/databases share a physical store.
func (b *BadgerEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	return b.withView(func(txn *badger.Txn) error {
		seekPrefix := append([]byte{prefixNode}, []byte(prefix)...)
		it := txn.NewIterator(badgerIterOptsPrefetchValues(seekPrefix, 10))
		defer it.Close()

		for it.Seek(seekPrefix); it.ValidForPrefix(seekPrefix); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			var node *Node
			err := item.Value(func(val []byte) error {
				key := item.Key()
				if len(key) <= 1 {
					return nil
				}
				nodeID := NodeID(key[1:])
				var decErr error
				node, decErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decErr
			})
			if err != nil {
				continue
			}
			if err := fn(node); err != nil {
				if err == ErrIterationStopped {
					return nil
				}
				return err
			}
		}
		return nil
	})
}

// StreamEdges implements StreamingEngine.StreamEdges for memory-efficient iteration.
// Iterates through all edges one at a time without loading all into memory.
func (b *BadgerEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	return b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixEdge}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, 10))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			key := item.Key()
			var edgeID EdgeID
			if len(key) > 1 {
				edgeID = EdgeID(key[1:])
			}
			var edge *Edge
			err := item.Value(func(val []byte) error {
				var decErr error
				edge, decErr = b.decodeEdgeBodyByID(val, edgeID)
				return decErr
			})
			if err != nil {
				continue // Skip invalid edges
			}
			if err := fn(edge); err != nil {
				if err == ErrIterationStopped {
					return nil // Normal stop
				}
				return err
			}
		}
		return nil
	})
}

// StreamNodeChunks implements StreamingEngine.StreamNodeChunks for batch processing.
// Iterates through nodes in chunks, more efficient for batch operations.
func (b *BadgerEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1000
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	return b.withView(func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(prefix, min(chunkSize, 100)))
		defer it.Close()

		chunk := make([]*Node, 0, chunkSize)

		for it.Rewind(); it.Valid(); it.Next() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			var node *Node
			err := item.Value(func(val []byte) error {
				key := item.Key()
				if len(key) <= 1 {
					return nil
				}
				nodeID := NodeID(key[1:])
				var decErr error
				node, decErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decErr
			})
			if err != nil {
				continue // Skip invalid nodes
			}

			chunk = append(chunk, node)

			if len(chunk) >= chunkSize {
				if err := fn(chunk); err != nil {
					if err == ErrIterationStopped {
						return nil
					}
					return err
				}
				// Reset chunk, reuse capacity
				chunk = chunk[:0]
			}
		}

		// Process remaining nodes
		if len(chunk) > 0 {
			if err := fn(chunk); err != nil {
				if err == ErrIterationStopped {
					return nil
				}
				return err
			}
		}

		return nil
	})
}

// ============================================================================
// Utility functions for compatibility
// ============================================================================

// HasPrefix checks if a byte slice has the given prefix.
func hasPrefix(s, prefix []byte) bool {
	return len(s) >= len(prefix) && bytes.Equal(s[:len(prefix)], prefix)
}

// ClearAllEmbeddings removes embeddings from all nodes, allowing them to be regenerated.
// Returns the number of nodes that had their embeddings cleared.
func (b *BadgerEngine) ClearAllEmbeddings() (int, error) {
	return b.ClearAllEmbeddingsForPrefix("")
}

// ClearAllEmbeddingsForPrefix removes embeddings from nodes whose IDs start with the given prefix.
// This is used to clear embeddings for a single logical database namespace (e.g., "nornic:").
//
// If idPrefix is empty, clears embeddings for all nodes.
func (b *BadgerEngine) ClearAllEmbeddingsForPrefix(idPrefix string) (int, error) {
	if err := b.ensureOpen(); err != nil {
		return 0, err
	}

	cleared := 0

	// First, collect all node IDs that have embeddings
	var nodeIDs []NodeID
	err := b.withView(func(txn *badger.Txn) error {
		keyPrefix := []byte{prefixNode}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(keyPrefix, 100))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			// Extract nodeID from key (skip prefix byte)
			key := item.Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])
			if idPrefix != "" && !strings.HasPrefix(string(nodeID), idPrefix) {
				continue
			}

			err := item.Value(func(val []byte) error {
				node, err := b.decodeNodeWithEmbeddings(txn, val, nodeID)
				if err != nil {
					return nil // Skip invalid nodes
				}
				if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
					nodeIDs = append(nodeIDs, node.ID)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("error scanning nodes: %w", err)
	}

	// Now update each node to clear its embedding
	for _, id := range nodeIDs {
		node, err := b.GetNode(id)
		if err != nil {
			continue // Skip if node no longer exists
		}
		node.ChunkEmbeddings = nil
		if err := b.UpdateNode(node); err != nil {
			b.log.Warn("failed to clear embedding for node",
				"subsystem", "embeddings_index",
				"node_id", string(id),
				slog.Any("error", err),
			)
			continue
		}
		cleared++
	}

	b.log.Info("cleared embeddings",
		"subsystem", "embeddings_index",
		"cleared", cleared,
	)

	// Refresh the pending embeddings index so nodes get re-processed
	added := b.RefreshPendingEmbeddingsIndex()
	if added > 0 {
		b.log.Info("added nodes to pending embeddings queue",
			"subsystem", "embeddings_index",
			"added", added,
		)
	}

	return cleared, nil
}
