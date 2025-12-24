// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// Backup creates a backup of the database to the specified file path.
// Uses BadgerDB's streaming backup which creates a consistent snapshot.
// The backup file is a self-contained, portable copy of the database.
func (b *BadgerEngine) Backup(path string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrStorageClosed
	}

	// Create backup file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer f.Close()

	// Use BufferedWriter for better performance
	buf := bufio.NewWriterSize(f, 16*1024*1024) // 16MB buffer

	// Stream backup (since=0 means full backup)
	_, err = b.db.Backup(buf, 0)
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}

	// Flush buffer
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("failed to flush backup: %w", err)
	}

	// Sync to disk
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync backup: %w", err)
	}

	return nil
}

// DeleteByPrefix deletes all nodes and edges with IDs starting with the given prefix.
// Used for DROP DATABASE operations to delete all data in a namespace.
//
// This iterates over all nodes and edges, checking if their IDs start with the prefix.
// For each matching node, it calls DeleteNode which handles cleanup of:
//   - The node itself
//   - All label indexes
//   - All connected edges (outgoing and incoming)
//   - Pending embedding indexes
//
// Returns the number of nodes and edges deleted.
func (b *BadgerEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	if prefix == "" {
		return 0, 0, fmt.Errorf("prefix cannot be empty")
	}

	if err := b.ensureOpen(); err != nil {
		return 0, 0, err
	}

	var nodeIDs []NodeID
	var edgeIDs []EdgeID

	// First pass: collect all node IDs with the prefix
	err = b.withView(func(txn *badger.Txn) error {
		nodePrefix := []byte{prefixNode}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(nodePrefix, 0))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			// Extract nodeID from key (skip prefix byte)
			key := it.Item().Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])

			var node *Node
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = decodeNodeWithEmbeddings(txn, val, nodeID)
				return decodeErr
			}); err != nil {
				continue
			}

			// Check if node ID starts with prefix
			if strings.HasPrefix(string(node.ID), prefix) {
				nodeIDs = append(nodeIDs, node.ID)
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	// Second pass: collect all edge IDs with the prefix
	// (in case there are orphaned edges)
	err = b.withView(func(txn *badger.Txn) error {
		edgePrefix := []byte{prefixEdge}
		it := txn.NewIterator(badgerIterOptsPrefetchValues(edgePrefix, 0))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			var edge *Edge
			if err := it.Item().Value(func(val []byte) error {
				var decodeErr error
				edge, decodeErr = decodeEdge(val)
				return decodeErr
			}); err != nil {
				continue
			}

			// Check if edge ID starts with prefix
			if strings.HasPrefix(string(edge.ID), prefix) {
				edgeIDs = append(edgeIDs, edge.ID)
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	// Delete all nodes (this will also delete connected edges)
	var totalEdgesDeleted int64
	for _, nodeID := range nodeIDs {
		// Count edges connected to this node before deletion
		outgoing, _ := b.GetOutgoingEdges(nodeID)
		incoming, _ := b.GetIncomingEdges(nodeID)
		edgeCountBefore := int64(len(outgoing) + len(incoming))

		// Delete node (this deletes all connected edges)
		if err := b.DeleteNode(nodeID); err != nil {
			// Continue on error - some nodes might already be deleted
			continue
		}

		nodesDeleted++
		totalEdgesDeleted += edgeCountBefore
	}

	// Delete any remaining orphaned edges (edges not connected to any node)
	// This shouldn't happen in practice, but handle it for safety
	for _, edgeID := range edgeIDs {
		// Check if edge still exists (might have been deleted with node)
		_, err := b.GetEdge(edgeID)
		if err == ErrNotFound {
			continue // Already deleted
		}
		if err != nil {
			continue
		}

		// Delete orphaned edge
		if err := b.DeleteEdge(edgeID); err == nil {
			totalEdgesDeleted++
		}
	}

	edgesDeleted = totalEdgesDeleted
	return nodesDeleted, edgesDeleted, nil
}

// Verify BadgerEngine implements Engine interface
var _ Engine = (*BadgerEngine)(nil)
