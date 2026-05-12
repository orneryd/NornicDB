// Package storage - Transaction types shared between storage implementations.
//
// This file defines shared transaction types used by BadgerTransaction.
// All transactions in NornicDB use Badger's native transaction system which
// provides real ACID guarantees through the Write-Ahead Log (WAL).
//
// # ACID Guarantees
//
// Transactions provide:
//   - Atomicity: All operations commit together or none do
//   - Consistency: Constraints validated before commit
//   - Isolation: Changes invisible until commit
//   - Durability: WAL ensures persistence even on crash
//
// # Usage
//
// All engines (including MemoryEngine) use BadgerTransaction:
//
//	engine := storage.NewMemoryEngine() // or NewBadgerEngine()
//	tx, err := engine.BeginTransaction()
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback() // Rollback if not committed
//
//	tx.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
//	tx.CreateNode(&Node{ID: "n2", Labels: []string{"Person"}})
//
//	return tx.Commit() // Atomic - both succeed or both fail
package storage

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Transaction errors
var (
	ErrNoTransaction       = errors.New("no active transaction")
	ErrTransactionActive   = errors.New("transaction already active")
	ErrTransactionClosed   = errors.New("transaction already closed")
	ErrTransactionRollback = errors.New("transaction rolled back")
)

// generateTxID generates a unique transaction ID using UUID v4.
func generateTxID() string {
	return "tx-" + uuid.New().String()
}

// TransactionStatus represents the current state of a transaction.
type TransactionStatus string

const (
	TxStatusActive     TransactionStatus = "active"
	TxStatusCommitted  TransactionStatus = "committed"
	TxStatusRolledBack TransactionStatus = "rolled_back"
)

// OperationType represents the type of operation in a transaction.
type OperationType string

const (
	OpCreateNode      OperationType = "create_node"
	OpUpdateNode      OperationType = "update_node"
	OpDeleteNode      OperationType = "delete_node"
	OpCreateEdge      OperationType = "create_edge"
	OpUpdateEdge      OperationType = "update_edge"
	OpDeleteEdge      OperationType = "delete_edge"
	OpUpdateEmbedding OperationType = "update_embedding" // Safe to skip on corruption - regenerable
)

// Operation represents a single operation within a transaction.
// Used by BadgerTransaction to track operations for constraint validation.
type Operation struct {
	Type      OperationType
	Timestamp time.Time

	// For node operations
	NodeID  NodeID
	Node    *Node // New state (for create/update) or nil
	OldNode *Node // Old state (for update/delete rollback)
	// For delete operations that cascade (e.g., DeleteNode deletes edges).
	EdgesDeleted   int64
	DeletedEdgeIDs []EdgeID

	// For edge operations
	EdgeID  EdgeID
	Edge    *Edge // New state (for create/update) or nil
	OldEdge *Edge // Old state (for update/delete rollback)

	// FreshID is set on OpCreateNode / OpCreateEdge when the caller asserted
	// the ID is newly minted and cannot collide with any prior tombstoned
	// MVCC head (the same contract that lets CreateNode skip its existence
	// read). When true, the commit loop writes the MVCC head without the
	// load-existing-floor round-trip. Safe default is false — the commit
	// loop falls back to the read-before-write path, preserving snapshot
	// semantics for recreated user-supplied IDs.
	FreshID bool
}

// Transaction is the public closure-facing transaction type used by DB.Update and DB.View.
type Transaction = BadgerTransaction

// copyNode creates a deep copy of a node.
// Used by transactions to preserve state for rollback.
func copyNode(node *Node) *Node {
	if node == nil {
		return nil
	}

	nodeCopy := &Node{
		ID:                         node.ID,
		Labels:                     make([]string, 0, len(node.Labels)),
		CreatedAt:                  node.CreatedAt,
		UpdatedAt:                  node.UpdatedAt,
		EmbeddingsStoredSeparately: node.EmbeddingsStoredSeparately,
	}
	nodeCopy.Labels = append(nodeCopy.Labels, node.Labels...)

	if node.Properties != nil {
		nodeCopy.Properties = make(map[string]interface{})
		for k, v := range node.Properties {
			nodeCopy.Properties[k] = v
		}
	}

	if node.EmbedMeta != nil {
		nodeCopy.EmbedMeta = make(map[string]any, len(node.EmbedMeta))
		for k, v := range node.EmbedMeta {
			nodeCopy.EmbedMeta[k] = v
		}
	}

	if len(node.ChunkEmbeddings) > 0 {
		nodeCopy.ChunkEmbeddings = make([][]float32, len(node.ChunkEmbeddings))
		for i, emb := range node.ChunkEmbeddings {
			nodeCopy.ChunkEmbeddings[i] = make([]float32, len(emb))
			copy(nodeCopy.ChunkEmbeddings[i], emb)
		}
	}

	if node.NamedEmbeddings != nil {
		nodeCopy.NamedEmbeddings = make(map[string][]float32, len(node.NamedEmbeddings))
		for name, emb := range node.NamedEmbeddings {
			embCopy := make([]float32, len(emb))
			copy(embCopy, emb)
			nodeCopy.NamedEmbeddings[name] = embCopy
		}
	}

	return nodeCopy
}

// copyEdge creates a deep copy of an edge.
// Used by transactions to preserve state for rollback.
func copyEdge(edge *Edge) *Edge {
	if edge == nil {
		return nil
	}

	edgeCopy := &Edge{
		ID:            edge.ID,
		StartNode:     edge.StartNode,
		EndNode:       edge.EndNode,
		Type:          edge.Type,
		CreatedAt:     edge.CreatedAt,
		UpdatedAt:     edge.UpdatedAt,
		Confidence:    edge.Confidence,
		AutoGenerated: edge.AutoGenerated,
	}

	if edge.Properties != nil {
		edgeCopy.Properties = make(map[string]interface{})
		for k, v := range edge.Properties {
			edgeCopy.Properties[k] = v
		}
	}

	return edgeCopy
}

// CopyNode is the exported version of copyNode for use by other packages.
func CopyNode(node *Node) *Node {
	return copyNode(node)
}

// CopyEdge is the exported version of copyEdge for use by other packages.
func CopyEdge(edge *Edge) *Edge {
	return copyEdge(edge)
}
