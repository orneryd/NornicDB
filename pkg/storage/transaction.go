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
// It is also JSON-serialized into the WAL buffered-commit payload, so every
// field carries a tag.
type Operation struct {
	Type      OperationType `json:"type"`
	Timestamp time.Time     `json:"timestamp,omitempty"`

	// For node operations
	NodeID  NodeID `json:"node_id,omitempty"`
	Node    *Node  `json:"node,omitempty"`     // New state (for create/update) or nil
	OldNode *Node  `json:"old_node,omitempty"` // Old state (for update/delete rollback)
	// For delete operations that cascade (e.g., DeleteNode deletes edges).
	EdgesDeleted     int64          `json:"edges_deleted,omitempty"`
	DeletedEdgeIDs   []EdgeID       `json:"deleted_edge_ids,omitempty"`
	DeletedEdgeFacts []AffectedFact `json:"deleted_edge_facts,omitempty"`

	// For edge operations
	EdgeID  EdgeID `json:"edge_id,omitempty"`
	Edge    *Edge  `json:"edge,omitempty"`     // New state (for create/update) or nil
	OldEdge *Edge  `json:"old_edge,omitempty"` // Old state (for update/delete rollback)
	// ReplacesExisting is decode-only compatibility for buffered WAL records
	// acknowledged by the short-lived 68db5d16d release writer. Current
	// writers never set it: CREATE no longer replaces a stored relationship.
	// Recovery must nevertheless preserve those records' zero-cardinality
	// meaning instead of publishing them as a second physical relationship.
	ReplacesExisting bool `json:"replaces_existing,omitempty"`

	// FreshID records proof that a prior MVCC head cannot exist for
	// this create. The commit can use the new version as its floor.
	// Recreated IDs retain the persisted floor.
	FreshID bool `json:"fresh_id,omitempty"`

	// Runtime fields never enter WAL or persisted data. Pack their small flags
	// beside the existing flags so observed intent does not grow each operation.
	nodeAnchorEligible bool // before-image is committed, not staged
	skipMissingDelete  bool
	embeddingOutcome   bool
	nodeImagePolicy    EntityImagePolicy
	nodeObservedBefore *Node
}

// Transaction is the public closure-facing transaction type used by DB.Update and DB.View.
type Transaction = BadgerTransaction

// copyNode creates a deep copy of a node that the caller owns: label, property,
// embedding-metadata and embedding-vector images are all its own. Used by
// transactions to preserve state for rollback, by the exported CopyNode, and by
// a query read whose delivered node belongs to the caller.
func copyNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	return copyNodeWithChunks(node, copyChunkVectors(node.ChunkEmbeddings))
}

// copyNodeWithChunks detaches the body and named vectors. The caller supplies
// chunk vectors it owns or can safely share between immutable stored images.
func copyNodeWithChunks(node *Node, chunks [][]float32) *Node {
	nodeCopy := copyNodeWithoutEmbeddings(node)
	nodeCopy.ChunkEmbeddings = chunks
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

func copyChunkVectors(chunks [][]float32) [][]float32 {
	if len(chunks) == 0 {
		return nil
	}
	result := make([][]float32, len(chunks))
	for i, vector := range chunks {
		result[i] = make([]float32, len(vector))
		copy(result[i], vector)
	}
	return result
}

// copyNodeWithoutEmbeddings copies the body image of node. A copy holds the
// value it copies: an absent label list stays absent, as an absent property or
// embedding-metadata map does. A cached body is a copy of a decoded body, so
// inventing an empty label list here would make a cache hit differ from the
// decode it replaces (snapshot frames, CDC events and caller comparisons read
// that image).
func copyNodeWithoutEmbeddings(node *Node) *Node {
	if node == nil {
		return nil
	}

	// The runtime record is immutable, so every copy of an image shares it:
	// a mutator replaces the record on the image it is called for. Reading the
	// source nil-safe keeps a detached copy of a zero node zero.
	nodeCopy := &Node{
		runtime: node.runtime,
		ID:      node.ID,
	}
	nodeCopy.CreatedAt = node.CreatedAt
	nodeCopy.UpdatedAt = node.UpdatedAt
	if node.Labels != nil {
		nodeCopy.Labels = append(make([]string, 0, len(node.Labels)), node.Labels...)
	}

	if node.Properties != nil {
		nodeCopy.Properties = ownedEntries(nil, node.Properties)
	}

	if node.EmbedMeta != nil {
		nodeCopy.EmbedMeta = ownedEntries(nil, node.EmbedMeta)
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
		Database:             edge.Database,
		ID:                   edge.ID,
		StartNode:            edge.StartNode,
		EndNode:              edge.EndNode,
		Type:                 edge.Type,
		CreatedAt:            edge.CreatedAt,
		UpdatedAt:            edge.UpdatedAt,
		Confidence:           edge.Confidence,
		AutoGenerated:        edge.AutoGenerated,
		VisibilitySuppressed: edge.VisibilitySuppressed,
	}

	if edge.Properties != nil {
		edgeCopy.Properties = ownedEntries(nil, edge.Properties)
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
