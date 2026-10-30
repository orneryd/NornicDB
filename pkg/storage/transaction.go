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
	"context"
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
	// Runtime notification kind; never part of WAL or persisted data.
	embeddingOutcome bool
	Type             OperationType `json:"type"`
	Timestamp        time.Time     `json:"timestamp,omitempty"`

	// For node operations
	NodeID  NodeID `json:"node_id,omitempty"`
	Node    *Node  `json:"node,omitempty"`     // New state (for create/update) or nil
	OldNode *Node  `json:"old_node,omitempty"` // Old state (for update/delete rollback)
	// For delete operations that cascade (e.g., DeleteNode deletes edges).
	EdgesDeleted   int64    `json:"edges_deleted,omitempty"`
	DeletedEdgeIDs []EdgeID `json:"deleted_edge_ids,omitempty"`

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

	// FreshID is set on OpCreateNode / OpCreateEdge when the caller asserted
	// the ID is newly minted and cannot collide with any prior tombstoned
	// MVCC head (the same contract that lets CreateNode skip its existence
	// read). When true, the commit loop writes the MVCC head without the
	// load-existing-floor round-trip. Safe default is false — the commit
	// loop falls back to the read-before-write path, preserving snapshot
	// semantics for recreated user-supplied IDs.
	FreshID bool `json:"fresh_id,omitempty"`
}

// Transaction is the public closure-facing transaction type used by DB.Update and DB.View.
type Transaction = BadgerTransaction

// copyNode creates a deep copy of a node.
// Used by transactions to preserve state for rollback.
func copyNode(node *Node) *Node {
	image, _ := copyNodeWithContext(context.Background(), node)
	return image
}

func copyNodeWithContext(ctx context.Context, node *Node) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nodeCopy, err := copyNodeWithoutEmbeddingsWithContext(ctx, node)
	if err != nil {
		return nil, err
	}
	if nodeCopy == nil {
		return nil, nil
	}

	if len(node.ChunkEmbeddings) > 0 {
		nodeCopy.ChunkEmbeddings = make([][]float32, len(node.ChunkEmbeddings))
		for i, emb := range node.ChunkEmbeddings {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			nodeCopy.ChunkEmbeddings[i] = make([]float32, len(emb))
			copy(nodeCopy.ChunkEmbeddings[i], emb)
		}
	}

	if node.NamedEmbeddings != nil {
		nodeCopy.NamedEmbeddings = make(map[string][]float32, len(node.NamedEmbeddings))
		for name, emb := range node.NamedEmbeddings {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			embCopy := make([]float32, len(emb))
			copy(embCopy, emb)
			nodeCopy.NamedEmbeddings[name] = embCopy
		}
	}

	return nodeCopy, ctx.Err()
}

// copyNodeForCaller is a copy of a node the transaction keeps in its snapshot
// caches, for a caller that may change it: its labels, properties and
// embedding metadata are its own, so a caller writing the node (a Cypher SET)
// leaves the cached snapshot copy, and the old version an update compares
// against, unchanged (#963). The embedding vectors, which callers replace
// rather than modify, are shared.
func copyNodeForCaller(node *Node) *Node {
	image, _ := copyNodeForCallerWithContext(context.Background(), node)
	return image
}

func copyNodeForCallerWithContext(ctx context.Context, node *Node) (*Node, error) {
	nodeCopy, err := copyNodeWithoutEmbeddingsWithContext(ctx, node)
	if err != nil {
		return nil, err
	}
	if nodeCopy == nil {
		return nil, nil
	}
	nodeCopy.ChunkEmbeddings = node.ChunkEmbeddings
	nodeCopy.NamedEmbeddings = node.NamedEmbeddings
	nodeCopy.VisibilitySuppressed = node.VisibilitySuppressed
	return nodeCopy, ctx.Err()
}

func copyNodeWithoutEmbeddings(node *Node) *Node {
	image, _ := copyNodeWithoutEmbeddingsWithContext(context.Background(), node)
	return image
}

func copyNodeWithoutEmbeddingsWithContext(ctx context.Context, node *Node) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}

	nodeCopy := &Node{
		Database:                   node.Database,
		embeddingOwner:             node.embeddingOwner,
		embeddingOwnerID:           node.embeddingOwnerID,
		ID:                         node.ID,
		embeddingRevision:          node.embeddingRevision,
		embeddingAttempt:           node.embeddingAttempt,
		embeddingPublication:       node.embeddingPublication,
		Labels:                     make([]string, 0, len(node.Labels)),
		CreatedAt:                  node.CreatedAt,
		UpdatedAt:                  node.UpdatedAt,
		EmbeddingsStoredSeparately: node.EmbeddingsStoredSeparately,
		VisibilitySuppressed:       node.VisibilitySuppressed,
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nodeCopy.Labels = append(nodeCopy.Labels, node.Labels...)

	if node.Properties != nil {
		nodeCopy.Properties = make(map[string]interface{}, len(node.Properties))
		for k, v := range node.Properties {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			nodeCopy.Properties[k] = v
		}
	}

	if node.EmbedMeta != nil {
		nodeCopy.EmbedMeta = make(map[string]any, len(node.EmbedMeta))
		for k, v := range node.EmbedMeta {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			nodeCopy.EmbedMeta[k] = v
		}
	}

	return nodeCopy, ctx.Err()
}

func copyNodeProjectedWithoutEmbeddings(node *Node, properties []string) *Node {
	image, _ := copyNodeProjectedWithoutEmbeddingsWithContext(context.Background(), node, properties)
	return image
}

func copyNodeProjectedWithoutEmbeddingsWithContext(ctx context.Context, node *Node, properties []string) (*Node, error) {
	out, err := copyNodeWithoutEmbeddingsWithContext(ctx, node)
	if err != nil {
		return nil, err
	}
	if out == nil || properties == nil {
		return out, nil
	}
	projected := make(map[string]any, len(properties))
	for _, property := range properties {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if value, ok := out.Properties[property]; ok {
			projected[property] = value
		}
	}
	out.Properties = projected
	return out, ctx.Err()
}

// copyEdge creates a deep copy of an edge.
// Used by transactions to preserve state for rollback.
func copyEdge(edge *Edge) *Edge {
	image, _ := copyEdgeWithContext(context.Background(), edge)
	return image
}

func copyEdgeWithContext(ctx context.Context, edge *Edge) (*Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if edge == nil {
		return nil, nil
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
		edgeCopy.Properties = make(map[string]interface{})
		for k, v := range edge.Properties {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			edgeCopy.Properties[k] = v
		}
	}

	return edgeCopy, nil
}

// CopyNode is the exported version of copyNode for use by other packages.
func CopyNode(node *Node) *Node {
	return copyNode(node)
}

// CopyNodeWithContext detaches the same rollback/notification image while
// observing caller-owned preparation. Property values retain their existing
// shallow-copy semantics; embedding vectors are detached as in CopyNode.
func CopyNodeWithContext(ctx context.Context, node *Node) (*Node, error) {
	return copyNodeWithContext(ctx, node)
}

// CopyEdge is the exported version of copyEdge for use by other packages.
func CopyEdge(edge *Edge) *Edge {
	return copyEdge(edge)
}

// CopyEdgeWithContext copies the rollback/read image while observing the caller
// inside its property pass. Property values retain CopyEdge's borrowing policy.
func CopyEdgeWithContext(ctx context.Context, edge *Edge) (*Edge, error) {
	return copyEdgeWithContext(ctx, edge)
}
