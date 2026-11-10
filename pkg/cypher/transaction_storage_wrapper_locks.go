package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var _ storage.MergeNodePreparer = (*transactionStorageWrapper)(nil)

// PrepareMergeNode admits a MERGE node's unique keys in the statement's
// transaction (storage.MergeNodePreparer); a view without one admits nothing.
func (w *transactionStorageWrapper) PrepareMergeNode(ctx context.Context, labels []string, props map[string]interface{}) error {
	if w.tx == nil {
		return nil
	}
	return w.tx.PrepareMergeNode(ctx, labels, props)
}

// PrepareMergeNodeForWrite admits a MERGE node its statement writes
// (storage.MergeNodePreparer): its keys are held even when the node exists.
func (w *transactionStorageWrapper) PrepareMergeNodeForWrite(ctx context.Context, labels []string, props map[string]interface{}) error {
	if w.tx == nil {
		return nil
	}
	return w.tx.PrepareMergeNodeForWrite(ctx, labels, props)
}

var _ storage.EntityWriteLocker = (*transactionStorageWrapper)(nil)

// LockNodeForWrite takes a node's write lock in the statement's transaction
// (storage.EntityWriteLocker) and returns its latest committed image, in the
// wrapper's namespace, when the transaction's snapshot predates it.
func (w *transactionStorageWrapper) LockNodeForWrite(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if w.tx == nil {
		return nil, nil
	}
	node, err := w.tx.LockNodeForWrite(ctx, w.prefixNodeID(id))
	if err != nil || node == nil || w.namespace == "" {
		return node, err
	}
	return w.toUserNode(node), nil
}

// LockEdgeForWrite takes a relationship's write lock in the statement's
// transaction, as LockNodeForWrite does for a node.
func (w *transactionStorageWrapper) LockEdgeForWrite(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	if w.tx == nil {
		return nil, nil
	}
	edge, err := w.tx.LockEdgeForWrite(ctx, w.prefixEdgeID(id))
	if err != nil || edge == nil || w.namespace == "" {
		return edge, err
	}
	return w.toUserEdge(edge), nil
}

// lockNodeForWrite takes node's write lock before a clause changes it, so a
// concurrent writer of the node waits for this transaction instead of
// failing at commit (NOR-213). When another transaction committed the node
// after this one's snapshot, the row's node becomes that committed image, so
// the clause's expressions read it and its change is applied on top of it.
func lockNodeForWrite(ctx context.Context, store storage.Engine, node *storage.Node) error {
	locker, ok := store.(storage.EntityWriteLocker)
	if !ok || node == nil {
		return nil
	}
	latest, err := locker.LockNodeForWrite(ctx, node.ID)
	if err != nil || latest == nil {
		return err
	}
	*node = *latest
	return nil
}

// lockEdgeForWrite takes relationship's write lock before a clause changes
// it, as lockNodeForWrite does for a node.
func lockEdgeForWrite(ctx context.Context, store storage.Engine, relationship *storage.Edge) error {
	locker, ok := store.(storage.EntityWriteLocker)
	if !ok || relationship == nil {
		return nil
	}
	latest, err := locker.LockEdgeForWrite(ctx, relationship.ID)
	if err != nil || latest == nil {
		return err
	}
	*relationship = *latest
	return nil
}
