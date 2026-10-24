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
