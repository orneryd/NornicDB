package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var _ storage.MergeNodePreparer = (*transactionStorageWrapper)(nil)

// PrepareMergeNode admits a MERGE node's unique keys in the statement's
// transaction (storage.MergeNodePreparer); a view without one admits nothing.
// A composite statement's participant storage begins its transaction for it
// without claiming the participant (mergeTransaction): a MERGE that matches
// writes nothing.
func (w *transactionStorageWrapper) PrepareMergeNode(ctx context.Context, labels []string, props map[string]interface{}) error {
	tx, err := w.mergeTransaction()
	if err != nil || tx == nil {
		return err
	}
	return tx.PrepareMergeNode(ctx, labels, props)
}

// PrepareMergeNodeForWrite admits a MERGE node its statement writes
// (storage.MergeNodePreparer): its keys are held even when the node exists.
func (w *transactionStorageWrapper) PrepareMergeNodeForWrite(ctx context.Context, labels []string, props map[string]interface{}) error {
	tx, err := w.mergeTransaction()
	if err != nil || tx == nil {
		return err
	}
	return tx.PrepareMergeNodeForWrite(ctx, labels, props)
}

// mergeTransaction is the transaction a MERGE admits its keys in: the
// storage's own, begun now on a participant's storage that has none yet.
func (w *transactionStorageWrapper) mergeTransaction() (*storage.BadgerTransaction, error) {
	if tx := w.transaction(); tx != nil || w.lazy == nil {
		return tx, nil
	}
	return w.lazy.begun()
}
