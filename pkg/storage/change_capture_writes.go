package storage

import (
	"context"
	"errors"
)

// inCaptureTransaction runs a direct (non-transactional) write as a
// transaction when the database it writes captures changes, so that its
// changes are recorded by the one capture point, BadgerTransaction.Commit
// (#937); handled is false for a database that doesn't capture, which keeps
// the direct write. id is any of the write's prefixed entity IDs.
func (b *BadgerEngine) inCaptureTransaction(id string, write func(tx *BadgerTransaction) error) (handled bool, err error) {
	namespace, _, ok := ParseDatabasePrefix(id)
	if !ok || b.ChangeCapture(namespace) == ChangeCaptureOff {
		return false, nil
	}
	tx, err := b.BeginTransaction()
	if err != nil {
		return true, err
	}
	if err := write(tx); err != nil {
		_ = tx.Rollback()
		return true, err
	}
	return true, tx.Commit()
}

// ApplyNodeUpdate is UpdateNode's upsert in a transaction: an
// update, or a create when the node doesn't exist.
func ApplyNodeUpdate(tx *BadgerTransaction, node *Node) error {
	return ApplyNodeUpdateWithContext(context.Background(), tx, node)
}

// ApplyNodeUpdateWithContext shares graph image admission and CREATE fallback.
func ApplyNodeUpdateWithContext(ctx context.Context, tx *BadgerTransaction, node *Node) error {
	err := tx.UpdateGraphNodeWithContext(ctx, node, ClientNodeEdits)
	if errors.Is(err, ErrNotFound) {
		_, err = tx.CreateNodeWithContext(ctx, node)
	}
	return err
}
