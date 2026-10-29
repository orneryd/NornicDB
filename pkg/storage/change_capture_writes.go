package storage

import (
	"context"
	"errors"
)

// inMutationTransaction uses the native transaction owner when a write must
// validate an observed predecessor or capture changes. Blind writes without
// capture retain their direct path. id identifies the target namespace; ctx is
// the write's context: the transaction begins, prepares (write) and commits
// under it, and ends on every outcome.
func (b *BadgerEngine) inMutationTransaction(ctx context.Context, id string, observed bool, write func(tx *BadgerTransaction) error) (handled bool, err error) {
	namespace, _, ok := ParseDatabasePrefix(id)
	if !observed && (!ok || b.ChangeCapture(namespace) == ChangeCaptureOff) {
		return false, nil
	}
	tx, err := beginEntityTransaction(ctx, b, CommitModeDirect)
	if err != nil {
		return true, err
	}
	if err := write(tx); err != nil {
		_ = tx.Rollback()
		return true, err
	}
	// The transaction is this owner's alone: a commit that fails before its
	// own cleanup (a cancelled request at entry) still ends it here.
	if err := tx.CommitContext(ctx); err != nil {
		_ = tx.Rollback()
		return true, err
	}
	return true, nil
}

// ApplyNodeUpdate is UpdateNode's upsert in a transaction: an
// update, or a create when the node doesn't exist.
func ApplyNodeUpdate(tx *BadgerTransaction, updateRequest NodeUpdateRequest) error {
	node := updateRequest.After
	err := tx.UpdateGraphNode(updateRequest)
	if errors.Is(err, ErrNotFound) {
		_, err = tx.CreateNode(node)
	}
	return err
}
