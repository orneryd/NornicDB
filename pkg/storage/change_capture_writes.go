package storage

import "errors"

// inMutationTransaction uses the native transaction owner when a write must
// validate an observed predecessor or capture changes. Blind writes without
// capture retain their direct path. id identifies the target namespace.
func (b *BadgerEngine) inMutationTransaction(id string, observed bool, write func(tx *BadgerTransaction) error) (handled bool, err error) {
	namespace, _, ok := ParseDatabasePrefix(id)
	if !observed && (!ok || b.ChangeCapture(namespace) == ChangeCaptureOff) {
		return false, nil
	}
	tx, err := BeginEntityTransaction(b, CommitModeDirect)
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
func ApplyNodeUpdate(tx *BadgerTransaction, updateRequest NodeUpdateRequest) error {
	node := updateRequest.After
	err := tx.UpdateGraphNode(updateRequest)
	if errors.Is(err, ErrNotFound) {
		_, err = tx.CreateNode(node)
	}
	return err
}
