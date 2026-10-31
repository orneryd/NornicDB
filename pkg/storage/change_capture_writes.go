package storage

import "errors"

// inMutationTransaction uses the native transaction owner when a write must
// validate an observed predecessor or capture changes. Blind writes without
// capture retain their direct path. id identifies the target namespace. The
// transaction carries the write's vector-work admission (origins), or one
// admitted at this counted owner site when the write carries none.
func (b *BadgerEngine) inMutationTransaction(id string, observed bool, write func(tx *BadgerTransaction) error, origins ...uint64) (handled bool, err error) {
	namespace, _, ok := ParseDatabasePrefix(id)
	if !observed && (!ok || b.ChangeCapture(namespace) == ChangeCaptureOff) {
		return false, nil
	}
	var origin uint64
	if len(origins) > 0 {
		origin = origins[0]
	} else {
		origin = fallbackVectorWorkOrigin("storage.mutation-transaction-write")
	}
	tx, err := BeginEntityTransaction(b, CommitModeDirect, origin)
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
