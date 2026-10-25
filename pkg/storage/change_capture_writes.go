package storage

import "errors"

// inCaptureTransaction runs a direct (non-transactional) write as a
// transaction when the database it writes captures changes, so that its
// changes are recorded by the one capture point, BadgerTransaction.Commit
// (#937); handled is false for a database that doesn't capture, which keeps
// the direct write. id is any of the write's prefixed entity IDs.
func (b *BadgerEngine) inCaptureTransaction(id string, write func(tx *BadgerTransaction) error, origins ...uint64) (handled bool, err error) {
	origin := CaptureVectorWorkOrigin()
	if len(origins) > 0 {
		origin = origins[0]
	}
	namespace, _, ok := ParseDatabasePrefix(id)
	if !ok || b.ChangeCapture(namespace) == ChangeCaptureOff {
		return false, nil
	}
	tx, err := b.BeginTransaction()
	if err != nil {
		return true, err
	}
	tx.mu.Lock()
	tx.bindVectorWorkOriginLocked(origin)
	tx.mu.Unlock()
	if err := write(tx); err != nil {
		_ = tx.Rollback()
		return true, err
	}
	return true, tx.Commit()
}

// ApplyNodeUpdate is UpdateNode's upsert in a transaction: an
// update, or a create when the node doesn't exist.
func ApplyNodeUpdate(tx *BadgerTransaction, node *Node) error {
	err := tx.UpdateGraphNode(node, ClientNodeEdits)
	if errors.Is(err, ErrNotFound) {
		_, err = tx.CreateNode(node)
	}
	return err
}
