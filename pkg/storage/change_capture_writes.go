package storage

import "errors"

// inCommitTransaction runs a direct (non-transactional) write as a
// transaction when the database it writes captures changes or has a commit
// hook, so that its changes reach the one commit point,
// BadgerTransaction.Commit: change capture records them (#937), the hook
// sees them (commit_hooks.go). handled is false for any other database,
// which keeps the direct write. id is any of the write's prefixed entity IDs.
func (b *BadgerEngine) inCommitTransaction(id string, write func(tx *BadgerTransaction) error) (handled bool, err error) {
	namespace, _, ok := ParseDatabasePrefix(id)
	if !ok || !b.needsCommitTransaction(namespace) {
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

// upsertNodeInTransaction is UpdateNode's upsert in a transaction: an
// update, or a create when the node doesn't exist.
func upsertNodeInTransaction(tx *BadgerTransaction, node *Node) error {
	err := tx.UpdateNode(node)
	if errors.Is(err, ErrNotFound) {
		_, err = tx.CreateNode(node)
	}
	return err
}
