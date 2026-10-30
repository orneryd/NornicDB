package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// HasActiveTransaction reports whether this executor still owns an explicit transaction.
func (e *StorageExecutor) HasActiveTransaction() bool {
	return e != nil && e.txContext != nil && e.txContext.active
}

// HasPendingTransactionWrites reports whether the active explicit transaction
// has staged graph mutations. Unknown transaction implementations return true
// so cache coherence fails safe.
func (e *StorageExecutor) HasPendingTransactionWrites() bool {
	if e == nil || e.txContext == nil || !e.txContext.active {
		return false
	}
	if tx, ok := e.txContext.tx.(*storage.BadgerTransaction); ok {
		return tx.OperationCount() > 0
	}
	return true
}
