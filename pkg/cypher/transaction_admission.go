package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

// beginTransactionSnapshot opens a transaction snapshot directly on the
// transaction-capable engine. Writes are committed synchronously by their
// statements, so no cache visibility barrier is needed.
func beginTransactionSnapshot(txEngine TransactionCapableEngine) (*storage.BadgerTransaction, error) {
	return txEngine.BeginTransaction()
}
