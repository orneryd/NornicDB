package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// beginTransactionSnapshot opens a transaction snapshot directly on the
// transaction-capable engine. Writes are committed synchronously by their
// statements, so no cache visibility barrier is needed. The transaction
// carries the request's admission (storage.BeginAdmittedTransaction).
func beginTransactionSnapshot(ctx context.Context, txEngine TransactionCapableEngine) (*storage.BadgerTransaction, error) {
	return storage.BeginAdmittedTransaction(ctx, txEngine)
}
