package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type transactionMetadataKey struct{}

// WithTransactionMetadata carries a client's transaction metadata (Bolt's
// tx_metadata, on BEGIN or on RUN for an auto-commit transaction) to the
// transaction the executor opens for ctx: it is that transaction's metadata,
// as if tx.setMetaData set it first, and change events and SHOW
// TRANSACTIONS see it. Empty metadata leaves ctx
// as it is.
func WithTransactionMetadata(ctx context.Context, metadata map[string]any) context.Context {
	if len(metadata) == 0 {
		return ctx
	}
	return context.WithValue(ctx, transactionMetadataKey{}, metadata)
}

// applyTransactionMetadata sets the metadata ctx carries on tx, and who the
// transaction runs for (StatementUsers), which its change events record.
func applyTransactionMetadata(ctx context.Context, tx *storage.BadgerTransaction) error {
	if tx == nil {
		return nil
	}
	if executing, authenticated := StatementUsers(ctx); executing != "" {
		if err := tx.SetUsers(executing, authenticated); err != nil {
			return err
		}
	}
	metadata, _ := ctx.Value(transactionMetadataKey{}).(map[string]any)
	if len(metadata) == 0 {
		return nil
	}
	return tx.SetMetadata(metadata)
}
