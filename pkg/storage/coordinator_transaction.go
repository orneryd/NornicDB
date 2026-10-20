package storage

import "context"

// CoordinatorOwnedTransactionMetadataKey identifies a participant whose
// terminal outcome belongs to a Fabric coordinator. Independent commits by a
// batch-owning procedure cannot be rolled back by that coordinator.
const CoordinatorOwnedTransactionMetadataKey = "nornicdb.fabric.coordinator_owned"

type coordinatorOwnedTransactionKey struct{}

func WithCoordinatorOwnedTransaction(ctx context.Context) context.Context {
	return context.WithValue(ctx, coordinatorOwnedTransactionKey{}, true)
}

func CoordinatorOwnsTransaction(ctx context.Context) bool {
	owned, _ := ctx.Value(coordinatorOwnedTransactionKey{}).(bool)
	return owned
}

func coordinatorTransactionMetadata(ctx context.Context) map[string]interface{} {
	if !CoordinatorOwnsTransaction(ctx) {
		return nil
	}
	return map[string]interface{}{CoordinatorOwnedTransactionMetadataKey: true}
}
