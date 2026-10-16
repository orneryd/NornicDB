package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Preserve database admission/accounting wrappers when committing the raw
// transaction opened by unwrapping the storage engine. Both explicit and
// implicit transactions must use this boundary.
func (e *StorageExecutor) commitStorageTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	engine := e.storage
	visited := make(map[storage.Engine]bool)
	for engine != nil && !visited[engine] {
		visited[engine] = true
		if committer, ok := engine.(interface {
			CommitTransaction(context.Context, *storage.BadgerTransaction) error
		}); ok {
			return committer.CommitTransaction(ctx, tx)
		}
		wrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		engine = wrapper.GetInnerEngine()
	}
	return tx.CommitContext(ctx)
}
