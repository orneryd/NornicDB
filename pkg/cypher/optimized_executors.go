package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// BatchGetNodes retrieves multiple nodes in a single operation
// Used for large result set optimization
func (e *StorageExecutor) BatchGetNodes(ctx context.Context, ids []storage.NodeID) map[storage.NodeID]*storage.Node {
	if err := ctx.Err(); err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	result := make(map[storage.NodeID]*storage.Node, len(ids))
	for _, id := range ids {
		node, err := e.storage.GetNode(ctx, id)
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				recordExpressionFailure(ctx, canceled)
				return nil
			}
			continue
		}
		if node != nil {
			result[id] = node
		}
	}
	if err := ctx.Err(); err != nil {
		recordExpressionFailure(ctx, err)
		return nil
	}
	return result
}
