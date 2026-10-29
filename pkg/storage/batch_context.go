package storage

import "context"

// BatchGetNodesWithContext retains caller cancellation through optional native
// batch preparation. Legacy engines are checked before and after their call.
func BatchGetNodesWithContext(ctx context.Context, engine Engine, ids []NodeID) (map[NodeID]*Node, error) {
	return batchNodesWithContext(ctx, engine, ids, false)
}

func batchNodesWithContext(ctx context.Context, engine Engine, ids []NodeID, withoutEmbeddings bool) (map[NodeID]*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var nodes map[NodeID]*Node
	var err error
	if withoutEmbeddings {
		if reader, ok := engine.(interface {
			BatchGetNodesWithoutEmbeddingsWithContext(context.Context, []NodeID) (map[NodeID]*Node, error)
		}); ok {
			return reader.BatchGetNodesWithoutEmbeddingsWithContext(ctx, ids)
		}
		reader, ok := engine.(BatchNodeWithoutEmbeddingsReader)
		if !ok {
			return nil, ErrNotImplemented
		}
		nodes, err = reader.BatchGetNodesWithoutEmbeddings(ids)
	} else {
		if reader, ok := engine.(interface {
			BatchGetNodesWithContext(context.Context, []NodeID) (map[NodeID]*Node, error)
		}); ok {
			return reader.BatchGetNodesWithContext(ctx, ids)
		}
		nodes, err = engine.BatchGetNodes(ids)
	}
	if err == nil {
		err = ctx.Err()
	}
	return nodes, err
}
