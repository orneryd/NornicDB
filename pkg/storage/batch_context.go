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
		return ReadWithCapability(engine, func(reader BatchNodeWithoutEmbeddingsReader) (map[NodeID]*Node, error) {
			if contextual, ok := OwnedCapability[interface {
				BatchGetNodesWithoutEmbeddingsWithContext(context.Context, []NodeID) (map[NodeID]*Node, error)
			}](reader); ok {
				return contextual.BatchGetNodesWithoutEmbeddingsWithContext(ctx, ids)
			}
			nodes, err := reader.BatchGetNodesWithoutEmbeddings(ids)
			if err == nil {
				err = ctx.Err()
			}
			return nodes, err
		}, nil)
	} else {
		if reader, ok := OwnedCapability[interface {
			BatchGetNodesWithContext(context.Context, []NodeID) (map[NodeID]*Node, error)
		}](engine); ok {
			return reader.BatchGetNodesWithContext(ctx, ids)
		}
		nodes, err = engine.BatchGetNodes(ids)
	}
	if err == nil {
		err = ctx.Err()
	}
	return nodes, err
}
