package storage

import "context"

// GetNodeEmbeddingMeta delegates to the wrapped engine on the caller's context.
func (w *WALEngine) GetNodeEmbeddingMeta(ctx context.Context, id NodeID) (map[string]any, error) {
	return embeddingMetaOf(ctx, w.engine, id)
}

// GetNodeEmbeddingMeta reads the metadata of the node's namespaced ID.
func (n *NamespacedEngine) GetNodeEmbeddingMeta(ctx context.Context, id NodeID) (map[string]any, error) {
	return embeddingMetaOf(ctx, n.inner, n.PrefixNodeID(id))
}

// embeddingMetaOf is the node's embedding metadata from engine: its
// metadata-only read when it has one, else the full node's. Both reads run on
// the caller's context; neither falls back to a detached lifetime.
func embeddingMetaOf(ctx context.Context, engine Engine, id NodeID) (map[string]any, error) {
	if reader, ok := engine.(NodeEmbeddingMetaReader); ok {
		return reader.GetNodeEmbeddingMeta(ctx, id)
	}
	node, err := engine.GetNode(ctx, id)
	if err != nil {
		return nil, err
	}
	return node.EmbedMeta, nil
}
