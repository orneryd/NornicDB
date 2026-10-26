package storage

// GetNodeEmbeddingMeta delegates to the wrapped engine.
func (w *WALEngine) GetNodeEmbeddingMeta(id NodeID) (map[string]any, error) {
	return embeddingMetaOf(w.engine, id)
}

// GetNodeEmbeddingMeta reads the metadata of the node's namespaced ID.
func (n *NamespacedEngine) GetNodeEmbeddingMeta(id NodeID) (map[string]any, error) {
	return embeddingMetaOf(n.inner, n.PrefixNodeID(id))
}

// embeddingMetaOf is the node's embedding metadata from engine: its
// metadata-only read when it has one, else the full node's.
func embeddingMetaOf(engine Engine, id NodeID) (map[string]any, error) {
	return ReadWithCapability(engine, func(reader NodeEmbeddingMetaReader) (map[string]any, error) { return reader.GetNodeEmbeddingMeta(id) }, func() (map[string]any, error) {
		node, err := engine.GetNode(id)
		if err != nil {
			return nil, err
		}
		return node.EmbedMeta, nil
	})
}
