package storage

import "errors"

// GetNodeEmbeddingMeta reads a node's embedding metadata through the
// wrapped engine. A deleted pending node is gone; a pending one answers with
// the metadata it carries, else with the stored node's (nothing when it
// isn't stored yet).
func (ae *AsyncEngine) GetNodeEmbeddingMeta(id NodeID) (map[string]any, error) {
	ae.mu.RLock()
	if ae.deleteNodes[id] {
		ae.mu.RUnlock()
		return nil, ErrNotFound
	}
	node, pending := ae.nodeCache[id]
	var meta map[string]any
	if pending && len(node.EmbedMeta) > 0 {
		meta = make(map[string]any, len(node.EmbedMeta))
		for key, value := range node.EmbedMeta {
			meta[key] = value
		}
	}
	ae.mu.RUnlock()
	if meta != nil {
		return meta, nil
	}
	meta, err := embeddingMetaOf(ae.engine, id)
	if pending && errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return meta, err
}

// GetNodeEmbeddingMeta delegates to the wrapped engine.
func (w *WALEngine) GetNodeEmbeddingMeta(id NodeID) (map[string]any, error) {
	return embeddingMetaOf(w.engine, id)
}

// GetNodeEmbeddingMeta reads the metadata of the node's namespaced ID.
func (n *NamespacedEngine) GetNodeEmbeddingMeta(id NodeID) (map[string]any, error) {
	return embeddingMetaOf(n.inner, n.prefixNodeID(id))
}

// embeddingMetaOf is the node's embedding metadata from engine: its
// metadata-only read when it has one, else the full node's.
func embeddingMetaOf(engine Engine, id NodeID) (map[string]any, error) {
	if reader, ok := engine.(NodeEmbeddingMetaReader); ok {
		return reader.GetNodeEmbeddingMeta(id)
	}
	node, err := engine.GetNode(id)
	if err != nil {
		return nil, err
	}
	return node.EmbedMeta, nil
}
