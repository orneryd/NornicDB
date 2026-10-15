package storage

import "maps"

// A lightweight snapshot omits sidecar vectors. Keep metadata consistent with
// the vectors actually exported without mutating the source entity or its map.
func snapshotNodeVectorMetadata(node *Node) *Node {
	if node == nil || !node.EmbeddingsStoredSeparately {
		return node
	}
	copy := *node
	copy.EmbeddingsStoredSeparately = false
	copy.EmbedMeta = maps.Clone(node.EmbedMeta)
	if copy.EmbedMeta != nil {
		if len(copy.ChunkEmbeddings) == 0 {
			delete(copy.EmbedMeta, "has_chunks")
			delete(copy.EmbedMeta, "chunk_count")
		}
		if len(copy.NamedEmbeddings) == 0 && len(copy.ChunkEmbeddings) == 0 {
			delete(copy.EmbedMeta, "has_embedding")
			delete(copy.EmbedMeta, "embedding_dimensions")
			delete(copy.EmbedMeta, "embedded_at")
		}
	}
	return &copy
}
