package storage

import "maps"

// A lightweight snapshot omits sidecar vectors. Keep metadata consistent with
// the vectors actually exported without mutating the source entity or its map.
func snapshotNodeVectorMetadata(node *Node) *Node {
	if node == nil || !node.RuntimeMetadata().EmbeddingsStoredSeparately {
		return node
	}
	copy := *node
	{
		metadata := copy.RuntimeMetadata()
		metadata.EmbeddingsStoredSeparately = false
		copy.SetRuntimeMetadata(metadata)
	}
	copy.EmbedMeta = maps.Clone(node.EmbedMeta)
	if copy.EmbedMeta != nil {
		if len(copy.ChunkEmbeddings) == 0 {
			delete(copy.EmbedMeta, "has_chunks")
			delete(copy.EmbedMeta, "chunk_count")
		}
		if !snapshotHasNamedVector(copy.NamedEmbeddings) && len(copy.ChunkEmbeddings) == 0 {
			delete(copy.EmbedMeta, "has_embedding")
			delete(copy.EmbedMeta, "embedding_dimensions")
			delete(copy.EmbedMeta, "embedded_at")
		}
	}
	return &copy
}

func snapshotHasNamedVector(vectors map[string][]float32) bool {
	for _, vector := range vectors {
		if len(vector) > 0 {
			return true
		}
	}
	return false
}
