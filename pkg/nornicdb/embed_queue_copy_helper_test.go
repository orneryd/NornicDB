package nornicdb

import "github.com/orneryd/nornicdb/pkg/storage"

// copyNodeForEmbedding is the embedding worker's former deep-copying
// projection, kept for tests that need an independent snapshot of a node
// (the worker itself now takes embeddingWorkView of the node it owns, NOR-160).
func copyNodeForEmbedding(src *storage.Node) *storage.Node {
	if src == nil {
		return nil
	}

	// Create a new node with copied scalar fields
	dst := &storage.Node{
		ID:        src.ID,
		Labels:    make([]string, len(src.Labels)),
		CreatedAt: src.CreatedAt,
		UpdatedAt: src.UpdatedAt,
	}

	dst.CopyEmbeddingIdentityFrom(src)

	// Copy labels
	copy(dst.Labels, src.Labels)

	// Copy chunk embeddings if present (always stored in ChunkEmbeddings, even single chunk = array of 1)
	if len(src.ChunkEmbeddings) > 0 {
		dst.ChunkEmbeddings = make([][]float32, len(src.ChunkEmbeddings))
		for i, emb := range src.ChunkEmbeddings {
			dst.ChunkEmbeddings[i] = make([]float32, len(emb))
			copy(dst.ChunkEmbeddings[i], emb)
		}
	}

	// Deep copy Properties map - this is the critical part to avoid race condition
	if src.Properties != nil {
		dst.Properties = make(map[string]any, len(src.Properties))
		for k, v := range src.Properties {
			dst.Properties[k] = v // Shallow copy of values is OK for our use case
		}
	}

	return dst
}
