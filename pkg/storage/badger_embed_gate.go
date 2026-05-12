package storage

// SetEmbeddingsEnabled toggles the pending-embed index. When false, CreateNode
// and UpsertNode skip the pendingEmbedKey write on user nodes — no embed worker
// will consume the marker when embeddings are globally disabled, so the Set is
// pure write amplification. Wired from the server at startup alongside the
// decay flag (see pkg/nornicdb/db.go).
func (b *BadgerEngine) SetEmbeddingsEnabled(enabled bool) {
	b.embeddingsEnabled.Store(enabled)
}

// IsEmbeddingsEnabled reports whether the engine should maintain the pending-
// embed index.
func (b *BadgerEngine) IsEmbeddingsEnabled() bool {
	return b.embeddingsEnabled.Load()
}

// shouldIndexPendingEmbed consolidates the three-way guard that governed the
// pending-embed Set at every node-create/upsert site. The node must be user-
// visible (not a system namespace), not already carry ChunkEmbeddings, and
// actually need an embedding by shape. Gated by the global enable flag so a
// server running with embeddings=off pays zero write amplification for the
// index.
func (b *BadgerEngine) shouldIndexPendingEmbed(node *Node) bool {
	if !b.embeddingsEnabled.Load() {
		return false
	}
	if node == nil {
		return false
	}
	if isSystemNamespaceID(string(node.ID)) {
		return false
	}
	if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
		return false
	}
	return NodeNeedsEmbedding(node)
}
