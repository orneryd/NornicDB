package storage

type embeddingLabelPolicy struct {
	include map[string]struct{}
	exclude map[string]struct{}
}

type embeddingLabelPolicies map[string]embeddingLabelPolicy

// SetEmbeddingLabelPolicy sets the managed-embedding label filter for a database.
// An empty include list allows all labels; exclusion always takes precedence.
func (b *BadgerEngine) SetEmbeddingLabelPolicy(namespace string, include, exclude []string) {
	policy := embeddingLabelPolicy{
		include: make(map[string]struct{}, len(include)),
		exclude: make(map[string]struct{}, len(exclude)),
	}
	for _, label := range include {
		policy.include[label] = struct{}{}
	}
	for _, label := range exclude {
		policy.exclude[label] = struct{}{}
	}
	for {
		old := b.embeddingLabelPolicies.Load()
		updated := make(embeddingLabelPolicies)
		if old != nil {
			for name, existing := range *old {
				updated[name] = existing
			}
		}
		updated[namespace] = policy
		if b.embeddingLabelPolicies.CompareAndSwap(old, &updated) {
			return
		}
	}
}

func (b *BadgerEngine) embeddingLabelsAllowed(node *Node) bool {
	policies := b.embeddingLabelPolicies.Load()
	if policies == nil {
		return true
	}
	policy, ok := (*policies)[namespaceForNodeID(node.ID)]
	if !ok {
		policy, ok = (*policies)[""]
		if !ok {
			return true
		}
	}
	matched := len(policy.include) == 0
	for _, label := range node.Labels {
		if _, excluded := policy.exclude[label]; excluded {
			return false
		}
		if _, included := policy.include[label]; included {
			matched = true
		}
	}
	return matched
}

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
	return NodeNeedsEmbedding(node) && b.embeddingLabelsAllowed(node)
}
