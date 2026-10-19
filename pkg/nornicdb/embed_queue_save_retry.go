package nornicdb

import "github.com/orneryd/nornicdb/pkg/storage"

// retainNodeEmbedding transfers this claim's private result to the retry owner.
// Keep the prepared sidecar, including vectors, spans and provider metadata,
// rather than reconstructing it or buying the provider result again.
func (ew *EmbedWorker) retainNodeEmbedding(node *storage.Node) {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	if ew.embeddingRetries == nil {
		ew.embeddingRetries = make(map[storage.NodeID]*storage.Node)
	}
	ew.embeddingRetries[node.ID] = node
}

// claimNextNodeResult reports progress for a deleted candidate or a retained
// write attempt. All provider paths enter here, so none can re-embed a retained
// result before its source has been checked.
func (ew *EmbedWorker) claimNextNodeResult() (*storage.Node, bool) {
	node, retained, progressed := ew.claimNextNodeSource()
	if node == nil {
		return nil, progressed
	}
	if retained == nil {
		return node, false
	}
	// Structured providers can consume properties excluded from built text.
	// Compare the full business source with storage's canonical value comparison;
	// matching text alone would reuse stale structured inputs or chunk spans.
	policy := storage.EmbeddingTextPolicy{IncludeLabels: true}
	if !policy.SameSource(retained, node) {
		return node, false
	}
	defer ew.releaseNodeClaim(node.ID)
	ew.writeEmbeddedNode(retained)
	return nil, true
}
