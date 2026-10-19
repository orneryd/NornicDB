package nornicdb

import "github.com/orneryd/nornicdb/pkg/storage"

// retainedNodeEmbedding owns the provider payload and the business-source
// evidence required to reuse it. Body timestamps, named vectors and storage
// flags belong to the fresh claim and must never be carried by a retry.
type retainedNodeEmbedding struct {
	labels     []string
	properties map[string]any
	vectors    [][]float32
	meta       map[string]any
}

// retainNodeEmbedding transfers this claim's private payload to the retry
// owner. Spans and provider metadata are already prepared in EmbedMeta.
func (ew *EmbedWorker) retainNodeEmbedding(node *storage.Node) {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	if ew.embeddingRetries == nil {
		ew.embeddingRetries = make(map[storage.NodeID]*retainedNodeEmbedding)
	}
	ew.embeddingRetries[node.ID] = &retainedNodeEmbedding{
		labels: node.Labels, properties: node.Properties,
		vectors: node.ChunkEmbeddings, meta: node.EmbedMeta,
	}
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
	source := &storage.Node{Labels: retained.labels, Properties: retained.properties}
	if !policy.SameSource(source, node) {
		return node, false
	}
	defer ew.releaseNodeClaim(node.ID)
	node.ChunkEmbeddings = retained.vectors
	node.EmbedMeta = retained.meta
	ew.writeEmbeddedNode(node)
	return nil, true
}

// claimNodeBatch keeps retained writes in the same bounded pass as provider
// batches. Count attempts, including rejected writes, so a storage outage
// cannot keep this pass alive indefinitely.
func (ew *EmbedWorker) claimNodeBatch(limit int) ([]*storage.Node, bool) {
	nodes := make([]*storage.Node, 0, limit)
	progressed := false
	for attempts := 0; attempts < limit; attempts++ {
		node, advanced := ew.claimNextNodeResult()
		progressed = progressed || advanced
		if node == nil {
			if advanced {
				continue
			}
			break
		}
		nodes = append(nodes, node)
	}
	return nodes, progressed
}
