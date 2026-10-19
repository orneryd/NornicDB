package nornicdb

import (
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// retainedNodeEmbedding owns the provider payload and the business-source
// evidence required to reuse it. Body timestamps, named vectors and storage
// flags belong to the fresh claim and must never be carried by a retry.
type retainedNodeEmbedding struct {
	labels     []string
	properties map[string]any
	vectors    [][]float32
	meta       map[string]any

	identity string
	epoch uint64
	failures int
	retryAt time.Time
}

// retainNodeEmbedding transfers this claim's private payload to the retry
// owner. Spans and provider metadata are already prepared in EmbedMeta.
func (ew *EmbedWorker) retainNodeEmbedding(node *storage.Node, provider embed.Embedder, retained *retainedNodeEmbedding) {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	if retained == nil {
		retained = &retainedNodeEmbedding{
			labels: node.Labels, properties: node.Properties,
			vectors: node.ChunkEmbeddings, meta: node.EmbedMeta,
			identity: ew.nodeEmbeddingIdentity(provider),
			epoch: ew.claimedNodes[string(node.ID)],
		}
	}
	if retained.epoch != ew.embeddingEpoch.Load() {
		return // A provider setter invalidated this in-flight result.
	}
	if retained.failures < 6 {
		retained.failures++
	}
	delay := time.Second << (retained.failures - 1)
	if delay > 30*time.Second {
		delay = 30*time.Second
	}
	retained.retryAt = time.Now().Add(delay)
	if ew.embeddingRetries == nil {
		ew.embeddingRetries = make(map[storage.NodeID]*retainedNodeEmbedding)
	}
	ew.embeddingRetries[node.ID] = retained
}

// Provider objects identify their configuration; model/space and worker text
// policy also participate so a live resolver cannot reuse an obsolete space.
func (ew *EmbedWorker) nodeEmbeddingIdentity(provider embed.Embedder) string {
	if provider == nil {
		return ""
	}
	space := ""
	if p, ok := provider.(embed.EmbeddingSpaceProvider); ok {
		space = p.EmbeddingSpace()
	}
	cfg := ew.config
	return fmt.Sprintf("%s:%s:%d:%s:%d:%d:%v:%v:%t", providerRetryKey(provider), provider.Model(), provider.Dimensions(), space,
		cfg.ChunkSize, cfg.ChunkOverlap, cfg.PropertiesInclude, cfg.PropertiesExclude, cfg.IncludeLabels)
}

func (ew *EmbedWorker) invalidateNodeEmbeddingRetries() {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	for id := range ew.embeddingRetries {
		ew.addNodeToPendingEmbeddings(id)
	}
	ew.embeddingRetries = nil
}

// nextEmbeddingNodeID runs under claimMu. Storage markers for retained IDs are
// consumed while seeking fresh work; the retry owner keeps those IDs and its
// deadline wakes the worker independently of the storage index.
func (ew *EmbedWorker) nextEmbeddingNodeID(now time.Time) storage.NodeID {
	var due storage.NodeID
	var earliest time.Time
	for id, result := range ew.embeddingRetries {
		if _, claimed := ew.claimedNodes[string(id)]; !claimed && !now.Before(result.retryAt) && (due == "" || result.retryAt.Before(earliest)) {
			due, earliest = id, result.retryAt
		}
	}
	if due != "" && ew.retryTurn {
		ew.retryTurn = false
		return due
	}
	for attempts := 0; attempts <= len(ew.embeddingRetries); attempts++ {
		pending := ew.findNodeWithoutEmbedding()
		if pending == nil {
			break
		}
		if _, retained := ew.embeddingRetries[pending.ID]; !retained {
			ew.retryTurn = true
			return pending.ID
		}
		ew.markNodeEmbedded(pending.ID)
	}
	// Engines without a mutable pending index cannot advance past a retained
	// candidate by removing its marker. Use their generic node stream and the
	// same storage eligibility predicate, excluding the retry owner's IDs.
	if _, indexed := ew.storage.(EmbeddingIndexManager); !indexed && len(ew.embeddingRetries) > 0 {
		var fresh storage.NodeID
		_ = ew.storage.StreamNodesWithOptions(ew.ctx, storage.StreamNodesOptions{}, func(node *storage.Node) bool {
			_, retained := ew.embeddingRetries[node.ID]
			_, claimed := ew.claimedNodes[string(node.ID)]
			if !retained && !claimed && storage.NodeNeedsEmbedding(node) {
				fresh = node.ID
				return false
			}
			return true
		})
		if fresh != "" {
			ew.retryTurn = true
			return fresh
		}
	}
	if due != "" {
		ew.retryTurn = false
	}
	return due
}

// Reuse the worker's existing idle wait rather than spawning retry goroutines.
func (ew *EmbedWorker) nodeEmbeddingRetryDelay(delay time.Duration) time.Duration {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	for id, result := range ew.embeddingRetries {
		if _, claimed := ew.claimedNodes[string(id)]; claimed {
			continue
		}
		remaining := time.Until(result.retryAt)
		if remaining < 100*time.Millisecond {
			remaining = 100*time.Millisecond
		}
		if remaining < delay {
			delay = remaining
		}
	}
	return delay
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
	provider, err := ew.resolveEmbedder(node.ID)
	if err != nil || provider == nil || retained.epoch != ew.embeddingEpoch.Load() || retained.identity != ew.nodeEmbeddingIdentity(provider) {
		return node, false // Regenerate through the current provider dispatch.
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
	return nil, ew.writeEmbeddedNode(node, provider, retained)
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
