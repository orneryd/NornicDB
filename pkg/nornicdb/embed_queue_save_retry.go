package nornicdb

import (
	"fmt"
	"reflect"
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
	epoch    uint64
	failures int
	retryAt  time.Time
	bytes    int64
}

const nodeEmbeddingRetryMaxCount = 128
const nodeEmbeddingRetryMaxBytes int64 = 64 << 20

// Bounds include claimed retry payloads. Fresh claims reserve count slots;
// provider outputs already in flight may not fit and are then left pending
// without retaining their payload. Their bounded batch memory is not a cache.
func (ew *EmbedWorker) nodeEmbeddingRetryBudget() (int, int64) {
	count, bytes := ew.retryMaxCount, ew.retryMaxBytes
	if count <= 0 {
		count = nodeEmbeddingRetryMaxCount
	}
	if bytes <= 0 {
		bytes = nodeEmbeddingRetryMaxBytes
	}
	return count, bytes
}

func (ew *EmbedWorker) nodeEmbeddingRetryBytes() int64 {
	var bytes int64
	for _, result := range ew.embeddingRetries {
		bytes += result.bytes
	}
	return bytes
}

// A conservative, bounded walk also charges typed slices, nested provider
// metadata and spare slice capacity. Unknown pointer graphs are not retained.
// Accounting is an admission budget, not a Go heap measurement.
func nodeEmbeddingRetryValueBytes(v reflect.Value, budget int64, depth int) int64 {
	if budget < 0 {
		return 1
	}
	if depth > 32 {
		return budget + 1
	}
	if !v.IsValid() {
		return 0
	}
	bytes := int64(32)
	add := func(value reflect.Value) { bytes += nodeEmbeddingRetryValueBytes(value, budget-bytes, depth+1) }
	switch v.Kind() {
	case reflect.Interface:
		if !v.IsNil() {
			add(v.Elem())
		}
	case reflect.String:
		bytes += int64(v.Len())
	case reflect.Map:
		bytes += int64(v.Len()) * 128
		if bytes <= budget {
			it := v.MapRange()
			for it.Next() {
				add(it.Key())
				add(it.Value())
				if bytes > budget {
					break
				}
			}
		}
	case reflect.Slice, reflect.Array:
		n := v.Len()
		if v.Kind() == reflect.Slice {
			n = v.Cap()
		}
		bytes += int64(n) * int64(v.Type().Elem().Size())
		if bytes <= budget {
			switch v.Type().Elem().Kind() {
			case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			default:
				for i := 0; i < v.Len(); i++ {
					add(v.Index(i))
					if bytes > budget {
						break
					}
				}
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			add(v.Field(i))
			if bytes > budget {
				break
			}
		}
	case reflect.Pointer, reflect.UnsafePointer, reflect.Func, reflect.Chan:
		if !v.IsNil() {
			return budget + 1
		}
	}
	if bytes > budget {
		return budget + 1
	}
	return bytes
}

func (ew *EmbedWorker) dropNodeEmbeddingRetryLocked(id storage.NodeID) {
	if _, ok := ew.embeddingRetries[id]; ok {
		delete(ew.embeddingRetries, id)
		ew.retryBudgetBlocked = false
	}
}

func (ew *EmbedWorker) dropNodeEmbeddingRetry(id storage.NodeID) {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	ew.dropNodeEmbeddingRetryLocked(id)
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
			epoch:    ew.claimedNodes[string(node.ID)],
		}
	}
	if retained.epoch != ew.embeddingEpoch.Load() {
		return // A provider setter invalidated this in-flight result.
	}
	if retained.bytes == 0 {
		countLimit, byteLimit := ew.nodeEmbeddingRetryBudget()
		retained.bytes = 512 + int64(len(node.ID)+len(retained.identity))
		for _, value := range []any{retained.labels, retained.properties, retained.vectors, retained.meta} {
			retained.bytes += nodeEmbeddingRetryValueBytes(reflect.ValueOf(value), byteLimit-retained.bytes, 0)
			if retained.bytes > byteLimit {
				break
			}
		}
		if len(ew.embeddingRetries) >= countLimit || retained.bytes > byteLimit-ew.nodeEmbeddingRetryBytes() {
			// A batch/another worker may already have bought this result before
			// the budget filled. Admission stays blocked until retained capacity
			// is released or the owner is invalidated.
			ew.retryBudgetBlocked = true
			return
		}
	}
	if retained.failures < 6 {
		retained.failures++
	}
	delay := time.Second << (retained.failures - 1)
	if delay > 30*time.Second {
		delay = 30 * time.Second
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
	return fmt.Sprintf("%s:%q:%d:%q:%d:%d:%q:%q:%t", providerRetryKey(provider), provider.Model(), provider.Dimensions(), space,
		cfg.ChunkSize, cfg.ChunkOverlap, cfg.PropertiesInclude, cfg.PropertiesExclude, cfg.IncludeLabels)
}

func (ew *EmbedWorker) invalidateNodeEmbeddingRetries() {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	for id := range ew.embeddingRetries {
		ew.addNodeToPendingEmbeddings(id)
	}
	ew.embeddingRetries = nil
	ew.retryBudgetBlocked = false
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
	countLimit, byteLimit := ew.nodeEmbeddingRetryBudget()
	count := len(ew.embeddingRetries)
	for id := range ew.claimedNodes {
		if _, retained := ew.embeddingRetries[storage.NodeID(id)]; !retained {
			count++
		}
	}
	if count >= countLimit || ew.nodeEmbeddingRetryBytes() >= byteLimit || ew.retryBudgetBlocked {
		return due // Saving retained work needs no new provider result.
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
		_ = ew.storage.StreamNodesWithOptions(ew.ctx, storage.StreamNodesOptions{}, func(node *storage.Node) error {
			_, retained := ew.embeddingRetries[node.ID]
			_, claimed := ew.claimedNodes[string(node.ID)]
			if !retained && !claimed && storage.NodeNeedsEmbedding(node) {
				fresh = node.ID
				return storage.ErrIterationStopped
			}
			return nil
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
			remaining = 100 * time.Millisecond
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
	// Rejected retained writes are not progress, but they are not an empty
	// queue either. Continue selection within a bounded pass so healthy work
	// reaches every dispatch route without waiting for a new wakeup.
	for attempts := 0; attempts <= nodeEmbeddingRetryMaxCount; attempts++ {
		node, progressed, attempted := ew.tryNextNodeResult()
		if node != nil || progressed || !attempted {
			return node, progressed
		}
	}
	return nil, false
}

func (ew *EmbedWorker) tryNextNodeResult() (*storage.Node, bool, bool) {
	node, retained, progressed := ew.claimNextNodeSource()
	if node == nil {
		return nil, progressed, false
	}
	if retained == nil {
		return node, false, false
	}
	provider, err := ew.resolveEmbedder(node.ID)
	if err != nil || provider == nil || retained.epoch != ew.embeddingEpoch.Load() || retained.identity != ew.nodeEmbeddingIdentity(provider) {
		ew.dropNodeEmbeddingRetry(node.ID)
		return node, false, false // Regenerate through the current provider dispatch.
	}
	// Text comparison intentionally omits bookkeeping properties. Structured
	// providers receive the entire property map unchanged, including id and
	// timestamp properties, so their retry must also compare that exact input.
	// Capability forwarding through cached/traced providers follows this check.
	policy := storage.EmbeddingTextPolicy{IncludeLabels: true}
	source := &storage.Node{Labels: retained.labels, Properties: retained.properties}
	structured, usesProperties := provider.(embed.DocumentPropertyChunkEmbedder)
	if !policy.SameSource(source, node) || (usesProperties && structured.UsesDocumentProperties() && !reflect.DeepEqual(retained.properties, node.Properties)) {
		ew.dropNodeEmbeddingRetry(node.ID)
		return node, false, false
	}
	defer ew.releaseNodeClaim(node.ID)
	node.ChunkEmbeddings = retained.vectors
	node.EmbedMeta = retained.meta
	return nil, ew.writeEmbeddedNode(node, provider, retained), true
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
