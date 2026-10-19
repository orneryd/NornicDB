package nornicdb

import (
	"fmt"
	"reflect"
	"strings"
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
	bytes    int64
}

// nodeEmbeddingRetry is the DLQ entry, independent of the optional payload
// cache. It retains no node body, properties, vectors or provider object.
// Like the existing MAIN index it has one small record per failed node.
// DLQ deadlines/order are process-local; terminal failure sidecars are durable.
type nodeEmbeddingRetry struct {
	updatedAt time.Time
	retryAt   time.Time
	order     uint64
	failures  int
	terminal  bool
}

const nodeEmbeddingRetryMaxCount = 128
const nodeEmbeddingRetryMaxBytes int64 = 64 << 20

// Bounds include claimed retry payloads. They bound the optional payload cache,
// never admission to MAIN. An uncached DLQ entry regenerates only on its turn.
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
	// time.Time is a supported immutable stored value. Its location is shared
	// library configuration, not an arbitrary application-owned pointer graph.
	// Charge the value and zone name without traversing time.Location internals.
	if v.Type() == reflect.TypeOf(time.Time{}) && v.CanInterface() {
		stamp := v.Interface().(time.Time)
		return int64(v.Type().Size()) + 64 + int64(len(stamp.Location().String()))
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
	delete(ew.embeddingRetries, id)
	delete(ew.nodeRetries, id)
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
	if !ew.validNodeClaimLocked(node.ID) {
		ew.claimMu.Unlock()
		return
	}
	if retained == nil {
		retained = &retainedNodeEmbedding{
			labels: node.Labels, properties: node.Properties,
			vectors: node.ChunkEmbeddings, meta: node.EmbedMeta,
			identity: ew.nodeEmbeddingIdentity(provider),
			epoch:    ew.claimedNodes[string(node.ID)].token.ProviderGeneration,
		}
	}
	if retained.epoch != ew.embeddingEpoch.Load() {
		ew.claimMu.Unlock()
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
			retained = nil // Queue the ID without retaining an oversized payload.
		}
	}
	if retained != nil && ew.embeddingRetries == nil {
		ew.embeddingRetries = make(map[storage.NodeID]*retainedNodeEmbedding)
	}
	if retained != nil {
		ew.embeddingRetries[node.ID] = retained
	}
	terminal := ew.recordNodeFailureLocked(node, nil)
	ew.claimMu.Unlock()
	if terminal {
		ew.markNodeEmbeddingFailed(node.ID, fmt.Errorf("embedding save retry limit reached"), node)
	}
}

func (ew *EmbedWorker) validNodeClaimLocked(id storage.NodeID) bool {
	ew.applyNodeUpdatesLocked()
	attempt, claimed := ew.claimedNodes[string(id)]
	return claimed && attempt.token.ProviderGeneration == ew.embeddingEpoch.Load() && !attempt.invalidated
}

// Enqueue only journals IDs; it never waits for a storage claim to finish.
// The retry owner consumes notifications before selection and result validity.
func (ew *EmbedWorker) applyNodeUpdatesLocked() {
	if !ew.nodeUpdatesPending.Load() {
		return
	}
	ew.mu.Lock()
	updates := ew.nodeUpdates
	ew.nodeUpdates = nil
	ew.nodeUpdatesPending.Store(false)
	ew.mu.Unlock()
	matches := func(id string) bool {
		for {
			if _, ok := updates[id]; ok {
				return true
			}
			_, suffix, ok := strings.Cut(id, ":")
			if !ok {
				return false
			}
			id = suffix
		}
	}
	for id := range ew.embeddingRetries {
		if !matches(string(id)) {
			continue
		}
		if _, tracked := ew.nodeRetries[id]; !tracked {
			if _, claimed := ew.claimedNodes[string(id)]; !claimed {
				ew.addNodeToPendingEmbeddings(id)
			}
		}
	}
	for id := range ew.nodeRetries {
		if !matches(string(id)) {
			continue
		}
		delete(ew.nodeRetries, id) // Reset attempts/deadline; validate payload source on its MAIN claim.
		if _, claimed := ew.claimedNodes[string(id)]; !claimed {
			ew.addNodeToPendingEmbeddings(id)
		}
	}
	for id, attempt := range ew.claimedNodes {
		if matches(id) {
			attempt.invalidated = true
		}
	}
}

// All six provider dispatch routes and rejected saves converge on this owner.
// MaxRetries counts DLQ retries after the initial attempt, never inline calls.
func (ew *EmbedWorker) recordNodeFailureLocked(node *storage.Node, failure error) bool {
	ew.failed.Add(1)
	if ew.nodeRetries == nil {
		ew.nodeRetries = make(map[storage.NodeID]*nodeEmbeddingRetry)
	}
	state := ew.nodeRetries[node.ID]
	if state == nil {
		state = &nodeEmbeddingRetry{updatedAt: node.UpdatedAt}
		ew.nodeRetries[node.ID] = state
	}
	state.failures++
	limit := ew.config.MaxRetries
	if limit < 0 {
		limit = 0
	}
	state.terminal = state.failures > limit
	ew.retrySequence++
	state.order = ew.retrySequence // Every failure rejoins the DLQ tail.
	base, cap := ew.config.ProviderRetryBackoff, ew.config.ProviderRetryBackoffMax
	if base <= 0 {
		base = time.Second
	}
	if cap < base {
		cap = 30 * time.Second
		if cap < base {
			cap = base
		}
	}
	delay := base
	for i := 1; i < state.failures && delay < cap; i++ {
		if delay > cap/2 {
			delay = cap
		} else {
			delay *= 2
		}
	}
	if requested, ok := retryDelay(failure); ok && requested > delay {
		delay = requested
	}
	state.retryAt = time.Now().Add(delay)
	if state.terminal {
		delete(ew.embeddingRetries, node.ID)
	}
	return state.terminal
}

func (ew *EmbedWorker) failNodeEmbedding(node *storage.Node, failure error) {
	ew.claimMu.Lock()
	valid := ew.validNodeClaimLocked(node.ID) && ew.ctx.Err() == nil
	terminal := false
	if valid {
		terminal = ew.recordNodeFailureLocked(node, failure)
	}
	ew.claimMu.Unlock()
	if terminal {
		ew.markNodeEmbeddingFailed(node.ID, failure, node)
	}
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
	ew.clearNodeEmbeddingRetriesLocked()
}

func (ew *EmbedWorker) clearNodeEmbeddingRetriesLocked() {
	for id := range ew.embeddingRetries {
		if _, tracked := ew.nodeRetries[id]; tracked {
			continue
		}
		if _, claimed := ew.claimedNodes[string(id)]; !claimed {
			ew.addNodeToPendingEmbeddings(id)
		}
	}
	for id, state := range ew.nodeRetries {
		if state.terminal {
			continue
		}
		if _, claimed := ew.claimedNodes[string(id)]; claimed {
			continue
		} // release owns its requeue
		ew.addNodeToPendingEmbeddings(id)
	}
	ew.embeddingRetries = nil
	ew.nodeRetries = nil
}

// nextEmbeddingNodeID runs under claimMu. Storage markers for retained IDs are
// consumed while seeking fresh work; the retry owner keeps those IDs and its
// deadline wakes the worker independently of the storage index.
func (ew *EmbedWorker) nextEmbeddingNodeID(now time.Time, mainOnly bool) storage.NodeID {
	// MAIN is the storage pending index. A body update restores a DLQ ID to
	// that index; detect it even when the update did not call Enqueue.
	blockedMain := false
	var skipped storage.NodeID
	for attempts, bound := 0, len(ew.nodeRetries)+len(ew.claimedNodes)+1; attempts < bound; attempts++ {
		pending := ew.findNodeWithoutEmbedding()
		if pending == nil {
			break
		}
		if pending.ID == skipped {
			blockedMain = true
			break
		} // Index deletion did not advance.
		if state := ew.nodeRetries[pending.ID]; state != nil {
			// Finder projections need only identify the node. Retry metadata
			// compares the authoritative body, never a partial index projection.
			body, err := ew.storage.GetNode(pending.ID)
			if err == nil && body != nil && !state.updatedAt.Equal(body.UpdatedAt) {
				delete(ew.nodeRetries, pending.ID) // New attempt; payload validity still uses consumed source.
			} else {
				blockedMain = true
				skipped = pending.ID
				ew.markNodeEmbedded(pending.ID)
				continue
			}
		}
		if _, claimed := ew.claimedNodes[string(pending.ID)]; !claimed {
			return pending.ID
		}
		blockedMain = true
		skipped = pending.ID
		ew.markNodeEmbedded(pending.ID)
	}
	// Engines without a mutable pending index cannot advance past a retained
	// candidate by removing its marker. Use their generic node stream and the
	// same storage eligibility predicate, excluding the retry owner's IDs.
	if _, indexed := ew.storage.(EmbeddingIndexManager); (blockedMain || !indexed) && len(ew.nodeRetries)+len(ew.claimedNodes) > 0 {
		var fresh storage.NodeID
		_ = ew.storage.StreamNodesWithOptions(ew.ctx, storage.StreamNodesOptions{WithEmbeddings: true}, func(node *storage.Node) error {
			state := ew.nodeRetries[node.ID]
			_, claimed := ew.claimedNodes[string(node.ID)]
			if state != nil && !state.updatedAt.Equal(node.UpdatedAt) {
				delete(ew.nodeRetries, node.ID)
				state = nil
			}
			if state == nil && !claimed && storage.NodeNeedsEmbedding(node) {
				fresh = node.ID
				return storage.ErrIterationStopped
			}
			return nil
		})
		if fresh != "" {
			return fresh
		}
	}
	if mainOnly {
		return ""
	}
	var due storage.NodeID
	var order uint64
	for id, state := range ew.nodeRetries {
		_, claimed := ew.claimedNodes[string(id)]
		if !claimed && !state.terminal && !now.Before(state.retryAt) && (due == "" || state.order < order) {
			due, order = id, state.order
		}
	}
	return due
}

// Reuse the worker's existing idle wait rather than spawning retry goroutines.
func (ew *EmbedWorker) nodeEmbeddingRetryDelay(delay time.Duration) time.Duration {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	for id, result := range ew.nodeRetries {
		if result.terminal {
			continue
		}
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
func (ew *EmbedWorker) claimNextNodeResult(selections ...embeddingSelection) (*storage.Node, bool) {
	return ew.claimNextNodeResultFrom(false, ew.embeddingSelection(selections))
}

func (ew *EmbedWorker) claimNextNodeResultFrom(mainOnly bool, selections ...embeddingSelection) (*storage.Node, bool) {
	// Rejected retained writes are not progress, but they are not an empty
	// queue either. Continue selection within a bounded pass so healthy work
	// reaches every dispatch route without waiting for a new wakeup.
	for attempts := 0; attempts <= nodeEmbeddingRetryMaxCount; attempts++ {
		node, progressed, attempted := ew.tryNextNodeResult(mainOnly, ew.embeddingSelection(selections))
		if node != nil || progressed || !attempted {
			return node, progressed
		}
	}
	return nil, false
}

func (ew *EmbedWorker) tryNextNodeResult(mainOnly bool, selection embeddingSelection) (*storage.Node, bool, bool) {
	node, retained, progressed := ew.claimNextNodeSourceFrom(mainOnly, selection)
	if node == nil {
		return nil, progressed, false
	}
	if retained == nil {
		return node, false, false
	}
	provider, err := ew.resolveAttemptEmbedder(node)
	if err != nil || provider == nil || retained.epoch != ew.embeddingEpoch.Load() || retained.identity != ew.nodeEmbeddingIdentity(provider) {
		ew.dropNodeEmbeddingRetry(node.ID)
		return node, false, false // Regenerate through the current provider dispatch.
	}
	// Retry, final storage acceptance and later body updates use the same
	// consumed-source record produced with the result.
	source := &storage.Node{Labels: retained.labels, Properties: retained.properties, EmbedMeta: retained.meta}
	if !storage.EmbeddingSourceMatches(source, node, storage.EmbeddingTextPolicy{IncludeLabels: true}) {
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
func (ew *EmbedWorker) claimNodeBatch(limit int, selections ...embeddingSelection) ([]*storage.Node, bool) {
	nodes := make([]*storage.Node, 0, limit)
	progressed := false
	for attempts := 0; attempts < limit; attempts++ {
		// Finish the MAIN batch before selecting or saving any DLQ entry.
		node, advanced := ew.claimNextNodeResultFrom(len(nodes) > 0, ew.embeddingSelection(selections))
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
