package nornicdb

import (
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// nodeEmbeddingRetry is the DLQ entry. It retains no node body, properties,
// vectors, provider result or provider object.
// Like the existing MAIN index it has one small record per failed node.
// DLQ deadlines/order are process-local; terminal failure sidecars are durable.
type nodeEmbeddingRetry struct {
	revision storage.EmbeddingBodyRevision
	retryAt  time.Time
	order    uint64
	failures int
}

func (ew *EmbedWorker) dropNodeEmbeddingRetryLocked(id storage.NodeID) {
	delete(ew.nodeRetries, id)
}

func (ew *EmbedWorker) dropNodeEmbeddingRetry(id storage.NodeID) {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	ew.dropNodeEmbeddingRetryLocked(id)
}

func (ew *EmbedWorker) validNodeClaimLocked(id storage.NodeID) bool {
	ew.applyNodeUpdatesLocked()
	attempt, claimed := ew.claimedNodes[string(id)]
	return claimed && attempt.token.ProviderGeneration == ew.embeddingEpoch.Load() && !attempt.invalidated
}

// Enqueue only journals IDs; it never waits for a storage claim to finish.
// The retry owner consumes notifications before selection and result validity.
func (ew *EmbedWorker) embeddingSnapshot(id storage.NodeID) (*storage.Node, error) {
	node, err := ew.storage.GetNode(id)
	if err != nil {
		return nil, err
	}
	return storage.SnapshotNodeForEmbedding(node)
}

// observeNodeRevisionLocked is the sole update/reset decision. Notifications
// only prompt a read; MAIN discovery, fallback scans and claims use the same
// revision comparison. Every due retry requests a new provider result.
func (ew *EmbedWorker) observeNodeRevisionLocked(node *storage.Node) bool {
	changed := false
	if state := ew.nodeRetries[node.ID]; state != nil {
		same, err := state.revision.MatchesNode(node)
		if err == nil && !same {
			delete(ew.nodeRetries, node.ID)
			changed = true
		}
	}
	if attempt := ew.claimedNodes[string(node.ID)]; attempt != nil {
		same, err := attempt.token.BodyRevision.MatchesNode(node)
		if err == nil && !same {
			attempt.invalidated = true
			changed = true
		}
	}
	return changed
}

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
	observed := make(map[storage.NodeID]bool)
	observe := func(id storage.NodeID) {
		if observed[id] || !matches(string(id)) {
			return
		}
		observed[id] = true
		node, err := ew.embeddingSnapshot(id)
		if err != nil || node == nil {
			if nodeGone(node, err) {
				ew.dropNodeEmbeddingRetryLocked(id)
				if attempt := ew.claimedNodes[string(id)]; attempt != nil {
					attempt.invalidated = true
				}
			}
			return
		}
		if ew.observeNodeRevisionLocked(node) {
			if _, claimed := ew.claimedNodes[string(id)]; !claimed {
				ew.addNodeToPendingEmbeddings(id)
			}
		}
	}
	for id := range ew.nodeRetries {
		observe(id)
	}
	for id := range ew.claimedNodes {
		observe(storage.NodeID(id))
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
		state = &nodeEmbeddingRetry{revision: node.EmbeddingRevision()}
		ew.nodeRetries[node.ID] = state
	}
	state.failures++
	limit := ew.config.MaxRetries
	if limit < 0 {
		limit = 0
	}
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
	// Crossing the allowance requests a durable failure marker; it does not
	// retire this retry. Only accepted outcome delivery transfers ownership.
	return state.failures > limit
}

func (ew *EmbedWorker) failNodeEmbedding(node *storage.Node, failure error) bool {
	ew.claimMu.Lock()
	valid := ew.validNodeClaimLocked(node.ID) && ew.ctx.Err() == nil
	park, firstFailure := false, false
	if valid {
		firstFailure = ew.nodeRetries[node.ID] == nil
		park = ew.recordNodeFailureLocked(node, failure)
	}
	ew.claimMu.Unlock()
	if park {
		ew.markNodeEmbeddingFailed(node.ID, failure, node)
	}
	return firstFailure // A repeatedly rejected retry is not progress.
}

func (ew *EmbedWorker) invalidateNodeEmbeddingRetries() {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	ew.clearNodeEmbeddingRetriesLocked()
}

func (ew *EmbedWorker) clearNodeEmbeddingRetriesLocked() {
	for id := range ew.nodeRetries {
		if _, claimed := ew.claimedNodes[string(id)]; claimed {
			continue
		} // release owns its requeue
		ew.addNodeToPendingEmbeddings(id)
	}
	ew.nodeRetries = nil
}

// nextEmbeddingNodeID runs under claimMu. Storage markers for DLQ IDs are
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
			body, err := ew.embeddingSnapshot(pending.ID)
			if err == nil && body != nil && ew.observeNodeRevisionLocked(body) {
				// New body attempt; the next attempt requests a new provider result.
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
	// Engines without a mutable pending index cannot advance past a DLQ
	// candidate by removing its marker. Use their generic node stream and the
	// same storage eligibility predicate, excluding the retry owner's IDs.
	if _, indexed := ew.storage.(EmbeddingIndexManager); (blockedMain || !indexed) && len(ew.nodeRetries)+len(ew.claimedNodes) > 0 {
		var fresh storage.NodeID
		_ = ew.storage.StreamNodesWithOptions(ew.ctx, storage.StreamNodesOptions{WithEmbeddings: true}, func(node *storage.Node) error {
			state := ew.nodeRetries[node.ID]
			_, claimed := ew.claimedNodes[string(node.ID)]
			if state != nil {
				snapshot, err := ew.embeddingSnapshot(node.ID)
				if err == nil && snapshot != nil && ew.observeNodeRevisionLocked(snapshot) {
					state = nil
				}
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
		if !claimed && !now.Before(state.retryAt) && (due == "" || state.order < order) {
			due, order = id, state.order
		}
	}
	return due
}

// Reuse the worker's existing idle wait rather than spawning retry goroutines.
func (ew *EmbedWorker) nodeEmbeddingRetryDelay(delay time.Duration) time.Duration {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	ew.applyNodeUpdatesLocked()
	for id, state := range ew.nodeRetries {
		if _, claimed := ew.claimedNodes[string(id)]; claimed {
			continue
		}
		remaining := time.Until(state.retryAt)
		if remaining < 100*time.Millisecond {
			remaining = 100 * time.Millisecond
		}
		if remaining < delay {
			delay = remaining
		}
	}
	return delay
}

// Every claim goes through the ordinary provider dispatch. No rejected-save
// payload survives into a retry; the DLQ owns only revision and scheduling.
func (ew *EmbedWorker) claimNextNodeResult(selections ...embeddingSelection) (*storage.Node, bool) {
	return ew.claimNextNodeResultFrom(false, ew.embeddingSelection(selections))
}

// claimNodeBatch finishes a MAIN batch before requesting any DLQ result.
// Every returned node has a fresh claim and needs a provider request.
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
