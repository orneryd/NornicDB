package nornicdb

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"time"
)

// A selection binds provider configuration before capability queries or claims.
// Body admission comes from MAIN/DLQ; an explicit operation can instead carry
// its admission here. Provider selection never relabels already-admitted work.
type embeddingSelection struct {
	vectorWorkOrigin  uint64
	vectorOriginBound bool
	provider          embed.Embedder
	resolver          func(storage.NodeID) (embed.Embedder, error)
	generation        uint64
}

type recentNodeEmbedding struct {
	at       time.Time
	revision storage.EmbeddingBodyRevision
}

type nodeEmbeddingAttempt struct {
	selection   embeddingSelection
	token       *storage.EmbeddingAttempt
	invalidated bool
}

func (ew *EmbedWorker) selectEmbeddingProvider() embeddingSelection {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	return embeddingSelection{provider: ew.embedder, resolver: ew.embedderResolver, generation: ew.embeddingEpoch.Load()}
}

func (ew *EmbedWorker) embeddingSelection(selections []embeddingSelection) embeddingSelection {
	if len(selections) != 0 {
		return selections[0]
	}
	return ew.selectEmbeddingProvider()
}

func (ew *EmbedWorker) resolveAttemptEmbedder(node *storage.Node) (embed.Embedder, error) {
	ew.claimMu.Lock()
	attempt := ew.claimedNodes[string(node.ID)]
	ew.claimMu.Unlock()
	if attempt == nil {
		return nil, storage.ErrEmbeddingSourceChanged
	}
	return attempt.selection.resolve(node.ID)
}

// bindNodeAttemptLocked is the sole writer of claim identity. The token is
// private to this body snapshot and is preserved by all storage decorators.
func (ew *EmbedWorker) bindNodeAttemptLocked(node *storage.Node, selection embeddingSelection) {
	if !selection.vectorOriginBound {
		if retry := ew.nodeRetries[node.ID]; retry != nil {
			selection.vectorWorkOrigin = retry.vectorWorkOrigin
		} else if origin, bound := node.EmbeddingWorkOrigin(); bound {
			selection.vectorWorkOrigin = origin
		} else {
			// A claimed body whose pending marker is gone carries no
			// admission: the attempt is admitted here, and counted.
			selection.vectorWorkOrigin = storage.AdmitUnrequestedVectorWork("nornicdb.embedding-attempt")
		}
	}
	if ew.claimedNodes == nil {
		ew.claimedNodes = make(map[string]*nodeEmbeddingAttempt)
	}

	ew.inFlight.Add(1)
	ew.claimSequence++
	attempt := &nodeEmbeddingAttempt{selection: selection}
	attempt.token = storage.NewEmbeddingAttempt(selection.vectorWorkOrigin, selection.generation, ew.claimSequence, node.EmbeddingRevision(), func() (func(), bool) {
		ew.claimMu.Lock()
		if ew.claimedNodes[string(node.ID)] != attempt || !ew.validNodeClaimLocked(node.ID) || ew.ctx.Err() != nil {
			ew.claimMu.Unlock()
			return nil, false
		}
		return ew.claimMu.Unlock, true
	}, func(authoritative *storage.Node) {
		// Keep DLQ ownership, deadlines and drain accounting until storage
		// accepts and delivers the outcome, including a terminal marker.
		ew.dropNodeEmbeddingRetry(node.ID)
		if failed, _ := authoritative.EmbedMeta["embedding_failed"].(bool); failed {
			ew.parked.Add(1)
			return
		}
		if len(authoritative.ChunkEmbeddings) == 0 {
			return
		} // explicit reset
		if authoritative.ID != node.ID {
			copy := *authoritative
			copy.ID = node.ID
			authoritative = &copy
		}
		if ew.onEmbedded != nil {
			ew.onEmbedded(authoritative)
		}
		ew.processed.Add(1)
		ew.markRecentlyProcessed(authoritative)
	})
	ew.claimedNodes[string(node.ID)] = attempt
	node.SetEmbeddingAttempt(attempt.token)
}

// commitNodeOutcome is the ONLY worker storage-write route for successful
// vectors, terminal failures and administrative resets. Storage validates the
// attempt at final acceptance and owns both notifications and worker callbacks.
func (ew *EmbedWorker) commitNodeOutcome(node *storage.Node) error {
	if node == nil || node.EmbeddingAttempt() == nil {
		return storage.ErrEmbeddingSourceChanged
	}
	ew.providerWriteMu.RLock()
	defer ew.providerWriteMu.RUnlock()
	if !node.EmbeddingAttempt().IsCurrent() {
		return storage.ErrEmbeddingSourceChanged
	}
	sidecar, ok := ew.storage.(storage.EmbeddingSidecarUpdater)
	if !ok {
		return fmt.Errorf("storage engine %T does not support embedding sidecar writes", ew.storage)
	}
	return sidecar.UpdateNodeEmbeddingSidecar(node)
}

// PendingEmbeddingsCount is the canonical drain view: MAIN union active claims
// union every nonterminal DLQ entry. The durable owner excludes IDs already
// counted here, including updates that have put an active/DLQ ID back in MAIN.
func (ew *EmbedWorker) PendingEmbeddingsCount() int {
	if ew == nil || ew.storage == nil {
		return 0
	}
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	ew.applyNodeUpdatesLocked()
	count := len(ew.claimedNodes)
	for id := range ew.nodeRetries {
		if _, claimed := ew.claimedNodes[string(id)]; !claimed {
			count++
		}
	}
	var accept func([]byte) bool
	if len(ew.claimedNodes)+len(ew.nodeRetries) > 0 {
		accept = func(id []byte) bool {
			_, claimed := ew.claimedNodes[string(id)]
			_, queued := ew.nodeRetries[storage.NodeID(id)]
			return !claimed && !queued
		}
	}
	count += storage.CountPendingEmbeddingIDs(ew.storage, accept)
	return count
}

func (ew *EmbedWorker) commitNodeReset(node *storage.Node, origin uint64) error {
	selection := ew.selectEmbeddingProvider()
	selection.vectorWorkOrigin, selection.vectorOriginBound = origin, true
	ew.claimMu.Lock()
	if _, claimed := ew.claimedNodes[string(node.ID)]; claimed {
		ew.claimMu.Unlock()
		return storage.ErrEmbeddingSourceChanged
	}
	ew.bindNodeAttemptLocked(node, selection)
	ew.claimMu.Unlock()
	defer ew.releaseNodeClaim(node.ID)
	return ew.commitNodeOutcome(node)
}
func (selection embeddingSelection) resolve(id storage.NodeID) (embed.Embedder, error) {
	if selection.resolver != nil {
		provider, err := selection.resolver(id)
		if err != nil || provider != nil {
			return provider, err
		}
	}
	return selection.provider, nil
}

// Rejected outcomes invalidate their attempt; only release transfers its ID
// back to MAIN. No publisher may independently enqueue a second copy.
func (ew *EmbedWorker) rejectNodeOutcome(id storage.NodeID) {
	ew.claimMu.Lock()
	defer ew.claimMu.Unlock()
	if attempt := ew.claimedNodes[string(id)]; attempt != nil {
		attempt.invalidated = true
	}
	ew.dropNodeEmbeddingRetryLocked(id)
}
