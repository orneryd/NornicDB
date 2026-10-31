package storage

// embeddingPublicationProof certifies a conflict-checked body at the actual
// sidecar commit, not at a later sampling of the current publication timestamp.
// It is private, process-local outcome state, never a second revision registry.
// The capture owner must still select the acknowledged overlay before using it.
type embeddingPublicationProof struct {
	engine   *BadgerEngine
	oracle   *commitOracle
	id       NodeID
	revision EmbeddingBodyRevision
	commitTs uint64
}

// commitOutcome preserves ordinary commit publication and carries its own
// assigned timestamp out of that boundary. A later publication cannot be
// mistaken for this commit, even when a commit-tail hook or listener reenters.
func (cw *commitWriter) commitOutcome() error {
	if node := cw.embeddingOutcome; node != nil {
		state := node.runtimeState()
		state.embeddingPublication = embeddingPublicationProof{}
		node.setRuntimeState(state)
	}
	readTs := cw.txn.ReadTs()
	ts, err := cw.commitOutcomeBatch()
	if err != nil {
		return err
	}
	if err := cw.db.oracle.waitPublished(nil, ts); err != nil {
		return err
	}
	cw.db.oracle.stage.apply(ts)
	// Certify a closed interval: the accepted read saw every publication
	// preceding this commit. An earlier in-flight commit, dictionary write or
	// any other unobserved predecessor requires fresh validation after commit.
	if node := cw.embeddingOutcome; node != nil && ts == readTs+1 {
		state := node.runtimeState()
		state.embeddingPublication = embeddingPublicationProof{engine: cw.engine, oracle: cw.db.oracle, id: node.ID, revision: state.embeddingRevision, commitTs: ts}
		node.setRuntimeState(state)
	}
	return nil
}

// matches runs only inside captureReadView's cached callback, after overlay
// selection. The accepted revision and its real commit boundary must both
// match. Any intervening durable publication, Restore, missing proof or owner
// change falls back to a fresh captured metadata read. No lock spans listeners.
func (proof embeddingPublicationProof) matches(b *BadgerEngine, id NodeID, revision EmbeddingBodyRevision) bool {
	if proof.engine != b || proof.id != id || proof.revision != revision || proof.commitTs == 0 {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.ensureOpenLocked(false) != nil || b.db == nil || proof.oracle != b.db.oracle {
		return false
	}
	o := proof.oracle
	o.mu.Lock()
	defer o.mu.Unlock()
	return !o.readsHeld && o.failed == nil && o.published.Load() == proof.commitTs
}
