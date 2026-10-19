package storage

import "sync/atomic"

// EmbeddingAttempt is the immutable identity of one selected provider's work.
// It is carried through decorators, never encoded in a node body. begin leases
// the queue identity through the storage commit; body/source acceptance belongs
// to that same commit. All outcome publication uses its authoritative body.
type EmbeddingAttempt struct {
	ProviderGeneration uint64
	ClaimEpoch         uint64
	BodyRevision       EmbeddingBodyRevision
	begin              func() (func(), bool)
	publish            func(*Node)
	committed          atomic.Bool
	delivered          chan struct{}
}

func NewEmbeddingAttempt(generation, epoch uint64, revision EmbeddingBodyRevision, begin func() (func(), bool), publish func(*Node)) *EmbeddingAttempt {
	return &EmbeddingAttempt{ProviderGeneration: generation, ClaimEpoch: epoch, BodyRevision: revision, begin: begin, publish: publish, delivered: make(chan struct{})}
}

// commitEmbeddingOutcome is the single acceptance/publication boundary for
// vectors, terminal markers and explicit resets. persist must conflict-check
// the body read and return that authoritative body with only the payload applied.
func commitEmbeddingOutcome(node *Node, persist func() error, notify func(*Node)) error {
	attempt := node.EmbeddingAttempt
	if attempt != nil {
		release, valid := attempt.begin()
		if !valid {
			return ErrEmbeddingSourceChanged
		}
		err := func() error { defer release(); return persist() }()
		if err != nil {
			return err
		}
	} else if err := persist(); err != nil {
		return err
	}
	// No queue mutex is held while calling listeners (listeners may read stats
	// or enqueue). The provider write lease still covers both publications.
	notify(node)
	if attempt != nil {
		<-attempt.delivered
	}
	return nil
}

// deliverEmbeddingOutcome is called by ordered storage event delivery, after
// the authoritative body's listeners. No second worker publication route exists.
func deliverEmbeddingOutcome(attempt *EmbeddingAttempt, node *Node) {
	if attempt == nil || !attempt.committed.CompareAndSwap(false, true) {
		return
	}
	defer close(attempt.delivered)
	if attempt.publish != nil {
		attempt.publish(node)
	}
}

// IsCurrent avoids sending already-invalid work to decorators. It uses the
// identical attempt lease as final storage acceptance, which always rechecks
// after decorators; this observation alone never authorizes publication.
func (attempt *EmbeddingAttempt) IsCurrent() bool {
	release, valid := attempt.begin()
	if valid {
		release()
	}
	return valid
}
