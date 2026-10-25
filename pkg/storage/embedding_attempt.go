package storage

import "sync/atomic"

// EmbeddingRevision returns runtime snapshot identity without exposing it as
// an exported Node field. Every serializer, including legacy gob, ignores it.
func (node *Node) EmbeddingRevision() EmbeddingBodyRevision { return node.embeddingRevision }

// EmbeddingAttempt returns the process-local acceptance/publication token.
func (node *Node) EmbeddingAttempt() *EmbeddingAttempt { return node.embeddingAttempt }

// SetEmbeddingAttempt binds selected work to this in-memory snapshot only.
func (node *Node) SetEmbeddingAttempt(attempt *EmbeddingAttempt) { node.embeddingAttempt = attempt }

// CopyEmbeddingIdentityFrom preserves runtime provenance across detached worker
// copies, just as storage's node copies do, without creating persisted fields.
func (node *Node) CopyEmbeddingIdentityFrom(source *Node) {
	node.embeddingOwner, node.embeddingOwnerID = source.embeddingOwner, source.embeddingOwnerID
	node.embeddingRevision, node.embeddingAttempt = source.embeddingRevision, source.embeddingAttempt
	node.embeddingPublication = source.embeddingPublication
	node.embeddingWaitDeferred = source.embeddingWaitDeferred
}

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
	obsolete           atomic.Bool
	delivered          chan struct{}
}

func NewEmbeddingAttempt(generation, epoch uint64, revision EmbeddingBodyRevision, begin func() (func(), bool), publish func(*Node)) *EmbeddingAttempt {
	return &EmbeddingAttempt{ProviderGeneration: generation, ClaimEpoch: epoch, BodyRevision: revision, begin: begin, publish: publish, delivered: make(chan struct{})}
}

// PersistEmbeddingOutcome carries the existing persistence/completion boundary
// through an outer lease owner. persist uses the ordinary sidecar route, with
// consumption deferred on a private wrapper copy. The returned completion must
// run after that owner's lease ends. An inner owner inherits the deferral and
// returns no completion of its own, so exactly the outermost caller waits.
func PersistEmbeddingOutcome(node *Node, persist func(*Node) error) (func() error, error) {
	if node == nil || node.embeddingAttempt == nil || node.embeddingWaitDeferred {
		return nil, persist(node)
	}
	detached := *node
	detached.embeddingWaitDeferred = true
	if err := persist(&detached); err != nil {
		return nil, err
	}
	return func() error { return waitEmbeddingOutcome(node.embeddingAttempt) }, nil
}

// commitEmbeddingOutcome is the single acceptance/publication boundary for
// vectors, terminal markers and explicit resets. persist must conflict-check
// the body read and return that authoritative body with only the payload applied.
// A bound caller lends this private snapshot until delivery completes; notify
// transfers it to the ordered queue instead of cloning the accepted payload.
func commitEmbeddingOutcome(node *Node, persist func() error, notify func(*Node)) error {
	deferredWait := node.embeddingWaitDeferred
	node.embeddingWaitDeferred = false // the wrapper scope must never enter published/cached bodies
	attempt := node.embeddingAttempt
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
	// notify only readies the already-reserved event. The authoritative attempt
	// acceptance and reservation are complete before any outer lease is released.
	notify(node)
	if !deferredWait {
		return waitEmbeddingOutcome(attempt)
	}
	return nil
}

// waitEmbeddingOutcome consumes exactly this attempt, outside wrapper leases.
func waitEmbeddingOutcome(attempt *EmbeddingAttempt) error {
	if attempt == nil {
		return nil
	}
	<-attempt.delivered
	if attempt.obsolete.Load() {
		return ErrEmbeddingSourceChanged
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
	if node == nil {
		attempt.obsolete.Store(true)
		return
	}
	if attempt.publish != nil {
		attempt.publish(node)
	}
}

// publishNodeOutcome is the common revision barrier on both storage branches.
// The notification dispatcher serializes this check and delivery with body/delete
// events. A body commit which already overtook this event suppresses it; a
// mutation arriving during delivery queues its event after this one. Listener
// reentrant writes remain safe: no storage/queue lock is held across callbacks.
func (b *BadgerEngine) publishNodeOutcome(node *Node, outcome bool, publish func()) {
	attempt := node.embeddingAttempt
	if outcome {
		same, err := b.embeddingRevisionIsCurrent(node.ID, node.embeddingRevision, node.embeddingPublication)
		if err != nil || !same {
			deliverEmbeddingOutcome(attempt, nil)
			return
		}
	}
	defer func() { b.callCommitListener(func() { deliverEmbeddingOutcome(attempt, node) }) }()
	publish()
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
