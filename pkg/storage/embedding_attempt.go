package storage

import (
	"context"
	"sync/atomic"
)

// EmbeddingRevision returns runtime snapshot identity without exposing it as
// an exported Node field. Every serializer, including legacy gob, ignores it.
func (node *Node) EmbeddingRevision() EmbeddingBodyRevision {
	return node.runtimeState().embeddingRevision
}

// EmbeddingAttempt returns the process-local acceptance/publication token.
func (node *Node) EmbeddingAttempt() *EmbeddingAttempt { return node.runtimeState().embeddingAttempt }

// SetEmbeddingAttempt binds selected work to this in-memory snapshot only.
// Unchanged identity keeps the attached record, so this does not allocate.
func (node *Node) SetEmbeddingAttempt(attempt *EmbeddingAttempt) {
	state := node.runtimeState()
	if state.embeddingAttempt == attempt {
		return
	}
	state.embeddingAttempt = attempt
	node.setRuntimeState(state)
}

// CopyEmbeddingIdentityFrom preserves runtime provenance across detached worker
// copies, just as storage's node copies do, without creating persisted fields.
// Only the identity fields are replaced: this image's metadata stays its own.
func (node *Node) CopyEmbeddingIdentityFrom(source *Node) {
	identity := source.runtimeState()
	state := node.runtimeState()
	state.embeddingOwner = identity.embeddingOwner
	state.embeddingOwnerID = identity.embeddingOwnerID
	state.embeddingRevision = identity.embeddingRevision
	state.embeddingAttempt = identity.embeddingAttempt
	state.embeddingPublication = identity.embeddingPublication
	node.setRuntimeState(state)
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

type embeddingDeliveryKey struct{}

type embeddingDelivery struct {
	completions []func()
}

func completeEmbeddingDelivery(ctx context.Context, complete func()) {
	if delivery, ok := ctx.Value(embeddingDeliveryKey{}).(*embeddingDelivery); ok {
		delivery.completions = append(delivery.completions, complete)
		return
	}
	complete()
}

// PersistEmbeddingOutcome separates commit from callback delivery across the
// outermost provider or WAL lease. Nested owners share the same boundary.
func PersistEmbeddingOutcome(ctx context.Context, node *Node, persist func(context.Context) error) error {
	if ctx.Value(embeddingDeliveryKey{}) != nil || node == nil {
		return persist(ctx)
	}
	delivery := &embeddingDelivery{}
	err := persist(context.WithValue(ctx, embeddingDeliveryKey{}, delivery))
	for _, complete := range delivery.completions {
		complete()
	}
	if err != nil {
		return err
	}
	return waitEmbeddingOutcome(node.EmbeddingAttempt())
}

// commitEmbeddingOutcome is the single acceptance/publication boundary for
// vectors, terminal markers and explicit resets. persist must conflict-check
// the body read and return that authoritative body with only the payload applied.
// A bound caller lends this private snapshot until delivery completes; notify
// transfers it to the ordered queue instead of cloning the accepted payload.
func commitEmbeddingOutcome(ctx context.Context, node *Node, persist func() error, notify func(*Node)) error {
	attempt := node.EmbeddingAttempt()
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
	if ctx.Value(embeddingDeliveryKey{}) != nil {
		completeEmbeddingDelivery(ctx, func() { notify(node) })
		return nil
	}
	notify(node)
	return waitEmbeddingOutcome(attempt)
}

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
// The notification drainer serializes this check and delivery with body/delete
// events. A body commit which already overtook this event suppresses it; a
// mutation arriving during delivery queues its event after this one. Listener
// reentrant writes remain safe: no storage/queue lock is held across callbacks.
// A "never sample a replacement boundary after committing" review note: the
// record is read once, so delivery sees the identity that barrier accepted.
func (b *BadgerEngine) publishNodeOutcome(node *Node, outcome bool, publish func()) {
	state := node.runtimeState()
	attempt := state.embeddingAttempt
	if outcome {
		same, err := b.embeddingRevisionIsCurrent(context.WithoutCancel(b.work.Context()), node.ID, state.embeddingRevision, state.embeddingPublication)
		if err != nil || !same {
			deliverEmbeddingOutcome(attempt, nil)
			return
		}
	}
	defer deliverEmbeddingOutcome(attempt, node)
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
