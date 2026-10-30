// Package storagefault provides storage-shaped test controls, including the
// write barriers tests use to interleave a second write with a caller's
// refresh. It is a subpackage of testfixture because these controls are typed
// by pkg/storage, which testfixture itself must not import.
package storagefault

import (
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// PostUpdateBarrier owns the post-success write barrier consumer tests need:
// the backing UpdateNode commits, the first armed call fires its arm, closes
// Entered, then waits for Release before UpdateNode returns. Consumers can
// then commit another write before the caller's refresh runs. No goroutine
// stacks are inspected.
type PostUpdateBarrier struct {
	// Armed is a pointer because a collection store that opens engines per
	// request must share one arm across the engines it returns.
	Armed   *atomic.Bool
	Entered chan struct{}
	Release chan struct{}
	opened  sync.Once
}

// NewPostUpdateBarrier returns a barrier with its channels allocated. ReleaseWrites is
// idempotent so a failed test releases a paused writer from cleanup.
func NewPostUpdateBarrier() *PostUpdateBarrier {
	return &PostUpdateBarrier{Armed: new(atomic.Bool), Entered: make(chan struct{}), Release: make(chan struct{})}
}

// ReleaseWrites releases a paused writer; later calls do nothing.
func (barrier *PostUpdateBarrier) ReleaseWrites() {
	barrier.opened.Do(func() { close(barrier.Release) })
}

// PauseUpdateAfterCommit returns engine wrapped so it shares this barrier.
// Callers wait through WaitForBarrier(..., barrier.Entered, ..., barrier.ReleaseWrites, ...).
func (barrier *PostUpdateBarrier) PauseUpdateAfterCommit(engine storage.Engine) storage.Engine {
	return &barrierUpdateEngine{Engine: engine, barrier: barrier}
}

type barrierUpdateEngine struct {
	storage.Engine
	barrier *PostUpdateBarrier
}

func (e *barrierUpdateEngine) UpdateNode(updateRequest storage.NodeUpdateRequest) error {
	err := e.Engine.UpdateNode(updateRequest)
	if err == nil && e.barrier.Armed.CompareAndSwap(true, false) {
		close(e.barrier.Entered)
		<-e.barrier.Release
	}
	return err
}

// ConsumerBarrier shares the post-update owner and adds a separate
// pre-delete arm. Both arms use the same idempotent release gate.
type ConsumerBarrier struct {
	storage.Engine
	*PostUpdateBarrier
	DeleteArm     atomic.Bool
	DeleteEntered chan struct{}
}

func NewConsumerBarrier(engine storage.Engine) *ConsumerBarrier {
	update := NewPostUpdateBarrier()
	return &ConsumerBarrier{
		Engine:            update.PauseUpdateAfterCommit(engine),
		PostUpdateBarrier: update,
		DeleteEntered:     make(chan struct{}),
	}
}

func (b *ConsumerBarrier) DeleteNode(id storage.NodeID) error {
	if b.DeleteArm.CompareAndSwap(true, false) {
		close(b.DeleteEntered)
		<-b.Release
	}
	return b.Engine.DeleteNode(id)
}
