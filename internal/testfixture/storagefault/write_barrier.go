// Package storagefault provides storage-shaped test controls, including the
// write barriers tests use to interleave a second write with a caller's
// refresh. It is a subpackage of testfixture because these controls are typed
// by pkg/storage, which testfixture itself must not import.
package storagefault

import (
	"sync/atomic"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// PostUpdateBarrier owns the post-success write barrier consumer tests need:
// the backing UpdateNode commits, the first armed call fires its arm, closes
// Entered, then waits for Release before UpdateNode returns. Consumers can
// then commit another write before the caller's refresh runs. No goroutine
// stacks are inspected.
type PostUpdateBarrier struct{ *testfixture.PauseGate }

// NewPostUpdateBarrier returns a shared gate with a post-success storage adapter.
func NewPostUpdateBarrier() *PostUpdateBarrier {
	return &PostUpdateBarrier{PauseGate: testfixture.NewPauseGate()}
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
	if err == nil {
		e.barrier.Pause(e.barrier.Armed, e.barrier.Entered)
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

func (b *ConsumerBarrier) DeleteNode(id storage.NodeID, origins ...uint64) error {
	b.Pause(&b.DeleteArm, b.DeleteEntered)
	return b.Engine.DeleteNode(id, origins...)
}
