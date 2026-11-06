package storagefault

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// CloseHold wraps a native Badger owner so a consumer's accepted command of one
// namespace can be held in flight while the engine closes: the Close-phase
// checks of every accepted-work consumer (multidb, Cypher, Qdrant) hold their
// command here, start Close, and release. The held submission yields to its
// caller's cancellation, as a real submitter does, and then reaches the native
// owner, which decides it against the closing engine.
// Author: NOR-288 lane (Claude), 2026-10-30.
type CloseHold struct {
	*storage.BadgerEngine
	namespace string

	mu           sync.Mutex
	armed        bool
	entered      chan struct{}
	release      chan struct{}
	releaseOnce  sync.Once
	closeEntered chan struct{}
	closeOnce    sync.Once
	held         atomic.Int32
}

// NewCloseHold wraps native; Arm selects the next submission of namespace.
func NewCloseHold(native *storage.BadgerEngine, namespace string) *CloseHold {
	return &CloseHold{BadgerEngine: native, namespace: namespace, closeEntered: make(chan struct{})}
}

// GetInnerEngine exposes the native owner beneath this submitter.
func (h *CloseHold) GetInnerEngine() storage.Engine { return h.BadgerEngine }

// Arm holds the next accepted submission of the namespace until Release. The
// returned channel closes when that submission is held.
func (h *CloseHold) Arm() <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.armed = true
	h.entered = make(chan struct{})
	h.release = make(chan struct{})
	h.releaseOnce = sync.Once{}
	return h.entered
}

// Release lets the held submission reach the native owner.
func (h *CloseHold) Release() {
	h.mu.Lock()
	release, once := h.release, &h.releaseOnce
	h.mu.Unlock()
	if release != nil {
		once.Do(func() { close(release) })
	}
}

// CloseEntered closes when Close is first called on this owner.
func (h *CloseHold) CloseEntered() <-chan struct{} { return h.closeEntered }

// Held reports how many submissions were held.
func (h *CloseHold) Held() int { return int(h.held.Load()) }

// Close signals CloseEntered, then closes the native owner.
func (h *CloseHold) Close() error {
	h.closeOnce.Do(func() { close(h.closeEntered) })
	return h.BadgerEngine.Close()
}

// SubmitEntityCommit holds the armed submission, then forwards to the native
// owner.
func (h *CloseHold) SubmitEntityCommit(ctx context.Context, prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	h.mu.Lock()
	hold := h.armed && prepared.Namespace() == h.namespace
	entered, release := h.entered, h.release
	if hold {
		h.armed = false
	}
	h.mu.Unlock()
	if hold {
		h.held.Add(1)
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
	}
	return h.BadgerEngine.SubmitEntityCommit(ctx, prepared, settle)
}
