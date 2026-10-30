package multidb

import (
	"context"
	"fmt"
	"sync"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// dropHandoff decides who finishes a new DROP intent: the DROP call that
// submitted it, while that call still waits, or else the owner's completion.
// The receipt callback and the waiter each report once; exactly one of them
// sees both facts and, if the waiter left first, schedules the completion.
type dropHandoff struct {
	mu       sync.Mutex
	name     string
	applied  bool
	detached bool
}

// intentApplied runs after a new DROP intent's fence is published and synced.
func (h *dropHandoff) intentApplied(m *DatabaseManager) {
	h.mu.Lock()
	h.applied = true
	detached := h.detached
	h.mu.Unlock()
	if detached {
		m.scheduleDropCompletion(h.name)
	}
}

// waiterDetached runs when the submitting call stops waiting without a
// terminal outcome (cancellation, replication wait timeout).
func (h *dropHandoff) waiterDetached(m *DatabaseManager) {
	h.mu.Lock()
	h.detached = true
	applied := h.applied
	h.mu.Unlock()
	if applied {
		m.scheduleDropCompletion(h.name)
	}
}

// scheduleDropCompletion is called for a new DROP intent whose caller stopped
// waiting. The completion runs as accepted work of the storage accepted-
// command owner (storage.AcceptedWorkFor): no lifetime of its own; engine
// Close stops and waits for it, and recovery finishes what it leaves.
// It takes the exclusive DROP admission, so it runs after the DROP that
// submitted the intent returns, and finishes the drop only while this
// generation's fence is still held: CREATE is refused under the fence, so the
// name cannot denote a newer database. A cleared fence means the drop already
// finished, and the completion does nothing.
func (m *DatabaseManager) scheduleDropCompletion(name string) {
	work, err := storage.AcceptedWorkFor(m.inner)
	if err != nil || !work.Enter() {
		return
	}
	go func() {
		defer work.Leave()
		_ = m.FinishPendingDrop(work.Context(), name)
	}()
}

// FinishPendingDrop finishes a drop the DROP owner already accepted for name,
// and nothing else. Without a held fence it reports ErrDatabaseNotFound, so it
// can never reach a live or newer database of that name. Entry points that
// may only remove their own kind of live database (Qdrant collections) use it
// for a name that is no longer available to them.
func (m *DatabaseManager) FinishPendingDrop(ctx context.Context, name string) error {
	if ctx == nil {
		return fmt.Errorf("DROP requires an operation context: %w", storage.ErrInvalidData)
	}
	release, err := m.dropAdmission.acquire(ctx, true, false)
	if err != nil {
		return err
	}
	if !m.dropFenceHeld(name) {
		release()
		return ErrDatabaseNotFound
	}
	return m.dropAdmitted(ctx, name, release)
}
