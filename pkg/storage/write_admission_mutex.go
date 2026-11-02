package storage

import (
	"context"
	"sync"
)

// contextRWMutex is the shared cancellation-aware admission kernel. No
// goroutine outlives the wait. Index/buffer locks use it directly; publication
// locks add their existing ownership observation through trackedRWMutex.
// Context-taking calls require their actual non-nil lifetime. Ordinary Lock and
// RLock select an unbounded lifetime at their context-free entry.
type contextRWMutex struct {
	mu             sync.Mutex
	readers        int
	writer         bool
	waitingWriters int
	changed        chan struct{}
}

func (m *contextRWMutex) acquire(ctx context.Context, exclusive bool, owner any) error {
	m.mu.Lock()
	if exclusive {
		m.waitingWriters++
	}
	for {
		if ctx.Err() != nil {
			if exclusive {
				m.waitingWriters--
				m.signalLocked()
			}
			m.mu.Unlock()
			return ctx.Err()
		}
		if !m.writer && (exclusive && m.readers == 0 || !exclusive && m.waitingWriters == 0) {
			if exclusive {
				m.waitingWriters--
				m.writer = true
			} else {
				m.readers++
			}
			m.mu.Unlock()
			return nil
		}
		if m.changed == nil {
			m.changed = make(chan struct{})
		}
		changed := m.changed
		m.mu.Unlock()
		observeWriteAdmissionWait(owner, ctx)
		select {
		case <-changed:
		case <-ctx.Done():
		}
		m.mu.Lock()
	}
}

func (m *contextRWMutex) signalLocked() {
	if m.changed != nil {
		close(m.changed)
		m.changed = nil
	}
}

func (m *contextRWMutex) LockContext(ctx context.Context) error  { return m.acquire(ctx, true, m) }
func (m *contextRWMutex) RLockContext(ctx context.Context) error { return m.acquire(ctx, false, m) }
func (m *contextRWMutex) Lock()                                  { _ = m.LockContext(context.Background()) }
func (m *contextRWMutex) RLock()                                 { _ = m.RLockContext(context.Background()) }
func (m *contextRWMutex) Unlock()                                { m.release(true) }
func (m *contextRWMutex) RUnlock()                               { m.release(false) }

func (m *contextRWMutex) TryLock() bool  { return m.tryAcquire(true) }
func (m *contextRWMutex) TryRLock() bool { return m.tryAcquire(false) }

func (m *contextRWMutex) tryAcquire(exclusive bool) bool {
	m.mu.Lock()
	available := !m.writer && (exclusive && m.readers == 0 || !exclusive && m.waitingWriters == 0)
	if available {
		if exclusive {
			m.writer = true
		} else {
			m.readers++
		}
	}
	m.mu.Unlock()
	return available
}

func (m *contextRWMutex) release(exclusive bool) {
	m.mu.Lock()
	if exclusive {
		m.writer = false
	} else {
		m.readers--
	}
	m.signalLocked()
	m.mu.Unlock()
}

// downgrade turns the caller's write hold into a read hold in one step under
// the internal mutex: no waiter can acquire the write lock in between. A caller
// that published state under the write lock keeps that state under the returned
// read hold, and a queued writer waits for this read hold as for any reader,
// so writer priority and cancellation semantics stay unchanged. The caller
// releases the downgraded hold with RUnlock.
func (m *contextRWMutex) downgrade() {
	m.mu.Lock()
	m.writer = false
	m.readers++
	m.signalLocked()
	m.mu.Unlock()
}

// trackedRWMutex observes the existing publication/transaction lock protocol.
// These holds must never span listener delivery. Ordinary schema, index and
// buffer locks share admission mechanics but are not publication-order holds.
type trackedRWMutex struct {
	contextRWMutex
	// Accepted ownership may release a hold on another goroutine. The
	// goroutine-local publication observer cannot represent that transfer.
	crossGoroutine bool
}

func (m *trackedRWMutex) acquire(ctx context.Context, exclusive bool) error {
	if err := m.contextRWMutex.acquire(ctx, exclusive, m); err != nil {
		return err
	}
	if !m.crossGoroutine && lockHoldHooks != nil {
		lockHoldHooks.held()
	}
	return nil
}
func (m *trackedRWMutex) LockContext(ctx context.Context) error  { return m.acquire(ctx, true) }
func (m *trackedRWMutex) RLockContext(ctx context.Context) error { return m.acquire(ctx, false) }
func (m *trackedRWMutex) Lock()                                  { _ = m.LockContext(context.Background()) }
func (m *trackedRWMutex) RLock()                                 { _ = m.RLockContext(context.Background()) }
func (m *trackedRWMutex) Unlock()                                { m.release(true) }
func (m *trackedRWMutex) RUnlock()                               { m.release(false) }
func (m *trackedRWMutex) TryLock() bool                          { return m.tryAcquire(true) }
func (m *trackedRWMutex) TryRLock() bool                         { return m.tryAcquire(false) }
func (m *trackedRWMutex) tryAcquire(exclusive bool) bool {
	acquired := m.contextRWMutex.tryAcquire(exclusive)
	if acquired && !m.crossGoroutine && lockHoldHooks != nil {
		lockHoldHooks.held()
	}
	return acquired
}
func (m *trackedRWMutex) release(exclusive bool) {
	if !m.crossGoroutine && lockHoldHooks != nil {
		lockHoldHooks.released()
	}
	m.contextRWMutex.release(exclusive)
}
