package storage

import (
	"context"
	"sync"
)

// trackedRWMutex retains the existing write-admission owner while permitting
// its actual waiter to leave on cancellation. No goroutine outlives the wait.
type trackedRWMutex struct {
	// Accepted ownership may release a hold on another goroutine. The
	// existing goroutine-local hold observer cannot represent that transfer.
	crossGoroutine bool
	mu             sync.Mutex
	readers        int
	writer         bool
	waitingWriters int
	changed        chan struct{}
}

func (m *trackedRWMutex) acquire(ctx context.Context, exclusive bool) error {
	m.mu.Lock()
	if exclusive {
		m.waitingWriters++
	}
	for {
		if ctx != nil && ctx.Err() != nil {
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
			if !m.crossGoroutine && lockHoldHooks != nil {
				lockHoldHooks.held()
			}
			return nil
		}
		if m.changed == nil {
			m.changed = make(chan struct{})
		}
		changed := m.changed
		m.mu.Unlock()
		observeWriteAdmissionWait(m, ctx)
		if ctx == nil {
			<-changed
		} else {
			select {
			case <-changed:
			case <-ctx.Done():
			}
		}
		m.mu.Lock()
	}
}

func (m *trackedRWMutex) signalLocked() {
	if m.changed != nil {
		close(m.changed)
		m.changed = nil
	}
}

func (m *trackedRWMutex) LockContext(ctx context.Context) error  { return m.acquire(ctx, true) }
func (m *trackedRWMutex) RLockContext(ctx context.Context) error { return m.acquire(ctx, false) }
func (m *trackedRWMutex) Lock()                                  { _ = m.LockContext(nil) }
func (m *trackedRWMutex) RLock()                                 { _ = m.RLockContext(nil) }
func (m *trackedRWMutex) Unlock()                                { m.release(true) }
func (m *trackedRWMutex) RUnlock()                               { m.release(false) }

func (m *trackedRWMutex) TryLock() bool  { return m.tryAcquire(true) }
func (m *trackedRWMutex) TryRLock() bool { return m.tryAcquire(false) }

func (m *trackedRWMutex) tryAcquire(exclusive bool) bool {
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
	if available && !m.crossGoroutine && lockHoldHooks != nil {
		lockHoldHooks.held()
	}
	return available
}

func (m *trackedRWMutex) release(exclusive bool) {
	if !m.crossGoroutine && lockHoldHooks != nil {
		lockHoldHooks.released()
	}
	m.mu.Lock()
	if exclusive {
		m.writer = false
	} else {
		m.readers--
	}
	m.signalLocked()
	m.mu.Unlock()
}
