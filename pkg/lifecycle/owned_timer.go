package lifecycle

import (
	"context"
	"sync"
	"time"
)

// OwnedTimer retains an accepted callback until Stop prevents it from starting
// or until its actual return. A failed Stop never reports completion.
type OwnedTimer struct {
	timer      *time.Timer
	mu         sync.Mutex
	release    func()
	stopCancel func() bool
	completed  bool
}

// AfterFuncOwned acquires the caller's existing lifetime before arming. It
// stops when that lifetime ends; it has no registry or cancellation source.
// arm creates the native timer (time.AfterFunc, or an owner's injected clock).
func AfterFuncOwned(ctx context.Context, arm func(time.Duration, func()) *time.Timer, delay time.Duration, retain func() (func(), bool), fn func()) *OwnedTimer {
	release, accepted := retain()
	if !accepted {
		return nil
	}
	t := &OwnedTimer{release: release}
	t.timer = arm(delay, func() { defer t.complete(); fn() })
	stop := context.AfterFunc(ctx, func() { t.Stop() })
	t.mu.Lock()
	completed := t.completed
	if !completed {
		t.stopCancel = stop
	}
	t.mu.Unlock()
	if completed {
		stop()
	}
	return t
}
func (t *OwnedTimer) complete() {
	t.mu.Lock()
	if t.completed {
		t.mu.Unlock()
		return
	}
	t.completed = true
	stop, release := t.stopCancel, t.release
	t.mu.Unlock()
	if stop != nil {
		stop()
	}
	release()
}
func (t *OwnedTimer) Stop() bool {
	if t == nil {
		return false
	}
	stopped := t.timer.Stop()
	if stopped {
		t.complete()
	}
	return stopped
}
