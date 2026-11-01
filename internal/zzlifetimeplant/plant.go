// Author: #1636 lane (Claude). Planted census controls: one member of each
// lifetime-copy class, in a new package outside the owner. Never product code;
// overlaid only for the planted census run.
package zzlifetimeplant

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// K1 and K2: a private admission flag, join and accumulator.
type plantedOwner struct {
	mu      sync.Mutex
	closing bool
	wg      sync.WaitGroup
	err     error
}

func (o *plantedOwner) record(err error) {
	o.mu.Lock()
	o.err = errors.Join(o.err, err)
	o.mu.Unlock()
}

// K3: a hand-written one-shot completion.
type plantedCompletion struct {
	once sync.Once
	done chan struct{}
	err  error
}

// K4: a timer that releases its own credit.
func plantedTimer(release func()) *time.Timer {
	return time.AfterFunc(time.Hour, func() { defer release() })
}

// K5: an accepted no-op release.
func plantedAccept() (func(), bool) { return func() {}, true }

// K6: an absent owner accepted as if present.
func plantedAbsent(owner context.Context) (context.Context, bool) {
	if owner == nil {
		return context.Background(), true
	}
	return owner, true
}

// K1 through a function value of errors.Join's type.
func (o *plantedOwner) recordThroughValue(err error) {
	join := errors.Join
	o.mu.Lock()
	o.err = join(o.err, err)
	o.mu.Unlock()
}

// K2 through an interface: the join is reached only by its method set.
type plantedInterfaceOwner struct {
	join interface {
		Add(int)
		Done()
		Wait()
	}
	stopped atomic.Bool
}

// K4 through a function value of time.AfterFunc's type.
func plantedTimerValue(release func()) *time.Timer {
	arm := time.AfterFunc
	return arm(time.Hour, func() { defer release() })
}

var _ = []any{(&plantedOwner{}).record, (&plantedOwner{}).recordThroughValue, plantedCompletion{}, plantedInterfaceOwner{}, plantedTimer, plantedTimerValue, plantedAccept, plantedAbsent}
