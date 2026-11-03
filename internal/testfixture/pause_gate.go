package testfixture

import (
	"sync"
	"sync/atomic"
)

// PauseGate pauses one armed observation until its owner releases it. Domain
// adapters decide when to call Pause; the gate owns only the once-only signal
// and idempotent release, including release before a paused worker arrives.
// A new gate is unarmed: the adapter arms the single observation it wants to
// park, and a one-shot adapter arms its gate in its own constructor.
type PauseGate struct {
	Armed   *atomic.Bool
	Entered chan struct{}
	Release chan struct{}
	opened  sync.Once
}

// NewPauseGate returns an initially unarmed gate.
func NewPauseGate() *PauseGate {
	return &PauseGate{Armed: new(atomic.Bool), Entered: make(chan struct{}), Release: make(chan struct{})}
}

// Pause reports whether this caller consumed the armed state and therefore
// paused. Exactly one caller can receive true: that caller closes entered
// before waiting for Release. A caller that receives false did not pause, so
// the observation it stands for must not report anything the gate owns.
func (g *PauseGate) Pause(armed *atomic.Bool, entered chan struct{}) bool {
	if !armed.CompareAndSwap(true, false) {
		return false
	}
	close(entered)
	<-g.Release
	return true
}

// ReleaseWrites releases a paused caller and is safe from repeated cleanup.
func (g *PauseGate) ReleaseWrites() { g.opened.Do(func() { close(g.Release) }) }
