package testfixture

import (
	"sync"
	"sync/atomic"
)

// PauseGate pauses one armed observation until its owner releases it. Domain
// adapters decide when to call Pause; the gate owns only the once-only signal
// and idempotent release, including release before a paused worker arrives.
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

// Pause blocks only the first caller that consumes the armed state.
func (g *PauseGate) Pause(armed *atomic.Bool, entered chan struct{}) {
	if armed.CompareAndSwap(true, false) {
		close(entered)
		<-g.Release
	}
}

// ReleaseWrites releases a paused caller and is safe from repeated cleanup.
func (g *PauseGate) ReleaseWrites() { g.opened.Do(func() { close(g.Release) }) }
