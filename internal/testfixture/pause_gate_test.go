package testfixture

import (
	"sync"
	"testing"
	"time"
)

func TestPauseGateSingleSignalAndIdempotentRelease(t *testing.T) {
	gate := NewPauseGate()
	t.Cleanup(gate.ReleaseWrites)
	gate.Pause(gate.Armed, gate.Entered) // unarmed calls do not consume the one signal
	gate.Armed.Store(true)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() { defer workers.Done(); gate.Pause(gate.Armed, gate.Entered) }()
	}
	WaitForBarrier(t, gate.Entered, time.Second, gate.ReleaseWrites, "armed gate did not signal")
	gate.ReleaseWrites()
	gate.ReleaseWrites()
	workers.Wait()
	if gate.Armed.Load() {
		t.Fatal("armed state was not consumed")
	}
}

func TestPauseGateReleaseBeforeObservation(t *testing.T) {
	gate := NewPauseGate()
	gate.ReleaseWrites()
	gate.Armed.Store(true)
	done := make(chan struct{})
	go func() { gate.Pause(gate.Armed, gate.Entered); close(done) }()
	WaitForBarrier(t, done, time.Second, gate.ReleaseWrites, "release before observation blocked")
	select {
	case <-gate.Entered:
	default:
		t.Fatal("released observation did not signal")
	}
}
