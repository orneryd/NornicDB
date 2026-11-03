package testfixture

import (
	"sync"
	"testing"
	"time"
)

// TestPauseGateSingleSignalAndIdempotentRelease races one armed gate: exactly
// one caller consumes the arm (Pause reports true and waits for the release),
// every other caller reports false without pausing, and repeated release stays
// idempotent.
func TestPauseGateSingleSignalAndIdempotentRelease(t *testing.T) {
	gate := NewPauseGate()
	t.Cleanup(gate.ReleaseWrites)
	if consumed := gate.Pause(gate.Armed, gate.Entered); consumed {
		t.Fatal("an unarmed gate paused a caller")
	}
	gate.Armed.Store(true)

	consumedByCaller := make(chan bool, 8)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() { defer workers.Done(); consumedByCaller <- gate.Pause(gate.Armed, gate.Entered) }()
	}
	WaitForBarrier(t, gate.Entered, time.Second, gate.ReleaseWrites, "armed gate did not signal")
	if gate.Armed.Load() {
		t.Fatal("armed state was not consumed")
	}
	gate.ReleaseWrites()
	gate.ReleaseWrites()

	var consumed, passed int
	for range 8 {
		select {
		case consumedArm := <-consumedByCaller:
			if consumedArm {
				consumed++
			} else {
				passed++
			}
		case <-time.After(time.Second):
			t.Fatal("a Pause caller did not return")
		}
	}
	workers.Wait()
	if consumed != 1 {
		t.Fatalf("exactly one caller must consume the arm, got %d", consumed)
	}
	if passed != 7 {
		t.Fatalf("the other 7 callers must report the consumed arm, got %d", passed)
	}
}

// TestPauseGateReleaseBeforeObservation releases the gate before a caller
// observes it: the caller that consumes the arm still reports the observation
// and does not block.
func TestPauseGateReleaseBeforeObservation(t *testing.T) {
	gate := NewPauseGate()
	gate.ReleaseWrites()
	gate.Armed.Store(true)
	consumedByCaller := make(chan bool, 1)
	go func() { consumedByCaller <- gate.Pause(gate.Armed, gate.Entered) }()
	select {
	case consumed := <-consumedByCaller:
		if !consumed {
			t.Fatal("the armed caller did not consume the gate")
		}
	case <-time.After(time.Second):
		gate.ReleaseWrites()
		t.Fatal("release before observation blocked")
	}
	select {
	case <-gate.Entered:
	default:
		t.Fatal("released observation did not signal")
	}
}
