package storage

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// Shutdown matrix S2, BadgerEngine: concurrent Close callers all join the one
// retirement; none returns while an admitted helper is held, admission is
// refused once the stop began, and a repeat Close reports the same outcome.
func TestOwnerMatrixBadgerConcurrentClose(t *testing.T) {
	engine := newTestMemoryEngine(t)
	if _, err := engine.beginHelperTxn(); err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			engine.work.Leave()
		}
	}()

	const callers = 4
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- engine.Close() }()
	}
	select {
	case err := <-results:
		t.Fatalf("a Close caller returned while a helper was admitted: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := engine.beginHelperTxn(); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("helper admission after Close began = %v, want ErrStorageClosed", err)
	}
	held = false
	engine.work.Leave()

	var first error
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if i == 0 {
				first = err
			} else if fmt.Sprint(err) != fmt.Sprint(first) {
				t.Errorf("Close caller %d got %v, another got %v", i, err, first)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Close did not complete")
		}
	}
	if err := engine.Close(); fmt.Sprint(err) != fmt.Sprint(first) {
		t.Errorf("repeat Close = %v, want %v", err, first)
	}
	if engine.DB() != nil {
		t.Error("engine state not released after Close")
	}
}
