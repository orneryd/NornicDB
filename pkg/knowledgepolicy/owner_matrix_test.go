package knowledgepolicy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	kp "github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// Shutdown matrix S2, AccessFlusher: concurrent Stop callers all join the
// run's final flush and receive its outcome; none returns while it is held.
func TestOwnerMatrixAccessFlusherConcurrentStop(t *testing.T) {
	failure := errors.New("native access write")
	store := &classAccessStore{entered: make(chan struct{}), release: make(chan struct{}), writeErr: failure}
	acc := kp.NewAccessAccumulator(true, 0)
	f := kp.NewAccessFlusher(acc, store, time.Hour)
	f.Start(context.Background())
	acc.IncrementAccess("ns:write")

	const callers = 4
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- f.Stop() }()
	}
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		close(store.release)
		t.Fatal("final flush did not enter the native store")
	}
	select {
	case err := <-results:
		close(store.release)
		t.Fatalf("a Stop caller returned while the final flush was held: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, failure) {
				t.Errorf("Stop caller %d omitted the flush outcome: %v", i, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent Stop did not complete")
		}
	}
	if n := store.writes.Load(); n != 1 {
		t.Errorf("final flush wrote %d times, want once", n)
	}
}
