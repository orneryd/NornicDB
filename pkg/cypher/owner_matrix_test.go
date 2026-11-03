package cypher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Shutdown matrix, standalone QueryEmbedderBinding (no host lifetime bound):
// its provider users are enrolled in the binding's own owner.

// S1: Shutdown waits for an admitted provider use; later use is refused.
func TestOwnerMatrixQueryEmbedderBindingStopDuringHeldUse(t *testing.T) {
	var binding QueryEmbedderBinding
	_, finish, err := binding.BeginUse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- binding.Shutdown(context.Background()) }()
	select {
	case err := <-stopped:
		t.Fatalf("Shutdown returned while a provider use was held: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, _, err := binding.BeginUse(context.Background()); !errors.Is(err, ErrQueryEmbedderClosed) {
		t.Fatalf("use after Shutdown began = %v, want ErrQueryEmbedderClosed", err)
	}
	finish()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}

// S2: repeated and concurrent Shutdown callers all join the same stop.
func TestOwnerMatrixQueryEmbedderBindingConcurrentStop(t *testing.T) {
	var binding QueryEmbedderBinding
	_, finish, err := binding.BeginUse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	const callers = 4
	results := make(chan error, callers)
	var started sync.WaitGroup
	for i := 0; i < callers; i++ {
		started.Add(1)
		go func() { started.Done(); results <- binding.Shutdown(context.Background()) }()
	}
	started.Wait()
	select {
	case err := <-results:
		t.Fatalf("a Shutdown caller returned before the held use: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	finish()
	for i := 0; i < callers; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := binding.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// S4: a host-bound binding admits through its host, and its own Shutdown
// does not refuse the host's users (the host's stop joins them).
func TestOwnerMatrixQueryEmbedderBindingHostBound(t *testing.T) {
	var binding QueryEmbedderBinding
	hostRefused := errors.New("host closed")
	var hostUses int
	binding.SetLifetime(func(ctx context.Context) (context.Context, func(), error) {
		hostUses++
		if hostUses > 1 {
			return nil, nil, hostRefused
		}
		return ctx, func() {}, nil
	})
	_, finish, err := binding.BeginUse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	finish()
	if _, _, err := binding.BeginUse(context.Background()); !errors.Is(err, hostRefused) {
		t.Fatalf("host-bound use = %v, want the host's refusal", err)
	}
}
