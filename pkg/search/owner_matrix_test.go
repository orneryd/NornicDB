package search_test

import (
	"context"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Shutdown matrix S2, search Service: BeginShutdown followed by concurrent
// Close callers all join the one stop; none returns while accepted work
// (the lazy warm build) is held, and a repeat Close joins the same result.
func TestOwnerMatrixSearchConcurrentStop(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer engine.Close()
	svc := search.NewService(engine, search.DefaultVectorDimensions, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	svc.SetLazyWarming(true, func() { close(entered); <-release })
	go func() { _ = svc.EnsureWarm(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("warm build did not start")
	}

	svc.BeginShutdown()
	const callers = 3
	results := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- svc.Close() }()
	}
	select {
	case err := <-results:
		t.Fatalf("a Close caller returned while the warm build was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	for i := 0; i < callers; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("Close caller %d: %v", i, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent Close did not complete")
		}
	}
	if err := svc.Close(); err != nil {
		t.Errorf("repeat Close: %v", err)
	}
	if !svc.Closed() {
		t.Error("service not closed after Close")
	}
}
