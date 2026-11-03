package txsession

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestManagerStopR2OpenFactoryLifetime(t *testing.T) {
	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	allow := func() { once.Do(func() { close(release) }) }
	m := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		close(entered)
		<-release
		return cypher.NewStorageExecutor(store), nil
	}, t.Context())
	t.Cleanup(func() {
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer joinCancel()
		if err := m.CloseContext(joinCtx); err != nil {
			t.Errorf("manager did not reach actual completion during cleanup: %v", err)
		}
	})
	// Cleanups run last-registered-first, so the factory gate is released
	// before the manager is joined even when an assertion fails.
	t.Cleanup(allow)
	opened := make(chan error, 1)
	go func() { _, err := m.Open(context.Background(), "nornic"); opened <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("public Open did not enter its factory")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- m.CloseContext(ctx) }()
	select {
	case err := <-closed:
		// An accepted factory is opening work the manager owns: the bounded
		// close reports its own deadline instead of joining that work.
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("CloseContext joined accepted factory work instead of reporting its deadline: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("CloseContext exceeded its budget behind an accepted factory")
	}
	// Actual-completion observation stays separate from the close caller: the
	// deadline expires here while the accepted factory still owns the work.
	if err := m.WaitClosed(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("actual cleanup reported completion while accepted Open still owned its factory: %v", err)
	}
	allow()
	select {
	case err := <-opened:
		if err == nil {
			t.Error("accepted factory published a new session after close")
		}
	case <-time.After(time.Second):
		t.Fatal("Open did not finish after releasing its factory")
	}
	m.mu.RLock()
	registered := len(m.sessions)
	m.mu.RUnlock()
	if registered != 0 {
		t.Errorf("closed manager registered %d session(s) from the rejected open", registered)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := m.WaitClosed(waitCtx); err != nil {
		t.Fatalf("actual manager completion: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}
