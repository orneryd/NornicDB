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
	defer store.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	allow := func() { once.Do(func() { close(release) }) }
	defer allow()
	m := NewManager(time.Hour, func(string) (*cypher.StorageExecutor, error) {
		close(entered)
		<-release
		return cypher.NewStorageExecutor(store), nil
	})
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
		if err != nil {
			t.Errorf("bounded CloseContext changed its empty registered-session contract: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Error("CloseContext exceeded its budget behind an accepted factory")
	}
	// Old CloseContext is the only completion observation available on the old
	// head. The new actual-completion API must distinguish it from real quiescence.
	wait := m.CloseContext
	if waiter, ok := interface{}(m).(interface{ WaitClosed(context.Context) error }); ok {
		wait = waiter.WaitClosed
	}
	if err := wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
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
	if waiter, ok := interface{}(m).(interface{ WaitClosed(context.Context) error }); ok {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := waiter.WaitClosed(ctx); err != nil {
			t.Fatalf("actual manager completion: %v", err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}
