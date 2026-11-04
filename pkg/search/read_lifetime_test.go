package search

import (
	"context"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

// The statement owner polls its caller synchronously even when its Done relay
// has not run yet. Deriving a service read must preserve that observation.
type fulltextDelayedContext struct {
	context.Context
	done chan struct{}
	once sync.Once
}

func (c *fulltextDelayedContext) Value(any) any         { return nil }
func (c *fulltextDelayedContext) Done() <-chan struct{} { return c.done }
func (c *fulltextDelayedContext) Err() error {
	err := c.Context.Err()
	if err != nil {
		c.once.Do(func() { close(c.done) })
	}
	return err
}

func TestSearchReadObservesCallerBeforeCancellationRelay(t *testing.T) {
	store := testfixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	svc := NewService(store, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	parent := &fulltextDelayedContext{Context: caller, done: make(chan struct{})}
	owned, release, err := svc.beginRead(parent)
	require.NoError(t, err)
	defer release()
	cancel()
	select {
	case <-parent.done:
		t.Fatal("control: the statement relay ran before polling")
	default:
	}
	require.ErrorIs(t, owned.Err(), context.Canceled)
	select {
	case <-owned.Done():
	default:
		t.Fatal("Err must close the owned read's Done channel")
	}
}
