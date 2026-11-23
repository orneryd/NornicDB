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

// Cancel when beginRead subscribes to the caller. As with a statement's lazy
// relay, the derived Done stays open until the request is polled.
type cancelOnSearchReadSubscription struct {
	*fulltextDelayedContext
	cancel context.CancelFunc
}

func (c *cancelOnSearchReadSubscription) Done() <-chan struct{} {
	c.cancel()
	return c.fulltextDelayedContext.Done()
}

func TestVectorQueryRelationshipsObservesCallerBeforeCancellationRelay(t *testing.T) {
	store := testfixture.Own(t, testfixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	svc := NewService(store, 2, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.IndexEdge(&storage.Edge{
		ID: "match", StartNode: "a", EndNode: "b", Type: "R",
		Properties: map[string]any{"emb": []float32{1, 0}},
	}))
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	parent := &cancelOnSearchReadSubscription{
		fulltextDelayedContext: &fulltextDelayedContext{Context: caller, done: make(chan struct{})},
		cancel:                 cancel,
	}
	hits, err := svc.VectorQueryRelationships(parent, []float32{1, 0}, RelationshipVectorQuerySpec{
		Type: "R", Property: "emb", Similarity: "cosine", Limit: 1,
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, hits)
}
