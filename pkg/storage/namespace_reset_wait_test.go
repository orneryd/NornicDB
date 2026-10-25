package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Deletion lands data without consuming callbacks. Its explicit external
// barrier covers the reset behind a callback already popped for delivery.
func TestDeleteByPrefixDataLandingAndResetBarrier(t *testing.T) {
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(&Node{ID: "test:keep"})
	require.NoError(t, err)
	delivering, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var consumed atomic.Bool
	require.NoError(t, engine.SubscribeEvents(EventHandlers{
		NodeCreated: func(n *Node) {
			if n.ID == "test:slow" {
				close(delivering)
				<-release
			}
		},
		NamespaceReset: func(string) { consumed.Store(true) },
	}))
	_, err = engine.CreateNode(&Node{ID: "test:slow"})
	require.NoError(t, err)
	<-delivering
	deleted := make(chan error, 1)
	go func() { _, _, err := engine.DeleteByPrefix("test:"); deleted <- err }()
	select {
	case err := <-deleted:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("data deletion consumed a blocked listener")
	}
	require.False(t, consumed.Load())
	_, err = engine.GetStoredNode("test:keep")
	require.ErrorIs(t, err, ErrNotFound)
	waited := make(chan struct{})
	go func() { engine.WaitForListeners(); close(waited) }()
	select {
	case <-waited:
		t.Fatal("barrier returned before reset consumption")
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("reset barrier did not complete")
	}
	require.True(t, consumed.Load())
}
