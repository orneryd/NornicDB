package storage

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// DeleteByPrefix returns only once its namespace reset was consumed, also
// when another writer is delivering earlier events: a database recreated
// after the deletion returns never meets the dropped one's derived state.
func TestDeleteByPrefixReturnsAfterResetConsumed(t *testing.T) {
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(&Node{ID: "test:keep"})
	require.NoError(t, err)

	delivering := make(chan struct{})
	release := make(chan struct{})
	var consumed atomic.Bool
	require.NoError(t, engine.SubscribeEvents(EventHandlers{
		NodeCreated: func(node *Node) {
			if node.ID == "test:slow" {
				close(delivering)
				<-release
			}
		},
		NamespaceReset: func(string) { consumed.Store(true) },
	}))
	go func() {
		_, _ = engine.CreateNode(&Node{ID: "test:slow"})
	}()
	<-delivering // that writer now delivers; the reset queues behind it

	deleted := make(chan struct{})
	go func() {
		_, _, _ = engine.DeleteByPrefix("test:")
		close(deleted)
	}()
	select {
	case <-deleted:
		t.Fatal("DeleteByPrefix returned before its namespace reset was consumed")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case <-deleted:
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteByPrefix did not return")
	}
	require.True(t, consumed.Load(), "DeleteByPrefix returned before its namespace reset was consumed")
}
