package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_PublicationBeforeReentrantListener(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	published := false
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "review:outer" {
			return
		}
		require.True(t, published, "accounting/admission callback precedes listener")
		_, err := engine.CreateNode(&Node{ID: "review:child"})
		require.NoError(t, err)
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:outer"})
	require.NoError(t, err)
	require.NoError(t, tx.CommitContextWithPublication(context.Background(), func() { published = true }))
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestBadgerTransaction_ListenerOrderAndSnapshotUnderReentry(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var observed []string
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "review:ordered" {
			return
		}
		close(entered)
		<-release
		mu.Lock()
		observed = append(observed, node.Properties["value"].(string))
		mu.Unlock()
	})
	engine.OnNodeUpdated(func(node *Node) {
		mu.Lock()
		observed = append(observed, node.Properties["value"].(string))
		mu.Unlock()
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:ordered", Properties: map[string]any{"value": "first"}})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- tx.Commit() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("listener did not start")
	}
	update, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, update.UpdateNode(&Node{ID: "review:ordered", Properties: map[string]any{"value": "second"}}))
	updated := make(chan error, 1)
	go func() { updated <- update.Commit() }()
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("independent publication waited for listener delivery")
	}
	close(release)
	require.NoError(t, <-done)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"first", "second"}, observed)
}

func TestBadgerTransaction_CloseWaitsForListenerWithoutWriteBarrier(t *testing.T) {
	engine := NewMemoryEngine()
	entered, release := make(chan struct{}), make(chan struct{})
	callbackResult := make(chan error, 1)
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "review:close" {
			return
		}
		close(entered)
		<-release
		_, err := engine.CreateNode(&Node{ID: "review:after-close"})
		callbackResult <- err
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:close"})
	require.NoError(t, err)
	committed := make(chan error, 1)
	go func() { committed <- tx.Commit() }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- engine.Close() }()
	require.Eventually(t, func() bool { engine.mu.RLock(); defer engine.mu.RUnlock(); return engine.closed }, time.Second, time.Millisecond)
	close(release)
	select {
	case err := <-callbackResult:
		require.ErrorIs(t, err, ErrStorageClosed)
	case <-time.After(time.Second):
		t.Fatal("Close held write barrier against listener re-entry")
	}
	require.NoError(t, <-committed)
	require.NoError(t, <-closed)
}

func TestBadgerTransaction_RejectedCommitDoesNotPublish(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:canceled"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, tx.CommitContextWithPublication(ctx, func() { t.Error("failed commit published") }), context.Canceled)
	require.NoError(t, tx.Rollback())
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestBadgerTransaction_ListenerPanicPreservesPublicationAndQueue(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	engine.OnNodeCreated(func(node *Node) {
		if node.ID == "review:panic" {
			panic("listener failed")
		}
	})
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "review:panic"})
	require.NoError(t, err)
	published := false
	require.PanicsWithValue(t, "listener failed", func() { _ = tx.CommitContextWithPublication(context.Background(), func() { published = true }) })
	require.True(t, published)
	_, err = engine.GetNode("review:panic")
	require.NoError(t, err, "listener failure cannot erase committed data")
	next, err := engine.BeginTransaction()
	require.NoError(t, err)
	_, err = next.CreateNode(&Node{ID: "review:after-panic"})
	require.NoError(t, err)
	require.NoError(t, next.Commit(), "the notification queue remains usable after a surfaced callback panic")
}
