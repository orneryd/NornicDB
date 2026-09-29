package storage

// gh726_bulk_delete_notification_drain_test.go — regression tests for #726:
// BulkDeleteNodes dispatches node-deleted notifications from a tracked
// goroutine that Close drains before releasing engine state, so a
// notification never runs against torn-down state and the -race suite stays
// clean.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGh726_BulkDeleteNotificationsDrainedBeforeClose(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)

	var notifications atomic.Int64
	engine.OnNodeDeleted(func(id NodeID) {
		notifications.Add(1)
	})

	// Three nodes, deleted in one bulk call.
	_, err = engine.CreateNode(&Node{ID: "nornic:n-1", Labels: []string{"T"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "nornic:n-2", Labels: []string{"T"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "nornic:n-3", Labels: []string{"T"}})
	require.NoError(t, err)

	require.NoError(t, engine.BulkDeleteNodes([]NodeID{"nornic:n-1", "nornic:n-2", "nornic:n-3"}))

	// Close must wait for the in-flight notification goroutine: after it
	// returns, every dispatch has run and the engine is closed.
	require.NoError(t, engine.Close())
	require.Equal(t, int64(3), notifications.Load(), "every deleted node dispatches its notification before Close returns")
}

func TestGh726_BulkDeleteNotificationsAfterCloseAreSkipped(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)

	var notifications atomic.Int64
	engine.OnNodeDeleted(func(id NodeID) {
		notifications.Add(1)
	})
	_, err = engine.CreateNode(&Node{ID: "nornic:n-1", Labels: []string{"T"}})
	require.NoError(t, err)

	require.NoError(t, engine.Close())

	// A bulk delete on a closed engine fails without dispatching.
	err = engine.BulkDeleteNodes([]NodeID{"nornic:n-1"})
	require.Error(t, err)
	require.Zero(t, notifications.Load())
}

func TestGh726_CloseWaitsForInFlightBulkDeleteNotification(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "nornic:n-1", Labels: []string{"T"}})
	require.NoError(t, err)

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce atomic.Bool
	var notifications atomic.Int64
	engine.OnNodeDeleted(func(id NodeID) {
		notifications.Add(1)
		if startedOnce.CompareAndSwap(false, true) {
			close(started)
		}
		<-release
	})

	var deleteErr error
	deleteDone := make(chan struct{})
	go func() {
		defer close(deleteDone)
		deleteErr = engine.BulkDeleteNodes([]NodeID{"nornic:n-1"})
	}()

	// The notification dispatch is in flight (blocked on release).
	<-started

	closeDone := make(chan error, 1)
	go func() { closeDone <- engine.Close() }()

	// Close must not return while the dispatch is still running: the write
	// barrier keeps BulkDeleteNodes' notification registration inside the
	// window Close drains.
	select {
	case <-closeDone:
		t.Fatal("Close returned before the in-flight notification finished")
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-closeDone)
	<-deleteDone
	require.NoError(t, deleteErr)
	require.Equal(t, int64(1), notifications.Load(), "the in-flight dispatch ran before Close returned")
}
