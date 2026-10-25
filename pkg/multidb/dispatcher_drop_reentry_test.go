package multidb

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDispatcherReentrantDropRetainsNameUntilReset(t *testing.T) {
	b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
	require.NoError(t, err)
	m, err := NewDatabaseManager(b, nil)
	require.NoError(t, err)
	require.NoError(t, m.CreateDatabase("victim"))
	require.NoError(t, m.CreateDatabase("audit"))
	releaseCallback, releaseReset := make(chan struct{}), make(chan struct{})
	var callbackOnce, resetOnce sync.Once
	unblockCallback := func() { callbackOnce.Do(func() { close(releaseCallback) }) }
	unblockReset := func() { resetOnce.Do(func() { close(releaseReset) }) }
	deadlocked := false
	defer func() {
		unblockCallback()
		unblockReset()
		if !deadlocked {
			require.NoError(t, m.Close())
		}
	}()
	dropped, resetting := make(chan error, 1), make(chan struct{})
	b.OnNamespaceReset(func(prefix string) {
		if prefix == "victim:" {
			close(resetting)
			<-releaseReset
		}
	})
	b.OnNodeCreated(func(n *storage.Node) {
		if n.ID == "audit:trigger" {
			dropped <- m.DropDatabase("victim")
			<-releaseCallback
		}
	})
	created := make(chan error, 1)
	go func() { _, err := b.CreateNode(&storage.Node{ID: "audit:trigger"}); created <- err }()
	select {
	case err := <-dropped:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		deadlocked = true
		t.Fatal("listener DropDatabase waited for its own delivery")
	}
	require.ErrorIs(t, m.CreateDatabase("victim"), ErrDatabaseExists, "callback return cannot release an unconsumed reset reservation")
	unblockCallback()
	select {
	case <-resetting:
	case <-time.After(4 * time.Second):
		deadlocked = true
		t.Fatal("queued reset did not enter delivery")
	}
	require.ErrorIs(t, m.CreateDatabase("victim"), ErrDatabaseExists)
	unblockReset()
	require.NoError(t, <-created)
	require.NoError(t, b.FlushWriteBehind())
	require.NoError(t, m.CreateDatabase("victim"), "consumed reset releases its name")
}
