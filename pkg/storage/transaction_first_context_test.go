package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestTransactionFirstNodeReadRetainsCallerContext(t *testing.T) {
	e := newTestMemoryEngine(t)
	_, err := e.CreateNode(t.Context(), &Node{ID: "test:first", Labels: []string{"First"}})
	require.NoError(t, err)
	tx, err := e.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	node, err := tx.GetFirstNodeByLabel(ctx, "First")
	require.Nil(t, node)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTransactionDeleteOldNodeReadRetainsCallerContext(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	bufferedCreate(t, engine, &Node{ID: "test:delete-context"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting := make(chan struct{}, 1)
	restore := setAdmissionWaitObserver(func(owner any, _ context.Context) {
		if owner == &engine.writeBehind.mu {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	})
	defer restore()
	engine.writeBehind.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(engine.writeBehind.mu.Unlock) }
	defer unlock()
	done := make(chan error, 1)
	go func() { done <- tx.DeleteGraphNode(ctx, "test:delete-context", CascadeEdges) }()
	joined := false
	defer func() {
		unlock()
		if !joined {
			<-done
		}
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("delete must reach its actual old-node reader")
	}
	cancel()
	stopped := true
	select {
	case err = <-done:
		joined = true
	case <-time.After(time.Second):
		stopped = false
		unlock()
		err = <-done
		joined = true
	}
	require.True(t, stopped, "caller cancellation must release the native old-node wait before its unrelated holder")
	require.ErrorIs(t, err, context.Canceled)
	unlock()
	require.Zero(t, tx.OperationCount())
	require.NoError(t, tx.DeleteGraphNode(t.Context(), "test:delete-context", CascadeEdges))
	require.NoError(t, tx.Rollback())
	_, err = engine.GetNode(t.Context(), "test:delete-context")
	require.NoError(t, err)
}
