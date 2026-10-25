package multidb

// A listener delivered by a managed explicit commit may start its own managed
// commit in the same database: delivery follows the admission lease release.

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestWriteBehindManagedListenerReentry(t *testing.T) {
	b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	m, err := NewDatabaseManager(b, nil)
	require.NoError(t, err)
	defer m.Close()
	e, err := m.GetStorage("nornic")
	require.NoError(t, err)
	require.NoError(t, m.SetDatabaseLimits("nornic", &Limits{Storage: StorageLimits{MaxNodes: 10}}))
	var listenerErr error
	var called bool
	b.OnNodeCreated(func(n *storage.Node) {
		if n.ID != "nornic:prior" {
			return
		}
		called = true
		child, err := b.BeginTransaction()
		if err != nil {
			listenerErr = err
			return
		}
		_, err = child.CreateNode(&storage.Node{ID: "nornic:from-listener"})
		if err != nil {
			listenerErr = err
			_ = child.Rollback()
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
		defer cancel()
		listenerErr = e.(*sizeTrackingEngine).CommitTransaction(ctx, child)
	})
	prior, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, prior.SetImplicit(true))
	_, err = prior.CreateNode(&storage.Node{ID: "nornic:prior"})
	require.NoError(t, err)
	require.NoError(t, e.(*sizeTrackingEngine).CommitTransaction(context.Background(), prior))
	require.True(t, prior.AcknowledgedBuffered())
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&storage.Node{ID: "nornic:original"})
	require.NoError(t, err)
	// Writing the buffered node orders the commit after its generation, so
	// the commit lands it and delivers its event.
	require.NoError(t, tx.UpdateNode(&storage.Node{ID: "nornic:prior", Properties: map[string]any{"v": int64(1)}}))
	require.NoError(t, e.(*sizeTrackingEngine).CommitTransaction(context.Background(), tx))
	b.WaitForListeners()
	require.True(t, called)
	require.NoError(t, listenerErr, "listener could not reacquire the database admission lease held by its calling commit")
}
