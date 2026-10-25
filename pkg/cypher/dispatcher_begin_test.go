package cypher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDispatcherExplicitBeginDoesNotConsume(t *testing.T) {
	b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	b.OnNodeCreated(func(n *storage.Node) {
		if n.ID == "test:begin-held" {
			close(entered)
			<-release
		}
	})
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&storage.Node{ID: "test:begin-held"})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
	begun := make(chan error, 1)
	go func() { _, beginErr := e.Execute(context.Background(), "BEGIN", nil); begun <- beginErr }()
	<-entered
	early := false
	select {
	case err := <-begun:
		require.NoError(t, err)
		early = true
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	if !early {
		require.NoError(t, <-begun)
	}
	_, err = e.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
	require.True(t, early, "explicit BEGIN waited for an earlier callback")
}
