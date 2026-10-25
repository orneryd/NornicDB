package multidb

import (
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDispatcherDropReservesNameThroughReset(t *testing.T) {
	b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	m, err := NewDatabaseManager(b, nil)
	require.NoError(t, err)
	defer m.Close()
	require.NoError(t, m.CreateDatabase("victim"))
	require.NoError(t, m.CreateDatabase("audit"))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	b.OnNamespaceReset(func(prefix string) {
		if prefix == "victim:" {
			close(entered)
			<-release
		}
	})
	done := make(chan error, 1)
	go func() { done <- m.DropDatabase("victim") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reset not delivered")
	}
	access := make(chan error, 1)
	go func() { _, err := m.GetStorage("audit"); access <- err }()
	select {
	case err := <-access:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("reset wait retained manager lock")
	}
	require.ErrorIs(t, m.CreateDatabase("victim"), ErrDatabaseExists)
	require.ErrorIs(t, m.CreateCompositeDatabase("victim", nil), ErrDatabaseExists)
	select {
	case err := <-done:
		t.Fatalf("drop returned before reset consumption: %v", err)
	default:
	}
	unblock()
	require.NoError(t, <-done)
	require.NoError(t, m.CreateDatabase("victim"))
}
