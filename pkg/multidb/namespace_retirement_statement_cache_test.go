package multidb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSolR3DropContextCacheCallback(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	m, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	require.NoError(t, m.CreateDatabase("a"))
	oldScope, err := m.StatementCacheScope("a")
	require.NoError(t, err)
	var calls atomic.Int32
	unregister := m.RegisterStatementCacheIndex(func() { m.Exists("a"); calls.Add(1) })
	defer unregister()
	m.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, m.DropDatabaseContext(ctx, "a"), context.DeadlineExceeded)
	m.mu.Unlock()
	require.True(t, m.Exists("a"))
	require.True(t, oldScope.Valid())
	require.Zero(t, calls.Load())
	done := make(chan error, 1)
	go func() { done <- m.DropDatabaseContext(context.Background(), "a") }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("DROP callback reentered a held metadata lock")
	}
	require.False(t, oldScope.Valid())
	require.Equal(t, int32(1), calls.Load())
	require.NoError(t, m.CreateDatabase("a"))
	fresh, err := m.StatementCacheScope("a")
	require.NoError(t, err)
	require.True(t, fresh.Valid())
	require.NotEqual(t, oldScope.Generation(), fresh.Generation())
}
