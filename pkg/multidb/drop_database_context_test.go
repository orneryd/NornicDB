package multidb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestDropDatabaseContextCancelsMetadataWait(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	manager, err := NewDatabaseManager(base, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("drop_cancel"))
	manager.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = manager.DropDatabaseContext(ctx, "drop_cancel")
	manager.mu.Unlock()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, manager.Exists("drop_cancel"))
	require.NoError(t, manager.DropDatabaseContext(context.Background(), "drop_cancel"))
}
