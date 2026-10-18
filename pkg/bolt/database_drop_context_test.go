package bolt

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoltDatabaseDropContext(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	adapter := &boltDatabaseManagerAdapter{manager: manager}
	require.NoError(t, manager.CreateDatabase("ctx_drop"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, adapter.DropDatabaseContext(ctx, "ctx_drop"), context.Canceled)
	require.True(t, manager.Exists("ctx_drop"))
	require.NoError(t, adapter.DropDatabaseContext(context.Background(), "ctx_drop"))
	require.False(t, manager.Exists("ctx_drop"))
}
