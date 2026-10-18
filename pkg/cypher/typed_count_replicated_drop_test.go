package cypher_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type blockedDropEngine struct {
	storage.Engine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type replicatedDropManager struct {
	cypher.DatabaseManagerInterface
	manager *multidb.DatabaseManager
}

func (m replicatedDropManager) DropDatabaseContext(ctx context.Context, name string) error {
	return m.manager.DropDatabaseContext(ctx, name)
}

func (m replicatedDropManager) GetStorageForUse(name, token string) (interface{}, error) {
	return m.manager.GetStorageWithAuth(name, token)
}

func (b *blockedDropEngine) GetInnerEngine() storage.Engine { return b.Engine }
func (b *blockedDropEngine) DeleteByPrefix(prefix string) (int64, int64, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.Engine.DeleteByPrefix(prefix)
}
func (b *blockedDropEngine) DeleteByPrefixContext(ctx context.Context, prefix string) (int64, int64, error) {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return 0, 0, ctx.Err()
}

// This is the real manager -> replication -> adapter -> namespace-owner path.
// The owner blocks before deletion, making cancellation independent of timing.
func TestTypedCountReplicatedDropCancellation(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	blocked := &blockedDropEngine{Engine: base, entered: make(chan struct{}), release: make(chan struct{})}
	adapter, err := replication.NewStorageAdapterWithWAL(blocked, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, adapter.Close()) })
	repl := replication.NewStandaloneReplicator(replication.DefaultConfig(), adapter)
	require.NoError(t, repl.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, repl.Shutdown()) })
	manager, err := multidb.NewDatabaseManager(replication.NewReplicatedEngine(blocked, repl, 30*time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("cancel_drop"))
	require.NoError(t, manager.CreateDatabase("cancel_peer"))
	_, err = base.CreateNode(&storage.Node{ID: "cancel_drop:seed", Labels: []string{"Saved"}})
	require.NoError(t, err)
	adminStorage, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	admin := cypher.NewStorageExecutor(adminStorage)
	admin.SetDatabaseManager(replicatedDropManager{manager: manager})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := admin.Execute(ctx, "DROP DATABASE cancel_drop", nil); done <- err }()
	select {
	case <-blocked.entered:
	case <-time.After(3 * time.Second):
		close(blocked.release)
		t.Fatal("DROP did not reach the namespace owner")
	}
	cancel()
	select {
	case err := <-done:
		close(blocked.release)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		close(blocked.release)
		<-done
		t.Fatal("replicated DROP lost the statement cancellation")
	}
	require.True(t, manager.Exists("cancel_drop"))
	_, err = base.GetNode("cancel_drop:seed")
	require.NoError(t, err, "cancelled pre-deletion DROP must leave the data intact")
}
