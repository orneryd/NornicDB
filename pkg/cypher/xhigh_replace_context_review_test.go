package cypher

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type xhighReplaceManager struct {
	*lifecycleSnapshotManager
	entered chan struct{}
}

func (m *xhighReplaceManager) DropDatabase(name string) error {
	close(m.entered)
	return m.manager.DropDatabase(name)
}

func (m *xhighReplaceManager) DropDatabaseContext(ctx context.Context, name string) error {
	close(m.entered)
	return m.manager.DropDatabaseContext(ctx, name)
}

func TestXHighReplaceDatabaseCancellation(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, command := range []string{"DROP DATABASE snapshot_a", "CREATE OR REPLACE DATABASE snapshot_a"} {
		t.Run(command, func(t *testing.T) {
			base := &lifecycleSnapshotEngine{MemoryEngine: storage.NewMemoryEngine(), entered: make(chan struct{}), resume: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(base.resume) }) }
			defer unblock()
			wal, err := storage.NewWAL(filepath.Join(t.TempDir(), "wal"), &storage.WALConfig{SyncMode: "none", SnapshotInterval: 10 * time.Millisecond})
			require.NoError(t, err)
			engine := storage.NewWALEngine(base, wal)
			manager, err := multidb.NewDatabaseManager(engine, nil)
			require.NoError(t, err)
			t.Cleanup(func() { unblock(); require.NoError(t, manager.Close()) })
			require.NoError(t, manager.CreateDatabase("snapshot_a"))
			view, err := manager.GetStorage("snapshot_a")
			require.NoError(t, err)
			writer := NewStorageExecutor(view)
			typedCountRun(t, writer, "CREATE (:Keep)")
			adminView, err := manager.GetStorage(manager.DefaultDatabaseName())
			require.NoError(t, err)
			admin := NewStorageExecutor(adminView)
			adapter := &xhighReplaceManager{lifecycleSnapshotManager: &lifecycleSnapshotManager{&testDatabaseManagerAdapter{manager: manager}}, entered: make(chan struct{})}
			admin.SetDatabaseManager(adapter)
			require.NoError(t, engine.EnableAutoCompaction(filepath.Join(t.TempDir(), "snapshots")))
			select {
			case <-base.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("snapshot did not enter its streamer")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := admin.Execute(ctx, command, nil); done <- err }()
			select {
			case <-adapter.entered:
			case err := <-done:
				t.Fatalf("DDL returned before reaching manager: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("DDL did not reach manager")
			}
			cancel()
			blocked := false
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(300 * time.Millisecond):
				blocked = true
			}
			unblock()
			if blocked {
				err := <-done
				t.Errorf("canceled DDL waited for snapshot release; returned %v", err)
			}
			fresh, err := manager.GetStorage("snapshot_a")
			require.NoError(t, err)
			reader := NewStorageExecutor(fresh)
			result, err := reader.Execute(context.Background(), "MATCH (n:Keep) RETURN count(n) AS n", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, "cancellation before journal admission must preserve existing data")
		})
	}
}
