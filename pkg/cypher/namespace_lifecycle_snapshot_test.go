package cypher

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type lifecycleSnapshotEngine struct {
	*storage.MemoryEngine
	entered, resume chan struct{}
	once            sync.Once
	armed           atomic.Bool
}

func (e *lifecycleSnapshotEngine) GetInnerEngine() storage.Engine { return e.MemoryEngine }
func (e *lifecycleSnapshotEngine) StreamNodesWithoutEmbeddings(ctx context.Context, visit func(*storage.Node) error) error {
	if e.armed.Load() {
		e.once.Do(func() { close(e.entered); <-e.resume })
	}
	return e.MemoryEngine.StreamNodesWithoutEmbeddings(ctx, visit)
}

func (e *lifecycleSnapshotEngine) StreamNodesWithOptions(ctx context.Context, options storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if e.armed.Load() {
		e.once.Do(func() { close(e.entered); <-e.resume })
	}
	return e.MemoryEngine.StreamNodesWithOptions(ctx, options, visit)
}

type lifecycleSnapshotManager struct{ *testDatabaseManagerAdapter }

func (m *lifecycleSnapshotManager) CreateDatabaseContext(ctx context.Context, name string) error {
	if owner, ok := any(m.manager).(interface {
		CreateDatabaseContext(context.Context, string) error
	}); ok {
		return owner.CreateDatabaseContext(ctx, name)
	}
	return m.manager.CreateDatabase(name)
}

// Pause the real native snapshot streamer while it owns WAL mutationMu. Both
// catalog DDL paths must leave peer public reads usable and cancel preparation
// before the snapshot is released, without publishing a catalog mutation.
func TestNamespaceLifecycleSnapshotKeepsPeerReadsUsable(t *testing.T) {
	for _, command := range []string{"CREATE DATABASE snapshot_new", "DROP DATABASE snapshot_a"} {
		for _, cancelPreparation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel=%v", command, cancelPreparation), func(t *testing.T) {
				base := &lifecycleSnapshotEngine{MemoryEngine: storage.NewMemoryEngine(), entered: make(chan struct{}), resume: make(chan struct{})}
				var unblockOnce sync.Once
				unblock := func() { unblockOnce.Do(func() { close(base.resume) }) }
				defer unblock()
				wal, err := storage.NewWAL(filepath.Join(t.TempDir(), "wal"), &storage.WALConfig{SyncMode: "none", SnapshotInterval: 10 * time.Millisecond})
				require.NoError(t, err)
				engine := storage.NewWALEngine(base, wal)
				manager, err := multidb.NewDatabaseManager(engine, nil)
				require.NoError(t, err)
				t.Cleanup(func() { unblock(); require.NoError(t, manager.Close()) })
				executor := func(name string) *StorageExecutor {
					view, err := manager.GetStorage(name)
					require.NoError(t, err)
					exec := NewStorageExecutor(view)
					exec.SetDatabaseManager(&lifecycleSnapshotManager{&testDatabaseManagerAdapter{manager: manager}})
					return exec
				}
				admin := executor(manager.DefaultDatabaseName())
				typedCountRun(t, admin, "CREATE DATABASE snapshot_a")
				typedCountRun(t, admin, "CREATE DATABASE snapshot_b")
				peer := executor("snapshot_b")
				routing := executor(manager.DefaultDatabaseName())
				typedCountRun(t, peer, "CREATE (:Peer)")
				// Storage sizing also uses the options stream. Pause only after
				// setup, when the native snapshot is the intended consumer.
				base.armed.Store(true)
				require.NoError(t, engine.EnableAutoCompaction(filepath.Join(t.TempDir(), "snapshots")))
				select {
				case <-base.entered:
				case <-time.After(3 * time.Second):
					t.Fatal("native snapshot did not enter its streamer")
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { _, err := admin.Execute(ctx, command, nil); done <- err }()
				select {
				case err := <-done:
					t.Fatalf("DDL did not wait for snapshot: %v", err)
				case <-time.After(30 * time.Millisecond):
				}
				reads := make(chan error, 1)
				started := time.Now()
				go func() {
					for i := 0; i < 5; i++ {
						result, err := routing.Execute(context.Background(), "USE snapshot_b MATCH (n:Peer) RETURN count(n) AS n", nil)
						if err != nil {
							reads <- err
							return
						}
						if fmt.Sprint(result.Rows) != "[[1]]" {
							reads <- fmt.Errorf("peer count: %v", result.Rows)
							return
						}
					}
					reads <- nil
				}()
				var readErr error
				blocked := false
				select {
				case readErr = <-reads:
				case <-time.After(time.Second):
					blocked = true
				}
				t.Logf("peer public reads while %s waits on native snapshot: %s; blocked=%v", command, time.Since(started), blocked)
				if blocked {
					unblock()
					readErr = <-reads
					<-done
					require.NoError(t, readErr)
					t.Fatal("public USE read waited for another database's lifecycle to release the native snapshot")
				}
				if cancelPreparation {
					cancel()
					select {
					case err := <-done:
						require.ErrorIs(t, err, context.Canceled)
					case <-time.After(time.Second):
						unblock()
						<-done
						t.Fatal("DDL admission did not cancel while native snapshot remained paused")
					}
					require.True(t, manager.Exists("snapshot_a"))
					require.False(t, manager.Exists("snapshot_new"))
					unblock()
				} else {
					unblock()
					require.NoError(t, <-done)
				}
				require.NoError(t, readErr)
				require.False(t, blocked, "peer reads must finish before releasing another database's snapshot wait")
			})
		}
	}
}
