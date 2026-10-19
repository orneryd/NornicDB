package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestManagedBareCypherTransactionsPreserveWrappers(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, buffered := range []bool{false, true} {
		for _, journaled := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("buffered=%v/wal=%v/explicit=%v", buffered, journaled, explicit), func(t *testing.T) {
					base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					defer base.Close()
					peer := storage.NewMemoryEngine()
					defer peer.Close()
					adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
					require.NoError(t, err)
					defer adapter.Close()
					remote, err := NewStorageAdapterWithWAL(peer, t.TempDir())
					require.NoError(t, err)
					defer remote.Close()
					local := NewStandaloneReplicator(DefaultConfig(), adapter)
					require.NoError(t, local.Start(context.Background()))
					defer local.Shutdown()
					router := &managedRouteReplicator{Replicator: local, peer: remote}
					var engine storage.Engine = NewReplicatedEngine(base, router, time.Second)
					if journaled {
						wal, walErr := storage.NewWAL(t.TempDir(), storage.DefaultWALConfig())
						require.NoError(t, walErr)
						wrapped := storage.NewWALEngine(engine, wal)
						defer wrapped.Close()
						engine = wrapped
					}
					const namespace = "bare_command"
					view := storage.NewNamespacedEngine(engine, namespace)
					exec := cypher.NewStorageExecutor(view)
					write := func(key string) error {
						ctx := context.Background()
						if explicit {
							_, err := exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						_, err := exec.Execute(ctx, "CREATE (:Doc {key:$key})", map[string]any{"key": key})
						if err == nil && explicit {
							_, err = exec.Execute(ctx, "COMMIT", nil)
						}
						return err
					}
					require.NoError(t, write("accepted"))
					require.Equal(t, 1, router.calls, "raw transaction must retain its replication wrapper")
					remoteView := storage.NewNamespacedEngine(peer, namespace)
					nodes, err := storage.CollectStoredNodes(context.Background(), remoteView)
					require.NoError(t, err)
					require.Len(t, nodes, 1)
					router.reject = true
					require.ErrorIs(t, write("rejected"), ErrNotLeader)
					for _, engine := range []storage.Engine{view, remoteView} {
						nodes, err := storage.CollectStoredNodes(context.Background(), engine)
						require.NoError(t, err)
						require.Len(t, nodes, 1)
					}
					router.reject, router.delay = false, true
					err = write("uncertain")
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.True(t, storage.MutationPending(err))
					require.NotNil(t, router.pending)
					// The caller's snapshot is retired. Delayed apply must use
					// the detached command and retain any outer WAL wrapper.
					router.delay = false
					require.NoError(t, router.Replicator.Apply(router.pending, time.Second))
					cmd := router.pending
					require.NoError(t, remote.ApplyCommand(&Command{Type: cmd.Type, Data: cmd.Data, Timestamp: cmd.Timestamp, RequestID: cmd.RequestID}))
					for _, engine := range []storage.Engine{view, remoteView} {
						nodes, err := storage.CollectStoredNodes(context.Background(), engine)
						require.NoError(t, err)
						require.Len(t, nodes, 2, "late apply uses a fresh execution after retirement")
					}
				})
			}
		}
	}
}

func TestManagedConcurrentSchemaNameAdmission(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	replicator := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, replicator.Start(context.Background()))
	defer replicator.Shutdown()
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, replicator, time.Second), nil)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateDatabase("schema_admission"))
	view, err := manager.GetStorage("schema_admission")
	require.NoError(t, err)
	const creators = 8
	start := make(chan struct{})
	outcomes := make(chan error, creators)
	for i := 0; i < creators; i++ {
		exec := cypher.NewStorageExecutor(view)
		go func() {
			<-start
			_, err := exec.Execute(context.Background(), "CREATE INDEX same_name IF NOT EXISTS FOR (n:Doc) ON (n.key)", nil)
			outcomes <- err
		}()
	}
	close(start)
	errors := make([]error, creators)
	for i := range errors {
		errors[i] = <-outcomes
	}
	for _, err := range errors {
		require.NoError(t, err, "guarded native schema admission remains atomic")
	}
}
