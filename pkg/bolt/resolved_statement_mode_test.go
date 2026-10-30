package bolt

import (
	"context"
	"fmt"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteDynamicModeUsesBoltSuccessType(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	manager := &mockDBManager{stores: map[string]storage.Engine{"nornic": storage.NewNamespacedEngine(base, "nornic")}, defaultDB: "nornic"}
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 8, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, manager)
	port := startBoltTestServer(t, server)
	for _, tc := range []struct {
		name, body string
		write      bool
	}{
		{"read", "RETURN 1 AS value", false},
		{"write", "CREATE (:U5BoltMode) RETURN 1 AS value", true},
		{"write without counters", "UNWIND [] AS x CREATE (:U5BoltMode) RETURN count(*) AS value", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: fmt.Sprintf("bolt://127.0.0.1:%d", port), Database: "nornic"})
			require.NoError(t, err)
			defer func() { require.NoError(t, engine.Close()) }()
			tx, err := engine.BeginCypherTx(context.Background())
			require.NoError(t, err)
			_, _, err = tx.QueryCypher(context.Background(), "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", map[string]interface{}{"body": tc.body})
			require.NoError(t, err)
			mode, ok := tx.(storage.RemoteCypherMode)
			require.True(t, ok)
			write, known := mode.ResolvedWriteMode()
			require.True(t, known)
			require.Equal(t, tc.write, write)
			require.NoError(t, tx.Rollback(context.Background()))
		})
	}
}

func TestRemotePeriodicCoordinatorMetadataBolt(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	manager := &mockDBManager{stores: map[string]storage.Engine{"nornic": store}, defaultDB: "nornic"}
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 8, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, manager)
	port := startBoltTestServer(t, server)
	for _, procedure := range []string{"apoc.periodic.iterate", "apoc.periodic.commit"} {
		for _, clearMetadata := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/clear=%v", procedure, clearMetadata), func(t *testing.T) {
				engine, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: fmt.Sprintf("bolt://127.0.0.1:%d", port), Database: "nornic"})
				require.NoError(t, err)
				defer engine.Close()
				tx, err := engine.BeginCypherTx(storage.WithCoordinatorOwnedTransaction(context.Background()))
				require.NoError(t, err)
				defer tx.Rollback(context.Background())
				if clearMetadata {
					_, _, err = tx.QueryCypher(context.Background(), "CALL tx.setMetaData({})", nil)
					require.NoError(t, err)
				}
				body := "CALL " + procedure + "('UNWIND [1,2,3] AS x RETURN x','CREATE (:U5PeriodicRemote)',{batchSize:1})"
				if procedure == "apoc.periodic.commit" {
					body = "CALL apoc.periodic.commit('CREATE (:U5PeriodicRemote) RETURN 1',{})"
				}
				_, _, err = tx.QueryCypher(context.Background(), "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", map[string]interface{}{"body": body})
				require.ErrorContains(t, err, "unsupported query type in transaction")
				nodes, err := store.GetNodesByLabel("U5PeriodicRemote")
				require.NoError(t, err)
				require.Empty(t, nodes, "coordinator-owned periodic call must fail before its first action")
			})
		}
	}
}
