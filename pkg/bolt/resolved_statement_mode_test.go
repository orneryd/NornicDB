package bolt

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRemoteDynamicModeUsesBoltSuccessType(t *testing.T) {
	base := storage.NewMemoryEngine()
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
