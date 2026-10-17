package bolt

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A before trigger installed on the database fires for Bolt writes, in the
// writing transaction: an auto-commit RUN and an explicit BEGIN / RUN /
// COMMIT, whose COMMIT carries the session's context into the commit.
func TestBoltWritesFireApocBeforeTriggers(t *testing.T) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "test")
	system := storage.NewNamespacedEngine(base, "system")
	dbExec := cypher.NewStorageExecutor(store)
	runtime, err := cypher.NewTriggerRuntime(context.Background(), cypher.TriggerRuntimeConfig{
		Engine: base,
		System: system,
		ExecutorFor: func(database string) (*cypher.StorageExecutor, error) {
			if database == "test" {
				return dbExec, nil
			}
			return nil, fmt.Errorf("no database %s", database)
		},
		DatabaseExists: func(database string) bool { return database == "test" },
		Enabled:        true,
	})
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	_, err = cypher.NewStorageExecutor(system).Execute(context.Background(),
		"CALL apoc.trigger.install('test', 'stamp', 'UNWIND $createdNodes AS n SET n.stamp = $trigger', {phase: 'before'})", nil)
	require.NoError(t, err)

	server := New(&Config{Port: 0, MaxConnections: 10, ReadBufferSize: 8192, WriteBufferSize: 8192}, newTxCapableCypherQueryExecutor(store))
	defer server.Close()
	port := serveBoltIntegrationServer(t, server)
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, PerformHandshakeWithTesting(t, conn))
	require.NoError(t, SendHello(t, conn, nil))
	require.NoError(t, ReadSuccess(t, conn))

	run := func(query string) {
		t.Helper()
		require.NoError(t, SendRun(t, conn, query, nil, nil))
		require.NoError(t, ReadSuccess(t, conn))
		require.NoError(t, SendPull(t, conn, nil))
		require.NoError(t, ReadSuccess(t, conn))
	}
	run("CREATE (:Doc {name: 'auto'})")

	require.NoError(t, SendBegin(t, conn, nil))
	require.NoError(t, ReadSuccess(t, conn))
	run("CREATE (:Doc {name: 'explicit'})")
	require.NoError(t, SendCommit(t, conn))
	require.NoError(t, ReadSuccess(t, conn))

	result, err := dbExec.Execute(context.Background(), "MATCH (n:Doc) RETURN n.name, n.stamp ORDER BY n.name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"auto", "stamp"}, {"explicit", "stamp"}}, result.Rows)
}
