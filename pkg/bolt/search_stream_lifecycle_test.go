package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamLifecycleBolt(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	server, port := startBoltIntegrationServer(t, store)
	svc := search.NewService(store)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	registry, err := resultstream.NewRegistry(resultstream.Config{MaxStreams: 1, MaxStreamsPerOwner: 1})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	svc.SetContinuationRegistry(registry)
	server.executor.(*cypherQueryExecutor).executor.SetSearchService(svc)
	conn := openBoltTestConn(t, port)
	for i := 0; i < 4; i++ {
		require.NoError(t, SendRun(t, conn, "CALL db.retrieve({mode: 'id', n: 1})", nil, nil))
		metadata, err := AssertSuccess(t, conn)
		require.NoError(t, err, "sequential continuation %d", i)
		qid := metadata["durable_qid"].(string)
		records, _ := pullStatement(t, conn, map[string]any{"n": int64(-1)})
		require.Len(t, records, 1)
		require.NoError(t, SendRun(t, conn, "CALL db.retrieve($request)", map[string]any{"request": map[string]any{"qid": qid, "n": int64(1)}}, nil))
		_, err = AssertSuccess(t, conn)
		require.NoError(t, err)
		records, _ = pullStatement(t, conn, map[string]any{"n": int64(-1)})
		require.Len(t, records, 1)
		require.Equal(t, false, records[0][0].(map[string]any)["has_more"])
	}
}
