package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"io"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSearchStreamLifecycleBolt covers both stream-lifecycle owners over Bolt:
// sequential durable-qid continuation reuse against a live TCP server, and slot
// release when the connection fails before a record is ever delivered. Each
// subtest builds its own store, search service and continuation registry, so
// the stream-slot accounting of the disconnect path cannot bleed into the
// sequential-continuation population.
func TestSearchStreamLifecycleBolt(t *testing.T) {
	// sequential_continuation closes and re-starts an id continuation four
	// times against a MaxStreams=1/MaxStreamsPerOwner=1 registry, proving each
	// completed pull releases its slot for the next RUN.
	t.Run("sequential_continuation", func(t *testing.T) {
		base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { require.NoError(t, base.Close()) })
		store := storage.NewNamespacedEngine(base, "nornic")
		for _, id := range []storage.NodeID{"a", "b"} {
			_, err := store.CreateNode(&storage.Node{ID: id})
			require.NoError(t, err)
		}
		server, port := startBoltIntegrationServer(t, store)
		svc := search.NewService(store, search.DefaultVectorDimensions, nil)
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
	})

	// disconnect_before_delivery drives RUN through a session whose connection
	// write fails, then proves the failed flush released the registry's only
	// stream slot by starting a replacement stream under a different owner.
	t.Run("disconnect_before_delivery", func(t *testing.T) {
		base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
		t.Cleanup(func() { require.NoError(t, base.Close()) })
		store := storage.NewNamespacedEngine(base, "nornic")
		for _, id := range []storage.NodeID{"a", "b"} {
			_, err := store.CreateNode(&storage.Node{ID: id})
			require.NoError(t, err)
		}
		svc := search.NewService(store, search.DefaultVectorDimensions, nil)
		t.Cleanup(func() { require.NoError(t, svc.Close()) })
		registry, err := resultstream.NewRegistry(resultstream.Config{MaxStreams: 1})
		require.NoError(t, err)
		t.Cleanup(registry.Close)
		svc.SetContinuationRegistry(registry)
		exec := cypher.NewStorageExecutor(store)
		exec.SetSearchService(svc)
		session := newTestSession(&errorConn{writeErr: io.ErrClosedPipe}, &cypherQueryExecutor{executor: exec})
		session.connCtx = resultstream.WithDelivery(context.Background(), &session.continuationDelivery)
		err = session.handleRun(buildRunMessageData("CALL db.retrieve({mode: 'id', n: 1})", nil, nil))
		require.ErrorIs(t, err, io.ErrClosedPipe)
		_, err = svc.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), search.SearchContinuationRequest{Owner: "replacement", Mode: search.SearchContinuationID, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
		require.NoError(t, err, "failed Bolt flush must release the only stream slot")
	})
}
