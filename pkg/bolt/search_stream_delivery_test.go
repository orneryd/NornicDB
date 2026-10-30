package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"bufio"
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestSearchStreamDeliveryBoltFailedPullAfterRunSuccess(t *testing.T) {
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
	conn := &errorConn{}
	session := newTestSession(conn, &cypherQueryExecutor{executor: exec})
	session.connCtx = resultstream.WithDelivery(context.Background(), &session.continuationDelivery)
	require.NoError(t, session.handleRun(buildRunMessageData("CALL db.retrieve({mode: 'id', n: 1})", nil, nil)))
	require.NotEmpty(t, conn.writeData, "RUN SUCCESS was sent before PULL")
	conn.writeErr = io.ErrClosedPipe
	require.ErrorIs(t, session.handlePull(encodePackStreamMap(map[string]any{"n": int64(-1)})), io.ErrClosedPipe)
	session.continuationDelivery.Fail() // handleConnection's deferred disconnect cleanup
	_, err = svc.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), search.SearchContinuationRequest{Owner: "replacement", Mode: search.SearchContinuationID, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err, "failed page PULL after successful RUN must release the only slot")
}

func newPageDeliverySession(t *testing.T, capacity int) (*Session, *errorConn, *search.Service) {
	t.Helper()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := store.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"content": "transport boundary"}})
		require.NoError(t, err)
	}
	svc := search.NewService(store, search.DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	registry, err := resultstream.NewRegistry(resultstream.Config{MaxStreams: int64(capacity)})
	require.NoError(t, err)
	t.Cleanup(registry.Close)
	svc.SetContinuationRegistry(registry)
	exec := cypher.NewStorageExecutor(store)
	exec.SetSearchService(svc)
	conn := &errorConn{}
	session := newTestSession(conn, &cypherQueryExecutor{executor: exec})
	session.connCtx = resultstream.WithDelivery(context.Background(), &session.continuationDelivery)
	return session, conn, svc
}
func startReplacementPage(svc *search.Service) (*search.SearchContinuationPage, error) {
	return svc.SearchTextContinuation(context.Background(), "", search.DefaultSearchOptions(), search.SearchContinuationRequest{Owner: "replacement", Mode: search.SearchContinuationID, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
}
func TestSearchStreamDeliveryBoltBoundaries(t *testing.T) {
	for _, point := range []string{"disconnect_after_run", "pull_write", "buffer_write", "pipeline_write", "discard", "reset", "success"} {
		t.Run(point, func(t *testing.T) {
			session, conn, svc := newPageDeliverySession(t, 1)
			if point == "pipeline_write" {
				session.messageQueue = make(chan *boltMessage, 1)
				session.messageQueue <- &boltMessage{msgType: MsgPull}
			}
			require.NoError(t, session.handleRun(buildRunMessageData("CALL db.retrieve({mode:'id',n:1})", nil, nil)))
			qid := session.lastResult.Rows[0][0].(map[string]any)["qid"].(string)
			switch point {
			case "pull_write", "buffer_write", "pipeline_write":
				if point == "buffer_write" {
					session.writer = bufio.NewWriterSize(conn, 16)
				}
				conn.writeErr = io.ErrClosedPipe
				require.ErrorIs(t, session.handlePull(encodePackStreamMap(map[string]any{"n": int64(-1)})), io.ErrClosedPipe)
			case "discard":
				require.NoError(t, session.handleDiscard(encodePackStreamMap(map[string]any{"n": int64(-1)})))
			case "reset":
				require.NoError(t, session.handleReset(nil))
			case "success":
				require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"n": int64(-1)})))
			}
			session.continuationDelivery.Fail()
			_, err := startReplacementPage(svc)
			if point == "success" {
				require.ErrorIs(t, err, resultstream.ErrCapacity, "a delivered page survives ordinary disconnect")
				_, err = svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "embedded", QID: qid, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
				require.NoError(t, err)
			} else {
				require.NoError(t, err, "undelivered/discarded page must release admission")
			}
		})
	}
}
func TestSearchStreamDeliveryBoltIndependentRecords(t *testing.T) {
	for _, kind := range []string{"partial_pull", "outstanding_statements"} {
		t.Run(kind, func(t *testing.T) {
			session, _, svc := newPageDeliverySession(t, 2)
			var firstQID string
			if kind == "outstanding_statements" {
				require.NoError(t, session.dispatchInner(MsgBegin, encodePackStreamMap(map[string]any{}), "BEGIN", nil))
				for i := 0; i < 2; i++ {
					require.NoError(t, session.handleRun(buildRunMessageData("CALL db.retrieve({mode:'id',n:1})", nil, nil)))
					if i == 0 {
						firstQID = session.lastResult.Rows[0][0].(map[string]any)["qid"].(string)
					}
				}
			} else {
				require.NoError(t, session.handleRun(buildRunMessageData("UNWIND [1,2] AS i CALL db.retrieve({mode:'id',n:1,repetition:i}) YIELD page RETURN page", nil, nil)))
				require.Len(t, session.lastResult.Rows, 2)
				firstQID = session.lastResult.Rows[0][0].(map[string]any)["qid"].(string)
			}
			require.NoError(t, session.handlePull(encodePackStreamMap(map[string]any{"n": int64(1), "qid": int64(0)})))
			session.continuationDelivery.Fail()
			_, err := startReplacementPage(svc)
			require.NoError(t, err, "unread record/statement must be released")
			_, err = startReplacementPage(svc)
			require.ErrorIs(t, err, resultstream.ErrCapacity, "exactly the delivered cursor and replacement remain")
			_, err = svc.SearchTextContinuation(context.Background(), "", nil, search.SearchContinuationRequest{Owner: "embedded", QID: firstQID, N: 1}, nil, nil, nil, search.ChunkedSearchErrorPolicy{})
			require.NoError(t, err, "acknowledging one record must preserve that cursor")
		})
	}
}
