package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A Bolt client's tx_metadata is its transaction's metadata, on BEGIN for an
// explicit transaction and on RUN for an auto-commit one, so change events
// carry it.
func TestBoltTxMetadataIsTheTransactionMetadata(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	capture, ok := storage.ChangeCaptureOf(store)
	require.True(t, ok)
	capture.SetChangeCapture("test", storage.ChangeCaptureFull)
	executor := cypher.NewStorageExecutor(store)
	session := newTestSession(&mockConn{}, &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: executor},
	})

	require.NoError(t, session.handleBegin(encodePackStreamMap(map[string]any{"tx_metadata": map[string]any{"writer": "explicit"}})))
	require.True(t, session.inTransaction)
	require.NoError(t, session.handleRun(buildRunMessageData("CREATE (:N {v: 1})", nil, nil)))
	require.NoError(t, session.handleCommit(nil))
	require.NoError(t, session.handleRun(buildRunMessageData("CREATE (:N {v: 2})", nil, map[string]any{"tx_metadata": map[string]any{"writer": "auto"}})))
	require.NoError(t, session.handlePull(nil))

	result, err := executor.Execute(context.Background(), "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD metadata, event RETURN event.state.after.properties.v AS v, metadata.txMetadata AS tx ORDER BY v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{int64(1), map[string]interface{}{"writer": "explicit"}},
		{int64(2), map[string]interface{}{"writer": "auto"}},
	}, result.Rows)
}
