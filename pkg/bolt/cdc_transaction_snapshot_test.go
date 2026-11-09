package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Over Bolt, a change feed read in an explicit transaction lists only the
// changes its snapshot holds: commits another connection makes after BEGIN
// are not listed, and every listed node is found by the same statement.
// A transaction begun afterwards lists and finds them all.
func TestBoltChangeDataCaptureQueryIsBoundedByTheTransactionSnapshot(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	capture, ok := storage.ChangeCaptureOf(store)
	require.True(t, ok)
	capture.SetChangeCapture("test", storage.ChangeCaptureFull)
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	reader, writer := openBoltTestConn(t, port), openBoltTestConn(t, port)

	start := runBoltQueryWithParams(t, writer, "CALL db.cdc.current() YIELD id RETURN id", nil)[0][0].(string)
	runBoltStatementNoRecords(t, writer, BuildRunMessage("UNWIND range(1, 20) AS i CREATE (:Before {i: i})", nil, nil))

	requireNoError(t, SendBegin(t, reader, nil))
	requireNoError(t, ReadSuccess(t, reader))
	for i := 0; i < 30; i++ {
		runBoltStatementNoRecords(t, writer, BuildRunMessage("CREATE (:After {i: $i})", map[string]any{"i": int64(i)}, nil))
	}
	const resolve = "CALL db.cdc.query($from) YIELD event WITH event OPTIONAL MATCH (n) WHERE elementId(n) = event.elementId " +
		"RETURN count(event) AS listed, count(n) AS found"
	rows := runBoltQueryWithParams(t, reader, resolve, map[string]any{"from": start})
	require.Equal(t, [][]any{{int64(20), int64(20)}}, rows)
	requireNoError(t, SendRollback(t, reader))
	requireNoError(t, ReadSuccess(t, reader))

	requireNoError(t, SendBegin(t, reader, nil))
	requireNoError(t, ReadSuccess(t, reader))
	rows = runBoltQueryWithParams(t, reader, resolve, map[string]any{"from": start})
	require.Equal(t, [][]any{{int64(50), int64(50)}}, rows)
	requireNoError(t, SendCommit(t, reader))
	requireNoError(t, ReadSuccess(t, reader))
}
