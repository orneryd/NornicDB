package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"net"
	"testing"
	"time"

	cypherconfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// boltCDCQuery reads the change feed from, matching an optional node label,
// as each event's identifier and the name its node was given.
const boltCDCQuery = "CALL db.cdc.query($from, $selectors) YIELD id, event RETURN id, event.state.after.properties.name"

// boltCDCEvents runs boltCDCQuery and returns the identifiers and names, in
// feed order.
func boltCDCEvents(t *testing.T, conn net.Conn, from, label string) (ids, names []string) {
	t.Helper()
	selectors := []any{}
	if label != "" {
		selectors = append(selectors, map[string]any{"select": "n", "labels": []any{label}})
	}
	for _, row := range runBoltQueryWithParams(t, conn, boltCDCQuery, map[string]any{"from": from, "selectors": selectors}) {
		ids = append(ids, row[0].(string))
		names = append(names, row[1].(string))
	}
	return ids, names
}

// runBoltQueryExpectFailureWithParams runs query with params and returns the
// FAILURE message's code and text.
func runBoltQueryExpectFailureWithParams(t *testing.T, conn net.Conn, query string, params map[string]any) (string, string) {
	t.Helper()
	requireNoError(t, SendRun(t, conn, query, params, nil))
	code, message, err := AssertFailure(t, conn)
	requireNoError(t, err)
	return code, message
}

// Neo4j 5.26.30 Enterprise keeps the start-of-retained-feed marker
// db.cdc.earliest() returns usable after old events are pruned, while an
// ordinary event cursor of a pruned event expires. Over Bolt the marker is
// read from the wire, the storage engine's retention owner prunes the first
// write's events, and the unchanged marker still lists the second write's two
// events with their exact identifiers in commit order — through auto-commit
// and through a fresh explicit transaction.
func TestBoltChangeFeedEarliestMarkerSurvivesPruning(t *testing.T) {
	for _, parser := range []string{cypherconfig.ParserTypeNornic, cypherconfig.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := cypherconfig.GetParserType()
			cypherconfig.SetParserType(parser)
			t.Cleanup(func() { cypherconfig.SetParserType(previous) })

			base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "test")
			capture, ok := storage.ChangeCaptureOf(store)
			require.True(t, ok)
			capture.SetChangeCapture("test", storage.ChangeCaptureFull)
			_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
			writer, reader := openBoltTestConn(t, port), openBoltTestConn(t, port)
			earliest := runBoltQueryWithParams(t, writer, "CALL db.cdc.earliest() YIELD id RETURN id", nil)[0][0].(string)

			// A commits two events; B commits two later ones. Every event
			// writes its own node name, so each event is uniquely identified.
			runBoltStatementNoRecords(t, writer, BuildRunMessage("CREATE (:CdcPruned {name: 'a-1'}), (:CdcPruned {name: 'a-2'})", nil, nil))
			prunedIDs, prunedNames := boltCDCEvents(t, writer, earliest, "CdcPruned")
			require.Equal(t, []string{"a-1", "a-2"}, prunedNames)
			runBoltStatementNoRecords(t, writer, BuildRunMessage("CREATE (:CdcKept {name: 'b-1'}), (:CdcKept {name: 'b-2'})", nil, nil))
			keptIDs, keptNames := boltCDCEvents(t, writer, earliest, "CdcKept")
			require.Equal(t, []string{"b-1", "b-2"}, keptNames)
			require.Len(t, keptIDs, 2)

			// The storage engine keeps a database's last transaction's events:
			// pruning with the retention owner's cutoff expires A's two and
			// leaves B's two.
			pruned, err := base.PruneChangeEvents(t.Context(), "test", time.Now().Add(time.Minute))
			require.NoError(t, err)
			require.Equal(t, 2, pruned)

			// The unchanged marker lists exactly B's events, in order, with
			// their exact identifiers, in an auto-commit statement and in
			// db.cdc.earliest() → db.cdc.query() chained in one statement.
			require.Equal(t, earliest, runBoltQueryWithParams(t, writer, "CALL db.cdc.earliest() YIELD id RETURN id", nil)[0][0].(string))
			ids, names := boltCDCEvents(t, writer, earliest, "")
			require.Equal(t, keptIDs, ids)
			require.Equal(t, keptNames, names)
			require.Equal(t, [][]any{{keptIDs[0]}, {keptIDs[1]}},
				runBoltQueryWithParams(t, writer, "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD id AS change RETURN change", nil))

			// A fresh explicit transaction reads the same retained feed from
			// the same marker.
			requireNoError(t, SendBegin(t, reader, nil))
			requireNoError(t, ReadSuccess(t, reader))
			ids, names = boltCDCEvents(t, reader, earliest, "")
			require.Equal(t, keptIDs, ids)
			require.Equal(t, keptNames, names)
			require.Equal(t, [][]any{{keptIDs[0]}, {keptIDs[1]}},
				runBoltQueryWithParams(t, reader, "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id) YIELD id AS change RETURN change", nil))
			requireNoError(t, SendCommit(t, reader))
			requireNoError(t, ReadSuccess(t, reader))

			// A's ordinary event cursor expired with its events.
			code, _ := runBoltQueryExpectFailureWithParams(t, writer, "CALL db.cdc.query($from)", map[string]any{"from": prunedIDs[0]})
			require.Equal(t, "Neo.ClientError.ChangeDataCapture.InvalidIdentifier", code)
		})
	}
}
