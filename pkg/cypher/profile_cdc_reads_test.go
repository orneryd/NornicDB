package cypher

// PROFILE covers the reads the change data capture procedures perform: the
// feed's retained bounds, which db.cdc.current, db.cdc.earliest and
// db.cdc.query each request once, and the change events db.cdc.query iterates.
//
// Which feed a statement reads, the database's capture mode and its identity are
// metadata and are not recorded as change data reads. Each case runs through the
// public entry point on both parsers, unobserved, with the collector PROFILE
// installs in the context, and under PROFILE itself, and holds the statement to
// the population the feed returned before its selectors and limit narrowed the
// rows it published.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestProfileCDCProceduresReportTheBoundsAndEventsTheyRead is one statement per
// read: both position markers acquire the feed's bounds and iterate no event,
// and a query reads its bounds plus every event of the page before its limit or
// selectors decide which of them become rows.
func TestProfileCDCProceduresReportTheBoundsAndEventsTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
		database := exec.databaseName()
		require.NotEmpty(t, database)
		start := cdcID(t, exec, "db.cdc.current")
		for _, statement := range []string{"CREATE (:Event {i: 1})", "CREATE (:Event {i: 2})", "CREATE (:Event {i: 3})"} {
			_, err := exec.Execute(t.Context(), statement, nil)
			require.NoError(t, err, statement)
		}

		for _, marker := range []struct{ name, query string }{
			{name: "current", query: "CALL db.cdc.current() YIELD id RETURN id"},
			{name: "earliest", query: "CALL db.cdc.earliest() YIELD id RETURN id"},
		} {
			t.Run(marker.name, func(t *testing.T) {
				rows, collector, plan := profiledProcedureStatement(t, exec, marker.query, nil)
				require.Len(t, rows, 1, marker.query)
				read := profiledProcedureRead(t, collector, plan, profileKindChangeCaptureBounds, database, 1)
				require.EqualValues(t, 1, read.ActualRows, "one bounds response, not the sequence numbers it holds")
				require.EqualValues(t, 1, plan.TotalDBHits)
				require.Empty(t, profileAccessesOfKind(collector, profileKindChangeEventScan),
					"a position marker reads no change event")
			})
		}

		t.Run("limit", func(t *testing.T) {
			// One of the three events is published: the limit narrows the rows,
			// not the page the statement read.
			query := "CALL db.cdc.query('" + start + "', [], 1) YIELD id RETURN id"
			rows, collector, plan := profiledProcedureStatement(t, exec, query, nil)
			require.Len(t, rows, 1, "the limit keeps one change")
			read := profiledProcedureRead(t, collector, plan, profileKindChangeEventScan, database, 3)
			require.NotEqual(t, int64(len(rows)), read.ActualRows, "the record is the events read, not the rows the limit kept")
			require.EqualValues(t, 3, read.DBHits, "reading an event is one hit")
			profiledProcedureRead(t, collector, plan, profileKindChangeCaptureBounds, database, 1)
			require.EqualValues(t, 4, plan.TotalDBHits, "three events and the one bounds request")
		})

		t.Run("selector", func(t *testing.T) {
			// No event matches the selector, and the page is still read: the
			// population is counted where the feed returned it, before the
			// statement's selector narrows it to nothing.
			rows, collector, plan := profiledProcedureStatement(t, exec,
				"CALL db.cdc.query($from, $selectors, null) YIELD id RETURN id",
				map[string]interface{}{
					"from":      start,
					"selectors": []interface{}{map[string]interface{}{"select": "n", "operation": "d"}},
				})
			require.Empty(t, rows, "no committed change is a node delete")
			profiledProcedureRead(t, collector, plan, profileKindChangeEventScan, database, 3)
			profiledProcedureRead(t, collector, plan, profileKindChangeCaptureBounds, database, 1)
			require.EqualValues(t, 4, plan.TotalDBHits)
		})

		t.Run("empty feed after the position", func(t *testing.T) {
			// A query from the current position reads no event, and says so:
			// the empty population is an observed zero, not a missing
			// measurement, and the bounds request is the statement's one hit.
			query := "CALL db.cdc.query('" + cdcID(t, exec, "db.cdc.current") + "', [], null) YIELD id RETURN id"
			rows, collector, plan := profiledProcedureStatement(t, exec, query, nil)
			require.Empty(t, rows)
			read := profiledProcedureRead(t, collector, plan, profileKindChangeEventScan, database, 0)
			require.Zero(t, read.ActualRows)
			require.Zero(t, read.DBHits)
			profiledProcedureRead(t, collector, plan, profileKindChangeCaptureBounds, database, 1)
			require.EqualValues(t, 1, plan.TotalDBHits)
		})
	})
}

// A database that captures no change answers with the same Disabled error with
// and without PROFILE: the observation neither hides the failure nor invents a
// read the procedure did not perform.
func TestProfileCDCProcedureFailuresKeepTheirCause(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newCDCExecutor(t, storage.ChangeCaptureOff)
		for _, query := range []string{
			"CALL db.cdc.current() YIELD id RETURN id",
			"CALL db.cdc.earliest() YIELD id RETURN id",
			"CALL db.cdc.query('', [], null) YIELD id RETURN id",
		} {
			t.Run(query, func(t *testing.T) {
				plain, plainErr := exec.Execute(t.Context(), query, nil)
				require.ErrorContains(t, plainErr, "Change Data Capture is not currently enabled for database 'test'", query)
				if plain != nil {
					require.Empty(t, plain.Rows, "a disabled capture publishes no row")
				}
				profiled, profiledErr := exec.Execute(t.Context(), "PROFILE "+query, nil)
				require.Error(t, profiledErr, query)
				require.Equal(t, plainErr.Error(), profiledErr.Error(), "PROFILE reports the statement's own failure")
				if profiled != nil {
					require.Empty(t, profiled.Rows, "PROFILE publishes no row either")
				}
			})
		}
	})
}
