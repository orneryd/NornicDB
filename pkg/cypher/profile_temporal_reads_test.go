package cypher

// PROFILE covers the reads the two temporal procedures perform.
// db.temporal.assertNoOverlap and db.temporal.asOf each read one label
// population, and asOf asks one temporal index lookup before it falls back to
// that read. A reviewed registration must state those reads - the nodes the
// label read handed over, the nodes a snapshot-visible read returned for the
// statement's system time, and the lookup's own outcome - and a replacement of
// the same name must stay opaque.
//
// Each case runs through the public entry point on both parsers, once
// unobserved, once with the collector PROFILE's owner installs in the context,
// and once under PROFILE itself: the rows are the same statement's rows, and
// the record is the population the reader returned, never the row the procedure
// admits from it.

import (
	"strconv"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// temporalProfileSeed is three :Doc versions of two temporal keys: k1 opened in
// 2020 and replaced in 2022, k2 opened in 2021. The label read returns three
// nodes for a procedure that admits one row, so no record can be satisfied by
// the row the procedure publishes.
const temporalProfileSeed = "CREATE (:Doc {key: 'k1', valid_from: '2020-01-01T00:00:00Z', valid_to: '2022-01-01T00:00:00Z', v: 'A'}), " +
	"(:Doc {key: 'k1', valid_from: '2022-01-01T00:00:00Z', v: 'B'}), " +
	"(:Doc {key: 'k2', valid_from: '2021-01-01T00:00:00Z', v: 'C'})"

// temporalSnapshotNamespace is the database the snapshot fixture writes to, so
// the fixture can name the stored node whose head is the statement's version.
const temporalSnapshotNamespace = "temporal_snapshot_profile"

type procedureProfileRows func(*testing.T, [][]interface{}) [][]interface{}

// profiledProcedureStatement runs one procedure statement through the public
// entry point unobserved, with the collector PROFILE's owner installs in the
// context, and under PROFILE itself. The three runs must publish the same
// columns and semantic rows. Exact comparison is the default; a call-site
// normalizer must independently validate any execution-specific metric it omits.
// Returned rows are unchanged, and collector records are the reads in the plan.

func profiledProcedureStatement(t *testing.T, exec *StorageExecutor, statement string, params map[string]interface{}, normalize ...procedureProfileRows) ([][]interface{}, *profileRuntimeCollector, *ExecutionPlan) {
	t.Helper()
	require.LessOrEqual(t, len(normalize), 1)
	comparisonRows := func(rows [][]interface{}) [][]interface{} {
		if len(normalize) == 0 {
			return rows
		}
		return normalize[0](t, rows)
	}
	plain, err := exec.Execute(t.Context(), statement, params)
	require.NoError(t, err, statement)
	require.NotNil(t, plain, statement)
	plainRows := comparisonRows(plain.Rows)
	collector := newProfileRuntimeCollector()
	observed, err := exec.Execute(withProfileRuntimeCollector(t.Context(), collector), statement, params)
	require.NoError(t, err, statement)
	require.Equal(t, plain.Columns, observed.Columns, statement)
	require.Equal(t, plainRows, comparisonRows(observed.Rows), "an observed statement must return the same semantic rows: %q", statement)
	profiled, err := exec.Execute(t.Context(), "PROFILE "+statement, params)
	require.NoError(t, err, statement)
	require.Equal(t, plain.Columns, profiled.Columns, statement)
	require.Equal(t, plainRows, comparisonRows(profiled.Rows), "PROFILE must return the statement's semantic rows: %q", statement)
	return profiled.Rows, collector, executionPlanFromResult(t, profiled)
}

// temporalIndexProfileExecutor returns an executor over the temporal fixture
// with the TEMPORAL NO OVERLAP constraint the procedure's lookup reads: the
// constraint owns the temporal index, so the lookup answers the request instead
// of leaving it to the label read.
func temporalIndexProfileExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec, _ := newProfileRuntimeTestExecutor(t,
		"CREATE CONSTRAINT temporal_profile_validity FOR (d:Doc) REQUIRE (d.key, d.valid_from, d.valid_to) IS TEMPORAL NO OVERLAP",
		temporalProfileSeed)
	return exec
}

// profiledProcedureRead holds one read a procedure reported to the population it
// returned: the collector PROFILE installs and the plan report the same record,
// under the same identity, and that record is the population, not the rows the
// procedure published from it.
func profiledProcedureRead(t *testing.T, collector *profileRuntimeCollector, plan *ExecutionPlan, kind, detail string, population int64) *PlanOperator {
	t.Helper()
	reads := profileAccessesOfKind(collector, kind)
	require.Len(t, reads, 1, "one %s read of %q", kind, detail)
	require.Equal(t, detail, reads[0].Detail)
	require.EqualValues(t, population, reads[0].Rows, "the population the read returned, not the rows the procedure published")
	require.EqualValues(t, population, reads[0].Hits)
	require.True(t, plan.TotalDBHitsObserved)
	require.Empty(t, plan.Unobserved)
	operator := planOperatorOfType(plan.Root, kind)
	require.NotNil(t, operator, "the read is reported as a plan operator: %s", kind)
	require.True(t, operator.RowsObserved, kind)
	require.EqualValues(t, population, operator.ActualRows, kind)
	require.EqualValues(t, population, operator.DBHits, kind)
	return operator
}

// Without a temporal index for the request, db.temporal.asOf's lookup answers
// with nothing and the label read answers the statement: both requests are
// reported, and the population the label read returned is three nodes for one
// admitted row.
func TestProfileTemporalAsOfReportsTheLookupAndLabelPopulationItReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t, temporalProfileSeed)
		rows, collector, plan := profiledProcedureStatement(t, exec,
			"CALL db.temporal.asOf('Doc', 'key', 'k1', 'valid_from', 'valid_to', '2021-06-01T00:00:00Z') YIELD node RETURN node.v AS v", nil)
		require.Equal(t, [][]interface{}{{"A"}}, rows)

		operator := profiledProcedureRead(t, collector, plan, profileKindNodeByLabelScan, "Doc", 3)
		require.NotEqual(t, int64(len(rows)), operator.ActualRows, "the record is the population read, not the admitted row")

		seek := profileAccessesOfKind(collector, profileKindTemporalNodeAsOfSeek)
		require.Len(t, seek, 1, "the lookup ran before the fallback")
		require.Equal(t, "Doc(key)", seek[0].Detail)
		require.Zero(t, seek[0].Rows, "the lookup found no temporal index and returned no version")
		require.EqualValues(t, 1, seek[0].Hits, "the lookup request happened")
		require.EqualValues(t, 4, plan.TotalDBHits, "the lookup and the three nodes it fell back to")
	})
}

// db.temporal.assertNoOverlap reads the label population and no temporal lookup:
// the registry mode reads the current view, so the constraint index is not
// consulted and no seek is claimed.
func TestProfileTemporalAssertNoOverlapReportsTheLabelPopulationItReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t, temporalProfileSeed)
		rows, collector, plan := profiledProcedureStatement(t, exec,
			"CALL db.temporal.assertNoOverlap('Doc', 'key', 'valid_from', 'valid_to', 'k1', '2019-01-01T00:00:00Z', '2020-01-01T00:00:00Z') YIELD ok RETURN ok", nil)
		require.Equal(t, [][]interface{}{{true}}, rows)

		profiledProcedureRead(t, collector, plan, profileKindNodeByLabelScan, "Doc", 3)
		require.Empty(t, profileAccessesOfKind(collector, profileKindTemporalNodeAsOfSeek),
			"assertNoOverlap asks no temporal lookup")
		require.EqualValues(t, 3, plan.TotalDBHits)
	})
}

// The temporal index answers the lookup itself, so the statement reads one
// version and no label population: reporting the fallback's population here
// would claim a read db.temporal.asOf did not perform.
func TestProfileTemporalAsOfReportsTheIndexLookupThatAnsweredIt(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := temporalIndexProfileExecutor(t)
		rows, collector, plan := profiledProcedureStatement(t, exec,
			"CALL db.temporal.asOf('Doc', 'key', 'k1', 'valid_from', 'valid_to', '2021-06-01T00:00:00Z') YIELD node RETURN node.v AS v", nil)
		require.Equal(t, [][]interface{}{{"A"}}, rows)

		read := profiledProcedureRead(t, collector, plan, profileKindTemporalNodeAsOfSeek, "Doc(key)", 1)
		require.Equal(t, int64(len(rows)), read.ActualRows, "the lookup returned the version the statement published")
		require.Empty(t, profileAccessesOfKind(collector, profileKindNodeByLabelScan),
			"the answered lookup never falls back to the label read")
		require.Nil(t, planOperatorOfType(plan.Root, profileKindNodeByLabelScan))
		require.EqualValues(t, 1, plan.TotalDBHits)
	})
}

// A statement that names a system time reads the snapshot-visible label
// population: a different request from the label's current nodes, recorded
// under its own kind and never as the current read.
func TestProfileTemporalAsOfReportsTheSnapshotVisiblePopulationItReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		store := storage.NewNamespacedEngine(engine, temporalSnapshotNamespace)
		exec := NewStorageExecutor(store)
		for _, node := range []*storage.Node{
			{ID: "doc-a", Labels: []string{"Doc"}, Properties: map[string]interface{}{
				"key": "k1", "valid_from": "2020-01-01T00:00:00Z", "valid_to": "2022-01-01T00:00:00Z", "v": "A",
			}},
			{ID: "doc-b", Labels: []string{"Doc"}, Properties: map[string]interface{}{
				"key": "k1", "valid_from": "2022-01-01T00:00:00Z", "v": "B",
			}},
			{ID: "doc-c", Labels: []string{"Doc"}, Properties: map[string]interface{}{
				"key": "k2", "valid_from": "2021-01-01T00:00:00Z", "v": "C",
			}},
		} {
			_, err := store.CreateNode(t.Context(), node)
			require.NoError(t, err)
		}
		// The last created node's head is the statement's version: every node
		// the fixture created before it is visible at that cut.
		head, err := engine.GetNodeCurrentHead(t.Context(), storage.NodeID(temporalSnapshotNamespace+":doc-c"))
		require.NoError(t, err)
		systemTime := head.Version.CommitTimestamp.UTC().Format(time.RFC3339Nano)
		systemSequence := strconv.FormatUint(head.Version.CommitSequence, 10)

		rows, collector, plan := profiledProcedureStatement(t, exec,
			"CALL db.temporal.asOf('Doc', 'key', 'k1', 'valid_from', 'valid_to', '2021-06-01T00:00:00Z', '"+systemTime+"', "+systemSequence+") YIELD node RETURN node.v AS v", nil)
		require.Equal(t, [][]interface{}{{"A"}}, rows)

		profiledProcedureRead(t, collector, plan, profileKindNodeByLabelVisibleAtScan, "Doc", 3)
		require.Empty(t, profileAccessesOfKind(collector, profileKindNodeByLabelScan),
			"a snapshot statement reads the historical view, not the current nodes")
		require.Empty(t, profileAccessesOfKind(collector, profileKindTemporalNodeAsOfSeek),
			"a snapshot statement never asks the current temporal lookup")
		require.EqualValues(t, 3, plan.TotalDBHits)
	})
}

// An empty label is an observed zero, not a missing measurement: both
// procedures report the population they read, so the statement's hit total
// stays certified.
func TestProfileTemporalProceduresReportAnObservedEmptyPopulation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t, temporalProfileSeed)

		rows, collector, plan := profiledProcedureStatement(t, exec,
			"CALL db.temporal.asOf('Missing', 'key', 'k1', 'valid_from', 'valid_to', '2021-06-01T00:00:00Z') YIELD node RETURN node.v AS v", nil)
		require.Empty(t, rows)
		profiledProcedureRead(t, collector, plan, profileKindNodeByLabelScan, "Missing", 0)
		seek := profileAccessesOfKind(collector, profileKindTemporalNodeAsOfSeek)
		require.Len(t, seek, 1)
		require.Zero(t, seek[0].Rows)
		require.EqualValues(t, 1, seek[0].Hits)
		require.EqualValues(t, 1, plan.TotalDBHits)

		rows, collector, plan = profiledProcedureStatement(t, exec,
			"CALL db.temporal.assertNoOverlap('Missing', 'key', 'valid_from', 'valid_to', 'k1', '2030-01-01T00:00:00Z', '2031-01-01T00:00:00Z') YIELD ok RETURN ok", nil)
		require.Equal(t, [][]interface{}{{true}}, rows)
		profiledProcedureRead(t, collector, plan, profileKindNodeByLabelScan, "Missing", 0)
		require.Zero(t, plan.TotalDBHits, "the empty population is the whole statement")
	})
}

// A procedure that reads its population and then fails still reports the same
// failure with and without PROFILE: the observation adds no row and hides no
// cause.
func TestProfileTemporalProcedureFailuresKeepTheirCause(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, test := range []struct {
			name      string
			statement string
			cause     string
		}{
			{
				name:      "asOf argument",
				statement: "CALL db.temporal.asOf('Doc', 'key', 'k1', 'valid_from', 'valid_to', 'not a time') YIELD node RETURN node.v AS v",
				cause:     "asOf must be a valid datetime",
			},
			{
				name:      "overlapping interval",
				statement: "CALL db.temporal.assertNoOverlap('Doc', 'key', 'valid_from', 'valid_to', 'k1', '2021-01-01T00:00:00Z', '2021-02-01T00:00:00Z') YIELD ok RETURN ok",
				cause:     "temporal overlap detected for key=k1",
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				exec, _ := newProfileRuntimeTestExecutor(t, temporalProfileSeed)
				plain, plainErr := exec.Execute(t.Context(), test.statement, nil)
				require.ErrorContains(t, plainErr, test.cause)
				if plain != nil {
					require.Empty(t, plain.Rows, "a failed procedure call publishes no partial rows")
				}
				profiled, profiledErr := exec.Execute(t.Context(), "PROFILE "+test.statement, nil)
				require.ErrorContains(t, profiledErr, test.cause)
				if profiled != nil {
					require.Empty(t, profiled.Rows, "PROFILE publishes no partial rows either")
				}
				require.Equal(t, plainErr.Error(), profiledErr.Error(), "PROFILE reports the statement's own failure")
			})
		}
	})
}
