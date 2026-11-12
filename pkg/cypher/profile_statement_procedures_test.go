package cypher

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileStatementProceduresObserveExecutedReaders(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct{ name, query string }{
			{"run", "CALL apoc.cypher.run($body,{})"},
			{"doIt", "CALL apoc.cypher.doIt($body,{})"},
			{"doitall", "CALL apoc.cypher.doitall($body,{})"},
			{"runMany", "CALL apoc.cypher.runMany($body,{})"},
			{"iterate", "CALL apoc.periodic.iterate($body,'RETURN 1 AS v',{batchSize:1})"},
			{"rock_n_roll", "CALL apoc.periodic.rock_n_roll($body,'RETURN 1 AS v',{batchSize:1})"},
			{"commit", "CALL apoc.periodic.commit('MATCH (n:ObservedDynamic) RETURN 0 AS count',{})"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:ObservedDynamic {v:1}),(:ObservedDynamic {v:2})")
				var normalize []procedureProfileRows
				if tc.name == "runMany" {
					normalize = append(normalize, runManySemanticProfileRows)
				}
				rows, _, plan := profiledProcedureStatement(t, exec, tc.query, map[string]any{"body": "MATCH (n:ObservedDynamic) RETURN n.v AS v, 42 AS time ORDER BY v"}, normalize...)
				if tc.name == "runMany" {
					require.Len(t, rows, 3)
					require.Equal(t, []interface{}{int64(0), map[string]interface{}{"v": int64(1), "time": int64(42)}}, rows[0])
					require.Equal(t, []interface{}{int64(1), map[string]interface{}{"v": int64(2), "time": int64(42)}}, rows[1])
				}
				require.NotEmpty(t, rows)
				require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
				require.Empty(t, plan.Unobserved)
				require.EqualValues(t, 2, plan.TotalDBHits)
			})
		}
	})
}

// Only runMany's row=-1 summary carries elapsed wall time. Validate that exact
// field on each execution, then compare every other summary and data field.
// Caller data named time remains untouched, and returned rows are not mutated.
func runManySemanticProfileRows(t *testing.T, rows [][]interface{}) [][]interface{} {
	t.Helper()
	normalized := slices.Clone(rows)
	summaries := 0
	for i, row := range rows {
		require.Len(t, row, 2)
		if row[0] != int64(-1) {
			continue
		}
		summaries++
		stats, ok := row[1].(map[string]interface{})
		require.True(t, ok)
		elapsed, ok := stats["time"].(int64)
		require.True(t, ok, "elapsed milliseconds retain their integer contract")
		require.GreaterOrEqual(t, elapsed, int64(0))
		stable := maps.Clone(stats)
		delete(stable, "time")
		normalized[i] = []interface{}{row[0], stable}
	}
	require.Equal(t, 1, summaries, "default runMany returns its summary")
	return normalized
}

func TestRunManyProfileComparisonPreservesDataAndSummaryFields(t *testing.T) {
	rows := [][]interface{}{
		{int64(0), map[string]interface{}{"time": int64(42), "v": int64(1)}},
		{int64(-1), map[string]interface{}{"time": int64(0), "rows": int64(1), "nodesCreated": int64(0)}},
	}
	other := [][]interface{}{
		{int64(0), map[string]interface{}{"time": int64(42), "v": int64(1)}},
		{int64(-1), map[string]interface{}{"time": int64(3), "rows": int64(1), "nodesCreated": int64(0)}},
	}
	stable := runManySemanticProfileRows(t, rows)
	require.Equal(t, stable, runManySemanticProfileRows(t, other), "independent elapsed values may differ")
	require.Equal(t, int64(0), rows[1][1].(map[string]interface{})["time"], "comparison never mutates its source")
	other[0][1].(map[string]interface{})["time"] = int64(43)
	require.NotEqual(t, stable, runManySemanticProfileRows(t, other), "user data named time must still differ")
	other[0][1].(map[string]interface{})["time"] = int64(42)
	other[1][1].(map[string]interface{})["nodesCreated"] = int64(1)
	require.NotEqual(t, stable, runManySemanticProfileRows(t, other), "semantic summary counters must still differ")
}
