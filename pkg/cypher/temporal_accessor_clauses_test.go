package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every date.<component> and datetime.<component> accessor is a known
// function and gives the same value in every clause and in both
// evaluators, with or without a plugin lookup (#1105). Values for
// date('2024-03-15') and datetime('2024-03-15T10:20:30Z').
func TestTemporalAccessorsBehaveAlikeInEveryClause(t *testing.T) {
	values := map[string][2]int64{
		"year": {2024, 2024}, "month": {3, 3}, "day": {15, 15},
		"hour": {0, 10}, "minute": {0, 20}, "second": {0, 30},
		"week": {11, 11}, "quarter": {1, 1}, "dayOfWeek": {5, 5},
		"dayOfYear": {75, 75}, "ordinalDay": {75, 75}, "weekYear": {2024, 2024},
	}
	clauses := []string{
		"RETURN %[1]s AS v",
		"MATCH (n:TA) RETURN %[1]s AS v",
		"RETURN CASE WHEN true THEN %[1]s END AS v",
		"RETURN [x IN [1] | %[1]s][0] AS v",
		"WITH %[1]s AS v RETURN v",
		"UNWIND [%[1]s] AS v RETURN v",
		"WITH 1 AS x WHERE %[1]s = %[2]d RETURN %[2]d AS v",
		"UNWIND [1] AS x RETURN %[2]d AS v ORDER BY %[1]s",
		"CALL { RETURN %[1]s AS v } RETURN v",
		"MATCH (n:TA) SET n.v = %[1]s RETURN n.v AS v",
	}
	for _, mode := range []struct {
		name   string
		lookup func(string) (interface{}, bool)
	}{
		{"without plugin lookup", nil},
		{"with plugin lookup", func(string) (interface{}, bool) { return nil, false }},
	} {
		t.Run(mode.name, func(t *testing.T) {
			previous := PluginFunctionLookup
			PluginFunctionLookup = mode.lookup
			t.Cleanup(func() { PluginFunctionLookup = previous })
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_accessors"))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:TA)", nil)
			require.NoError(t, err)
			for component, want := range values {
				for index, call := range []string{
					fmt.Sprintf("date.%s(date('2024-03-15'))", component),
					fmt.Sprintf("datetime.%s(datetime('2024-03-15T10:20:30Z'))", component),
				} {
					for _, clause := range clauses {
						query := fmt.Sprintf(clause, call, want[index])
						result, err := exec.Execute(ctx, query, nil)
						require.NoError(t, err, query)
						require.Equal(t, [][]interface{}{{want[index]}}, result.Rows, query)
					}
					// Both evaluators read the accessor alike.
					fixtureAdmission873Context, fixtureAdmission873 := expressionTestContext(ctx, call)
					_ = fixtureAdmission873
					require.Equal(t, want[index], exec.evaluateExpressionWithContext(fixtureAdmission873, fixtureAdmission873Context, call, nil, nil), call)
					fixtureAdmission874Context, fixtureAdmission874 := expressionTestRow(context.Background(), call, nil)
					_ = fixtureAdmission874
					value, ok, err := exec.evaluateRowValue(fixtureAdmission874, call, fixtureAdmission874Context)
					require.NoError(t, err, call)
					require.True(t, ok, call)
					require.Equal(t, want[index], value, call)
				}
			}
			// A value that isn't temporal gives null in both.
			fixtureAdmission875Context, fixtureAdmission875 := expressionTestContext(ctx, "date.year(1)")
			_ = fixtureAdmission875
			require.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission875, fixtureAdmission875Context, "date.year(1)", nil, nil))
			fixtureAdmission876Context, fixtureAdmission876 := expressionTestRow(context.Background(), "date.year(1)", nil)
			_ = fixtureAdmission876
			value, ok, err := exec.evaluateRowValue(fixtureAdmission876, "date.year(1)", fixtureAdmission876Context)
			require.NoError(t, err)
			require.True(t, ok)
			require.Nil(t, value)
		})
	}
	// The minimum date reads as itself, not as a failed parse (Go's zero
	// time is 0001-01-01T00:00:00Z).
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_accessors_min"))
	for _, call := range []string{"date.year('0001-01-01')", "datetime.year('0001-01-01T00:00:00Z')", "date.day('0001-01-01')"} {
		fixtureAdmission877Context, fixtureAdmission877 := expressionTestContext(context.Background(), call)
		_ = fixtureAdmission877
		require.Equal(t, int64(1), exec.evaluateExpressionWithContext(fixtureAdmission877, fixtureAdmission877Context, call, nil, nil), call)
		fixtureAdmission878Context, fixtureAdmission878 := expressionTestRow(context.Background(), call, nil)
		_ = fixtureAdmission878
		value, ok, err := exec.evaluateRowValue(fixtureAdmission878, call, fixtureAdmission878Context)
		require.NoError(t, err, call)
		require.True(t, ok, call)
		require.Equal(t, int64(1), value, call)
	}
	fixtureAdmission879Context, fixtureAdmission879 := expressionTestContext(context.Background(), "date.year('not a date')")
	_ = fixtureAdmission879
	require.Nil(t, exec.evaluateExpressionWithContext(fixtureAdmission879, fixtureAdmission879Context, "date.year('not a date')", nil, nil))
	// A component that isn't an accessor is not one (date.truncate is a
	// function of its own; the unknown-function check rejects the rest).
	value, known := temporalComponent("truncate", "2024-03-15")
	require.False(t, known)
	require.Nil(t, value)
}
