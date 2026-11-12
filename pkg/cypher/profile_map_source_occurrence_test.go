package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProfileMapLiteralEntriesAreDistinctOccurrences is the public regression
// matrix for map entry values: every value is an expression written at its own
// position, so two equal scalar subqueries in one map literal are two operators,
// a nested map keeps its own entry positions, comments and Unicode between
// entries move those positions without merging them, and one written occurrence
// stays one however many rows it runs for. The rows are the maps the statement
// returned, unchanged. Every case runs cold and warm, with the compiled row
// programs both disabled and enabled, because the text reader and the compiled
// reader must report the same occurrences.
func TestProfileMapLiteralEntriesAreDistinctOccurrences(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, query string
			want        [][]interface{}
			unions      int
			executions  int64
		}{
			{
				name:       "two entries",
				query:      "RETURN {a: COUNT { RETURN 1 UNION ALL RETURN 1 }, b: COUNT { RETURN 1 UNION ALL RETURN 1 }} AS value",
				want:       [][]interface{}{{map[string]interface{}{"a": int64(2), "b": int64(2)}}},
				unions:     2,
				executions: 1,
			},
			{
				name:       "nested entry",
				query:      "RETURN {outer: {inner: COUNT { RETURN 1 UNION ALL RETURN 1 }}, other: COUNT { RETURN 1 UNION ALL RETURN 1 }} AS value",
				want:       [][]interface{}{{map[string]interface{}{"outer": map[string]interface{}{"inner": int64(2)}, "other": int64(2)}}},
				unions:     2,
				executions: 1,
			},
			{
				name:       "block comment before an entry",
				query:      "RETURN {first: 1, /* two rows */ second: COUNT { RETURN 1 UNION ALL RETURN 1 }, third: COUNT { RETURN 1 UNION ALL RETURN 1 }} AS value",
				want:       [][]interface{}{{map[string]interface{}{"first": int64(1), "second": int64(2), "third": int64(2)}}},
				unions:     2,
				executions: 1,
			},
			{
				name:       "line comment before an entry",
				query:      "RETURN {first: 1, // one row\n second: COUNT { RETURN 1 UNION ALL RETURN 1 }, third: COUNT { RETURN 1 UNION ALL RETURN 1 }} AS value",
				want:       [][]interface{}{{map[string]interface{}{"first": int64(1), "second": int64(2), "third": int64(2)}}},
				unions:     2,
				executions: 1,
			},
			{
				name:       "Unicode keys and bodies",
				query:      "RETURN {`火`: COUNT { RETURN '火' UNION ALL RETURN '火' }, `水火`: COUNT { RETURN '火' UNION ALL RETURN '火' }} AS value",
				want:       [][]interface{}{{map[string]interface{}{"火": int64(2), "水火": int64(2)}}},
				unions:     2,
				executions: 1,
			},
			{
				name:       "one entry over many rows",
				query:      "UNWIND [1, 2, 3] AS x RETURN {a: COUNT { RETURN x UNION ALL RETURN x }} AS value",
				want:       [][]interface{}{{map[string]interface{}{"a": int64(2)}}, {map[string]interface{}{"a": int64(2)}}, {map[string]interface{}{"a": int64(2)}}},
				unions:     1,
				executions: 3,
			},
			{
				name:       "two entries over many rows",
				query:      "UNWIND [1, 2, 3] AS x RETURN {a: COUNT { RETURN x UNION ALL RETURN x }, b: COUNT { RETURN x UNION ALL RETURN x }} AS value",
				want:       [][]interface{}{{map[string]interface{}{"a": int64(2), "b": int64(2)}}, {map[string]interface{}{"a": int64(2), "b": int64(2)}}, {map[string]interface{}{"a": int64(2), "b": int64(2)}}},
				unions:     2,
				executions: 3,
			},
		} {
			for _, programs := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/programs=%t", tc.name, programs), func(t *testing.T) {
					previous := rowValueProgramsDisabled.Swap(!programs)
					t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
					exec, ctx := newProfileRuntimeTestExecutor(t, "CREATE (:Seed)")
					for _, run := range []string{"cold", "warm"} {
						result, err := exec.Execute(ctx, "PROFILE "+tc.query, nil)
						require.NoError(t, err, run)
						require.Equal(t, tc.want, result.Rows, "%s: %s", run, tc.query)
						plan := executionPlanFromResult(t, result)
						unions := profileScalarUnionOperators(plan)
						require.Len(t, unions, tc.unions, "%s: %s", run, formatPlan(plan))
						for _, union := range unions {
							require.Equal(t, tc.executions, union.Arguments["executions"], "%s: %s", run, formatPlan(plan))
							require.True(t, union.RowsObserved, run)
							require.Equal(t, 2*tc.executions, union.ActualRows, "%s: %s", run, formatPlan(plan))
						}
					}
				})
			}
		}
	})
}

// mapControlRowsWithPrograms returns a query's rows with the compiled row reader
// forced on or off, restoring the reader the test found.
func mapControlRowsWithPrograms(exec *StorageExecutor, ctx context.Context, query string, compiled bool) ([][]interface{}, error) {
	previous := rowValueProgramsDisabled.Swap(!compiled)
	defer rowValueProgramsDisabled.Store(previous)
	result, err := exec.Execute(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return result.Rows, nil
}

// TestMapLiteralValuesDoNotDependOnProfile is the ordinary-statement control:
// the same map literals return the same maps with and without PROFILE and with
// the compiled reader both disabled and enabled, and no map value carries a
// position the expression reader kept. The maps hold the keys they were written
// with, not internal row state.
func TestMapLiteralValuesDoNotDependOnProfile(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t)
		plain, err := exec.Execute(ctx, "RETURN {a: 1, b: 2} AS value", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{map[string]interface{}{"a": int64(1), "b": int64(2)}}}, plain.Rows)
		for _, query := range []string{
			"UNWIND [1, 2] AS x RETURN {a: x, b: x + 1} AS value",
			"RETURN {a: COUNT { RETURN 1 UNION ALL RETURN 1 }, b: COUNT { RETURN 1 UNION ALL RETURN 1 }} AS value",
			"RETURN {outer: {inner: 1}, other: 2} AS value",
			"RETURN {first: 1, /* two rows */ second: 2, third: 3} AS value",
		} {
			plain, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			profiled, err := exec.Execute(ctx, "PROFILE "+query, nil)
			require.NoError(t, err, query)
			require.Equal(t, plain.Rows, profiled.Rows, query)
			for _, compiled := range []bool{false, true} {
				rows, err := mapControlRowsWithPrograms(exec, ctx, query, compiled)
				require.NoError(t, err, query)
				require.Equal(t, plain.Rows, rows, "%s: compiled=%t", query, compiled)
			}
			for _, row := range plain.Rows {
				value, ok := row[0].(map[string]interface{})
				require.True(t, ok, query)
				for key := range value {
					require.False(t, strings.HasPrefix(key, "\x00"), "%s: map value holds %q", query, key)
				}
			}
		}
	})
}

// TestMapEntryValueStartsAreWrittenBytes pins the sidecar's own contract: a
// reported start is the byte where the value is written in the text the reader
// was given. The assertion is the source text itself, so comments that
// StripComments shortens, repeated equal values and multi-byte text all have to
// name their own bytes.
func TestMapEntryValueStartsAreWrittenBytes(t *testing.T) {
	for _, text := range []string{
		"{first: 1, /* two rows */ second: COUNT { RETURN 1 UNION ALL RETURN 1 }, `火`: 2}",
		"{a: COUNT { RETURN 1 UNION ALL RETURN 1 }, b: COUNT { RETURN 1 UNION ALL RETURN 1 }}",
		"{\n  a: 1,\n  // one row\n  b: 2\n}",
		"{`火`:　COUNT { RETURN 1 UNION ALL RETURN 1 }}",
	} {
		var values []string
		var starts []int
		require.NoError(t, forEachMapEntry(text, mapSourceText, func(_ string, value string) {
			values = append(values, value)
		}, &starts), text)
		require.NotEmpty(t, values, text)
		require.Len(t, starts, len(values), text)
		for index, value := range values {
			start := starts[index]
			require.GreaterOrEqual(t, start, 0, text)
			require.LessOrEqual(t, start+len(value), len(text), text)
			require.Equal(t, value, text[start:start+len(value)], "%s: entry %d", text, index)
		}
		if len(starts) > 1 {
			require.NotEqual(t, starts[0], starts[1], text)
		}
	}
}
