package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestTemporalMapFieldsMatchNeo4j: a temporal constructor's map fails with
// Neo4j 5.26.30's code and message for an out-of-range field, a missing
// field, a day its month doesn't have and a mix of date forms, instead of
// rolling over; a field a base value supplies is clamped.
func TestTemporalMapFieldsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	const argument = "Neo.ClientError.Statement.ArgumentError"
	const typeError = "Neo.ClientError.Statement.TypeError"
	failures := []struct{ query, code, message string }{
		{"RETURN date({year: 2020, month: 13}) AS d", argument, "Invalid value for MonthOfYear (valid values 1 - 12): 13"},
		{"RETURN date({year: 2020, month: 0}) AS d", argument, "Invalid value for MonthOfYear (valid values 1 - 12): 0"},
		{"RETURN date({year: 2020, month: 2, day: 30}) AS d", argument, "Invalid date 'FEBRUARY 30'"},
		{"RETURN date({year: 2020, month: 4, day: 31}) AS d", argument, "Invalid date 'APRIL 31'"},
		{"RETURN date({year: 2021, month: 2, day: 29}) AS d", argument, "Invalid date 'February 29' as '2021' is not a leap year"},
		{"RETURN date({year: 2020, month: 1, day: 0}) AS d", argument, "Invalid value for DayOfMonth (valid values 1 - 28/31): 0"},
		{"RETURN date({year: 2020, week: 54}) AS d", argument, "Invalid value for WeekOfWeekBasedYear (valid values 1 - 52/53): 54"},
		{"RETURN date({year: 2020, week: 1, dayOfWeek: 8}) AS d", argument, "Invalid value for DayOfWeek (valid values 1 - 7): 8"},
		{"RETURN date({year: 2020, quarter: 5}) AS d", argument, "Invalid value for QuarterOfYear (valid values 1 - 4): 5"},
		{"RETURN date({year: 2020, quarter: 1, dayOfQuarter: 93}) AS d", argument, "Invalid value for DayOfQuarter (valid values 1 - 90/92): 93"},
		{"RETURN date({year: 2020, ordinalDay: 367}) AS d", argument, "Invalid value for DayOfYear (valid values 1 - 365/366): 367"},
		{"RETURN date({year: 2021, ordinalDay: 366}) AS d", argument, "Invalid date 'DayOfYear 366' as '2021' is not a leap year"},
		{"RETURN date({year: 1000000000}) AS d", argument, "Invalid value for Year (valid values -999999999 - 999999999): 1000000000"},
		{"RETURN date({month: 3}) AS d", argument, "year must be specified"},
		{"RETURN date({year: 2020, day: 3}) AS d", argument, "day cannot be specified without month"},
		{"RETURN date({year: 2020, dayOfWeek: 3}) AS d", argument, "dayOfWeek cannot be specified without week"},
		{"RETURN date({year: 2020, dayOfQuarter: 3}) AS d", argument, "dayOfQuarter cannot be specified without quarter"},
		{"RETURN date({year: 2020, month: 1, week: 2}) AS d", typeError, "Cannot assign month to week date."},
		{"RETURN date({year: 2020, quarter: 1, day: 3}) AS d", typeError, "Cannot assign quarter to calendar date."},
		{"RETURN date({year: 2020, ordinalDay: 3, week: 1}) AS d", typeError, "Cannot assign ordinalDay to week date."},
		{"RETURN localtime({hour: 25}) AS t", argument, "Invalid value for HourOfDay (valid values 0 - 23): 25"},
		{"RETURN localtime({hour: -1}) AS t", argument, "Invalid value for HourOfDay (valid values 0 - 23): -1"},
		{"RETURN localtime({hour: 1, minute: 60}) AS t", argument, "Invalid value for MinuteOfHour (valid values 0 - 59): 60"},
		{"RETURN localtime({hour: 1, minute: 0, second: 60}) AS t", argument, "Invalid value for SecondOfMinute (valid values 0 - 59): 60"},
		{"RETURN localtime({minute: 1}) AS t", argument, "hour must be specified"},
		{"RETURN time({hour: 1, second: 61}) AS t", argument, "second cannot be specified without minute"},
		{"RETURN localtime({hour: 1, millisecond: 5}) AS t", argument, "subsecond cannot be specified without minute"},
		{"RETURN localtime({hour: 1, minute: 0, millisecond: 5}) AS t", argument, "subsecond cannot be specified without second"},
		{"RETURN localtime({hour: 1, minute: 0, second: 0, millisecond: 1000}) AS t", argument, "Invalid value for Millisecond: 1000"},
		{"RETURN localtime({hour: 1, minute: 0, second: 0, millisecond: 5, microsecond: 1000}) AS t", argument, "Invalid value for Microsecond: 1000"},
		{"RETURN localtime({hour: 1, minute: 0, second: 0, millisecond: 5, nanosecond: 1000000}) AS t", argument, "Invalid value for Nanosecond: 1000000"},
		{"RETURN localtime({hour: 1, minute: 0, second: 0, microsecond: 5, nanosecond: 1000}) AS t", argument, "Invalid value for Nanosecond: 1000"},
		{"RETURN localtime({hour: 1, minute: 0, second: 0, nanosecond: 1000000000}) AS t", argument, "Invalid value for Nanosecond: 1000000000"},
		{"RETURN localdatetime({year: 2020, month: 1, day: 1, hour: 24}) AS t", argument, "Invalid value for HourOfDay (valid values 0 - 23): 24"},
		{"RETURN localdatetime({year: 2020, hour: 1}) AS t", argument, "month must be specified"},
		{"RETURN localdatetime({year: 2020, month: 1, hour: 1}) AS t", argument, "day must be specified"},
		{"RETURN localdatetime({year: 2020, week: 1, hour: 1}) AS t", argument, "dayOfWeek must be specified"},
		{"RETURN localdatetime({year: 2020, quarter: 1, hour: 1}) AS t", argument, "dayOfQuarter must be specified"},
		{"RETURN datetime({year: 2020, month: 13, day: 1}) AS t", argument, "Invalid value for MonthOfYear (valid values 1 - 12): 13"},
		{"RETURN datetime({year: 2020, month: 1, day: 1, minute: 5}) AS t", argument, "hour must be specified"},
		{"RETURN datetime({year: 2020, month: 1, day: 32, timezone: '+01:00'}) AS t", argument, "Invalid value for DayOfMonth (valid values 1 - 28/31): 32"},
		{"RETURN time({hour: 24, timezone: '+01:00'}) AS t", argument, "Invalid value for HourOfDay (valid values 0 - 23): 24"},
		{"RETURN date({date: date('2020-05-05'), month: 13}) AS d", argument, "Invalid value for MonthOfYear (valid values 1 - 12): 13"},
		{"RETURN localtime({time: localtime('10:00'), minute: 60}) AS t", argument, "Invalid value for MinuteOfHour (valid values 0 - 59): 60"},
	}
	for _, tc := range failures {
		_, err := exec.Execute(ctx, tc.query, nil)
		require.Error(t, err, tc.query)
		require.Contains(t, err.Error(), tc.code, tc.query)
		require.Contains(t, err.Error(), tc.message, tc.query)
	}

	values := map[string]string{
		"RETURN toString(date({year: 2020, month: 2, day: 29})) AS v":                                        "2020-02-29",
		"RETURN toString(date({year: 2020, week: 53})) AS v":                                                 "2020-12-28",
		"RETURN toString(date({year: 2021, week: 53})) AS v":                                                 "2022-01-03",
		"RETURN toString(date({year: 2020, quarter: 1, dayOfQuarter: 92})) AS v":                             "2020-04-01",
		"RETURN toString(date({date: date('2020-05-05'), day: 3})) AS v":                                     "2020-05-03",
		"RETURN toString(date({date: date('2020-05-31'), month: 2})) AS v":                                   "2020-02-29",
		"RETURN toString(date({date: date('2020-02-29'), year: 2021})) AS v":                                 "2021-02-28",
		"RETURN toString(date({date: date('2020-05-31'), quarter: 1})) AS v":                                 "2020-02-29",
		"RETURN toString(localtime({time: localtime('10:00'), second: 5})) AS v":                             "10:00:05",
		"RETURN toString(localtime({hour: 1, minute: 0, second: 0, millisecond: 5, microsecond: 999})) AS v": "01:00:00.005999",
		"RETURN toString(localdatetime({year: 2020, ordinalDay: 5, hour: 1})) AS v":                          "2020-01-05T01:00",
	}
	for query, want := range values {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}

// TestStringFunctionGraphArgumentsAreTypeErrors: a node, relationship, path
// or list of them for a string function's STRING parameter is Neo4j's
// compile-time SyntaxError, raised even when no row reaches the call.
func TestStringFunctionGraphArgumentsAreTypeErrors(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for query, typeName := range map[string]string{
		"MATCH (n:NoSuch)-[r]->() RETURN trim(r FROM 'a') AS t":      "Relationship",
		"MATCH (n:NoSuch)-[r]->() RETURN trim(BOTH r FROM 'a') AS t": "Relationship",
		"MATCH (n:NoSuch)-[r]->() RETURN trim('a' FROM r) AS t":      "Relationship",
		"MATCH (n:NoSuch)-[r]->() RETURN trim(r) AS t":               "Relationship",
		"MATCH (n:NoSuch)-[r]->() RETURN ltrim('a', r) AS t":         "Relationship",
		"MATCH (n:NoSuch)-[r]->() RETURN btrim('a', r) AS t":         "Relationship",
		"MATCH (n:NoSuch) RETURN trim(n FROM 'a') AS t":              "Node",
		"MATCH (n:NoSuch) RETURN toUpper(n) AS t":                    "Node",
		"MATCH (n:NoSuch) RETURN substring(n, 1) AS t":               "Node",
		"MATCH (n:NoSuch) RETURN replace(n, 'a', 'b') AS t":          "Node",
		"MATCH (n:NoSuch) RETURN split(n, 'a') AS t":                 "Node",
		"MATCH p = (n:NoSuch)-->() RETURN trim(p) AS t":              "Path",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError", query)
		require.Contains(t, err.Error(), "Type mismatch: expected String but was "+typeName, query)
	}
	result, err := exec.Execute(ctx, "MATCH (n:NoSuch) RETURN trim(n.name FROM 'a') AS t", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}
