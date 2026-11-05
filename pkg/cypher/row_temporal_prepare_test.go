package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A temporal constructor is evaluated by evaluateTemporalConstructor with the
// call its caller already read (functionCallFacts) and, in a row value
// program, with its arguments compiled (rowTemporalFunctionNode). These tests
// hold the compiled form to the text path: the same value, resolution and
// statement error for every constructor form and row, and the same statement
// clock, short-circuit order and error.

// rowTemporalConstructorNode returns the temporal constructor node a compiled
// expression uses, or nil.
func rowTemporalConstructorNode(node rowValueNode) *rowTemporalFunctionNode {
	switch n := rowValueShape(node).(type) {
	case *rowTemporalFunctionNode:
		return n
	case *rowBoundNode:
		return rowTemporalConstructorNode(n.inner)
	}
	return nil
}

// The six constructors are compiled with their argument texts retained; a
// qualified temporal function (a truncate, the between family, fromepoch) is
// not one and keeps the text path.
func TestRowValueProgramCompilesTemporalConstructors(t *testing.T) {
	for _, tc := range []struct {
		expr     string
		function string
		argument string
		texts    []string
	}{
		{"date('2020-01-02')", "date", "'2020-01-02'", []string{"'2020-01-02'"}},
		{"DATE({year: 2020, month: 1, day: 2})", "date", "{year: 2020, month: 1, day: 2}", []string{"{year: 2020, month: 1, day: 2}"}},
		{"time(n.time)", "time", "n.time", []string{"n.time"}},
		{"localtime('03:04:05')", "localtime", "'03:04:05'", []string{"'03:04:05'"}},
		{"localdatetime(n.localdatetime)", "localdatetime", "n.localdatetime", []string{"n.localdatetime"}},
		{"datetime('2020-01-02T03:04:05Z')", "datetime", "'2020-01-02T03:04:05Z'", []string{"'2020-01-02T03:04:05Z'"}},
		{"duration({seconds: 90})", "duration", "{seconds: 90}", []string{"{seconds: 90}"}},
		// A pattern call keeps both arguments and the whole argument text the
		// evaluator falls back to for a call it can't read as a pattern.
		{"date('2020-01-02', 'yyyy-MM-dd')", "date", "'2020-01-02', 'yyyy-MM-dd'", []string{"'2020-01-02'", "'yyyy-MM-dd'", "'2020-01-02', 'yyyy-MM-dd'"}},
		// Three arguments are not a pattern: the evaluator evaluates the whole
		// argument text, so the node retains it.
		{"date(1, 2, 3)", "date", "1, 2, 3", []string{"1", "2", "3", "1, 2, 3"}},
	} {
		compiler := rowValueCompiler{}
		root, info := compiler.compile(t.Context(), tc.expr)
		node := rowTemporalConstructorNode(root)
		require.NotNil(t, node, tc.expr)
		require.Equal(t, tc.function, node.function, tc.expr)
		require.Equal(t, tc.argument, node.argument, tc.expr)
		require.Equal(t, tc.texts, node.texts, tc.expr)
		require.Len(t, node.parts, len(node.texts), tc.expr)
		require.False(t, info.pure, tc.expr)
	}
	for _, expr := range []string{
		"date.truncate('day', date('2020-01-02'))",
		"datetime.fromepoch(1, 0)",
		"datetime.fromepochmillis(1)",
		"duration.between(date('2020-01-02'), date('2020-01-03'))",
		"duration.inSeconds(date('2020-01-02'), date('2020-01-03'))",
		"toLower('x')",
	} {
		compiler := rowValueCompiler{}
		root, _ := compiler.compile(t.Context(), expr)
		require.Nil(t, rowTemporalConstructorNode(root), expr)
		require.Equal(t, expr, firstRowTextNode(root), expr)
	}
}

// A temporal constructor inside a list evaluation is never memoized: it may
// read the statement's clock, so it is evaluated for every element.
func TestRowValueProgramKeepsTemporalConstructorsUnhoisted(t *testing.T) {
	compiler := rowValueCompiler{}
	root, _ := compiler.compile(t.Context(), "[k IN [1, 2] | datetime(n.datetime)]")
	comprehension, ok := rowValueShape(rowValueShape(root).(*rowBoundNode).inner).(*rowComprehensionNode)
	require.True(t, ok)
	require.NotNil(t, rowTemporalConstructorNode(comprehension.projection))
	root, _ = compiler.compile(t.Context(), "[k IN [1, 2] | datetime()]")
	comprehension, ok = rowValueShape(rowValueShape(root).(*rowBoundNode).inner).(*rowComprehensionNode)
	require.True(t, ok)
	_, memoized := rowValueShape(comprehension.projection).(*rowMemoNode)
	require.False(t, memoized)
	require.NotNil(t, rowTemporalConstructorNode(comprehension.projection))
}

// rowTemporalConstructorExecutor: (a:T) carries a valid text for every
// constructor, an unparseable text, and a fields map; (b:T) carries none.
func rowTemporalConstructorExecutor(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	fields := map[string]interface{}{"year": int64(2020), "month": int64(1), "day": int64(2), "hour": int64(3)}
	for _, node := range []*storage.Node{
		{ID: "a", Labels: []string{"T"}, Properties: map[string]interface{}{
			"id": "a", "date": "2020-01-02", "time": "03:04:05", "localtime": "03:04:05",
			"localdatetime": "2020-01-02T03:04:05", "datetime": "2020-01-02T03:04:05Z",
			"duration": "PT26H", "bad": "not a temporal value", "fields": fields,
		}},
		{ID: "b", Labels: []string{"T"}, Properties: map[string]interface{}{"id": "b"}},
	} {
		_, err := store.CreateNode(t.Context(), node)
		require.NoError(t, err)
	}
	return NewStorageExecutor(store), store
}

// rowTemporalConstructorCorpus: every constructor form — text, parameter, row
// property, map, nested constructor, pattern, null, unparseable text, a wrong
// type, an unresolved argument, short-circuit operands and list elements.
var rowTemporalConstructorCorpus = []string{
	"date('2020-01-02')", "date($text)", "date(n.date)", "date(n.bad)", "date(n.absent)", "date(null)",
	"date({year: 2020, month: 1, day: 2})", "date(n.fields)", "date({year: n.fields.year, month: 1, day: 2})",
	"date(1)", "date(true)", "date(datetime('2020-01-02T03:04:05Z'))", "date('02.01.2020', 'dd.MM.yyyy')",
	"date('2020-01-02', 'yyyy-MM-dd')", "date(1, 2, 3)",
	"time('03:04:05')", "time(n.time)", "time(n.bad)", "time({hour: 3, minute: 4})", "time(1)",
	"localtime('03:04:05')", "localtime(n.localtime)", "localtime(n.bad)", "localtime(1)",
	"localdatetime('2020-01-02T03:04:05')", "localdatetime(n.localdatetime)", "localdatetime(n.bad)", "localdatetime(1)",
	"datetime('2020-01-02T03:04:05Z')", "datetime(n.datetime)", "datetime(n.bad)", "datetime(1)",
	"datetime({year: 2020, month: 1, day: 2, hour: 3, timezone: 'Europe/Stockholm'})", "datetime(n.fields)",
	"duration('PT1H')", "duration(n.duration)", "duration({seconds: 90})", "duration({days: 1, hours: 2})",
	"duration(n.bad)", "duration(null)", "duration(1)", "duration({seconds: 1}, 'x')",
	"datetime(datetime(n.datetime))", "date(date(n.date))",
	"coalesce(date(n.bad), date('2020-01-02'))", "date(n.bad) IS NULL", "date(n.date) = date('2020-01-02')",
	"CASE WHEN n.bad IS NULL THEN date('2020-01-02') ELSE date(n.bad) END",
	"date(missing)", "true OR date(n.bad) IS NULL", "false AND date(n.bad) IS NULL",
	"n.bad IS NOT NULL OR date(n.bad) IS NULL", "date(n.bad) IS NULL OR n.id = 'a'",
	"[k IN [1, 2] | datetime({year: 2020, month: 1, day: k})]",
	"[k IN [1, 2] | date(n.date) = date($text)]",
	"any(k IN [1, 2] WHERE date(n.date) = date('2020-01-02'))",
}

func rowTemporalConstructorRows(t *testing.T, store storage.Engine) []pipelineRow {
	t.Helper()
	node, err := store.GetNode(t.Context(), "a")
	require.NoError(t, err)
	bare, err := store.GetNode(t.Context(), "b")
	require.NoError(t, err)
	fields := map[string]interface{}{"year": int64(2020), "month": int64(1), "day": int64(2), "hour": int64(3)}
	return []pipelineRow{
		{"n": node, "$text": "2020-01-02", "$fields": fields},
		{"n": bare},
		{"n": nil, "$text": "not a temporal value"},
		{"n": int64(1), "$text": int64(7)},
		// A row binding wins over the expression's form, and a binding of an
		// argument's own text wins inside the call.
		{"n": node, "date('2020-01-02')": "bound", "n.bad": "2020-01-02", "$text": "2020-01-02"},
	}
}

// A compiled temporal constructor gives what the row evaluator reading the
// text gives, for every form and row: value, resolution and error.
func TestRowTemporalConstructorProgramAdapterConsistency(t *testing.T) {
	exec, store := rowTemporalConstructorExecutor(t)
	rows := rowTemporalConstructorRows(t, store)
	defer rowValueProgramsDisabled.Store(false)
	for _, expr := range rowTemporalConstructorCorpus {
		compiler := rowValueCompiler{}
		root, _ := compiler.compile(t.Context(), expr)
		for index, row := range rows {
			rowValueProgramsDisabled.Store(true)
			want, wantOK, wantErr := exec.evaluateRowValue(t.Context(), expr, row)
			rowValueProgramsDisabled.Store(false)
			got, gotOK, gotErr := root.eval(t.Context(), exec, row, nil)
			require.Equal(t, wantOK, gotOK, "%s (row %d): resolved", expr, index)
			require.Equal(t, renderTemporalParityValue(want), renderTemporalParityValue(got), "%s (row %d)", expr, index)
			require.Equal(t, temporalParityError(wantErr), temporalParityError(gotErr), "%s (row %d): error", expr, index)
			// The public row entry point reaches the same compiled form.
			public, publicOK, publicErr := exec.evaluateRowValue(t.Context(), expr, row)
			require.Equal(t, renderTemporalParityValue(want), renderTemporalParityValue(public), "%s (row %d): public", expr, index)
			require.Equal(t, wantOK, publicOK, "%s (row %d): public resolved", expr, index)
			require.Equal(t, temporalParityError(wantErr), temporalParityError(publicErr), "%s (row %d): public error", expr, index)
		}
	}
	// The public row entry point reaches the compiled constructor, not the
	// text node.
	require.NoError(t, drainRowValue(t, exec, "date(n.date)", rows[0]))
	program, cached := rowValuePrograms.get("date(n.date)")
	require.True(t, cached)
	require.NotNil(t, rowTemporalConstructorNode(program.root))
}

// The public query path returns the same rows and errors with temporal
// constructors compiled and read from the text.
func TestTemporalConstructorPublicProgramParity(t *testing.T) {
	exec, _ := rowTemporalConstructorExecutor(t)
	queries := []struct {
		query  string
		params map[string]interface{}
	}{
		{"RETURN date('2020-01-02') AS v", nil},
		{"RETURN date(1) AS v", nil},
		{"RETURN date('2020-01-02', 'yyyy-MM-dd') AS v", nil},
		{"RETURN localtime('03:04:05') AS v, localdatetime('2020-01-02T03:04:05') AS w, time('03:04:05') AS x", nil},
		{"RETURN duration({seconds: 90}) AS v, duration('PT1H') AS w", nil},
		{"WITH date('2020-01-02') AS d RETURN date(d) AS v", nil},
		{"RETURN datetime($text) AS v", map[string]interface{}{"text": "2020-01-02T03:04:05Z"}},
		{"RETURN datetime($text) AS v", map[string]interface{}{"text": "not a temporal value"}},
		{"MATCH (n:T) RETURN n.id AS id, date(n.date) AS v ORDER BY id", nil},
		{"MATCH (n:T) WHERE date(n.date) = date($text) RETURN n.id AS id", map[string]interface{}{"text": "2020-01-02"}},
		{"MATCH (n:T) RETURN datetime(n.bad) AS v", nil},
		{"MATCH (n:T) RETURN coalesce(date(n.bad), date('2020-01-02')) AS v ORDER BY v", nil},
		{"MATCH (n:T) RETURN [k IN [1, 2] | date(n.date)] AS v ORDER BY v", nil},
		{"UNWIND [1, 2] AS k RETURN datetime({year: 2020 + k, month: 1, day: 2}) AS v", nil},
		{"UNWIND [1, 2] AS k RETURN CASE WHEN k = 1 THEN date('2020-01-02') ELSE duration({days: k}) END AS v", nil},
	}
	defer rowValueProgramsDisabled.Store(false)
	for _, tc := range queries {
		rowValueProgramsDisabled.Store(true)
		want, wantErr := exec.Execute(t.Context(), tc.query, tc.params)
		rowValueProgramsDisabled.Store(false)
		got, gotErr := exec.Execute(t.Context(), tc.query, tc.params)
		require.Equal(t, temporalParityError(wantErr), temporalParityError(gotErr), tc.query)
		if wantErr != nil {
			continue
		}
		require.Equal(t, temporalParityRows(want.Rows), temporalParityRows(got.Rows), tc.query)
		require.Equal(t, want.Columns, got.Columns, tc.query)
	}
}

// The constructors' clock is the row's statement clock, read on every
// evaluation: the compiled node neither drops the statement context nor keeps
// the instant it read.
func TestRowTemporalConstructorStatementClock(t *testing.T) {
	exec, _ := rowTemporalConstructorExecutor(t)
	instant := time.Date(2021, 5, 6, 7, 8, 9, 123456789, time.UTC)
	current := instant
	statement := context.WithValue(withExpressionFailureSlot(context.Background()), temporalStatementTimeKey{}, &current)
	operation := withExpressionFailureSlot(context.Background())
	row := pipelineRow{rowStatementContextKey: statement}
	defer rowValueProgramsDisabled.Store(false)
	for _, tc := range []struct{ expr, kind string }{
		{"date()", "date"}, {"time()", "time"}, {"localtime()", "localtime"},
		{"localdatetime()", "localdatetime"}, {"datetime()", "datetime"},
	} {
		want, handled := temporalClockValue(tc.kind, instant, "")
		require.True(t, handled, tc.expr)
		compiler := rowValueCompiler{}
		root, _ := compiler.compile(t.Context(), tc.expr)
		got, resolved, err := root.eval(operation, exec, row, nil)
		require.NoError(t, err, tc.expr)
		require.True(t, resolved, tc.expr)
		require.Equal(t, renderTemporalParityValue(want), renderTemporalParityValue(got), tc.expr)
		rowValueProgramsDisabled.Store(true)
		text, textResolved, textErr := exec.evaluateRowValue(operation, tc.expr, row)
		rowValueProgramsDisabled.Store(false)
		require.NoError(t, textErr, tc.expr)
		require.True(t, textResolved, tc.expr)
		require.Equal(t, renderTemporalParityValue(got), renderTemporalParityValue(text), tc.expr)
	}
	// The same compiled node re-reads the clock: an instant it kept would not
	// follow the statement's clock.
	compiler := rowValueCompiler{}
	root, _ := compiler.compile(t.Context(), "datetime()")
	first, _, err := root.eval(operation, exec, row, nil)
	require.NoError(t, err)
	current = instant.Add(time.Hour)
	second, _, err := root.eval(operation, exec, row, nil)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprint(first), fmt.Sprint(CypherDateTime{Time: instant}))
	require.Equal(t, fmt.Sprint(second), fmt.Sprint(CypherDateTime{Time: instant.Add(time.Hour)}))
	require.NotEqual(t, fmt.Sprint(first), fmt.Sprint(second))
}

// A compiled temporal constructor answers only the argument texts it
// compiled: an argument it doesn't know is unresolved, never re-read from the
// text.
func TestRowTemporalFunctionNodeRejectsUnknownArguments(t *testing.T) {
	exec, store := rowTemporalConstructorExecutor(t)
	node, err := store.GetNode(t.Context(), "a")
	require.NoError(t, err)
	row := pipelineRow{"n": node}
	unprepared := &rowTemporalFunctionNode{function: "date", argument: "n.date"}
	value, resolved, err := unprepared.eval(t.Context(), exec, row, nil)
	require.Nil(t, value)
	require.False(t, resolved)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	require.Contains(t, err.Error(), "could not evaluate expression: n.date")

	columnApplied := &rowTemporalFunctionNode{function: "date", argument: "n.date", texts: []string{"n.date"}, parts: []rowValueNode{rowConstNode{value: "2020-01-02"}}}
	value, resolved, err = columnApplied.eval(t.Context(), exec, row, nil)
	require.NoError(t, err)
	require.True(t, resolved)
	require.Equal(t, "2020-01-02", fmt.Sprint(value))
}

// evaluateTemporalConstructor reads the call from the facts its caller
// passes: a zero call isn't a temporal call, and a constructor whose facts name
// a single argument reports that argument's error without parsing an
// expression again.
func TestEvaluateTemporalConstructorReadsCallFacts(t *testing.T) {
	exec, _ := rowTemporalConstructorExecutor(t)
	unresolved := func(string) (interface{}, bool, error) { return nil, false, nil }
	value, handled, err := exec.evaluateTemporalConstructor(context.Background(), unresolved, functionCallFacts{})
	require.False(t, handled)
	require.NoError(t, err)
	require.Nil(t, value)
	value, handled, err = exec.evaluateTemporalConstructor(context.Background(), unresolved, functionCallFacts{name: "toLower", inner: "'X'"})
	require.False(t, handled)
	require.NoError(t, err)
	require.Nil(t, value)

	integer := func(string) (interface{}, bool, error) { return int64(1), true, nil }
	value, handled, err = exec.evaluateTemporalConstructor(context.Background(), integer, functionCallFacts{name: "date", inner: "1"})
	require.True(t, handled)
	require.Nil(t, value)
	requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
	require.Contains(t, err.Error(), "Long(1)")
}

// A canceled operation context is an error for a compiled temporal
// constructor, as it is for the text path: neither returns a value.
func TestRowTemporalConstructorCancellation(t *testing.T) {
	exec, store := rowTemporalConstructorExecutor(t)
	node, err := store.GetNode(t.Context(), "a")
	require.NoError(t, err)
	row := pipelineRow{"n": node}
	// Compile the expression before cancelling, so the program is cached.
	require.NoError(t, drainRowValue(t, exec, "date(n.date)", row))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	defer rowValueProgramsDisabled.Store(false)
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		value, resolved, err := exec.evaluateRowValue(canceled, "date(n.date)", row)
		require.Nil(t, value)
		require.False(t, resolved)
		require.ErrorIs(t, err, context.Canceled)
	}
}

func drainRowValue(t *testing.T, exec *StorageExecutor, expr string, row pipelineRow) error {
	t.Helper()
	_, _, err := exec.evaluateRowValue(t.Context(), expr, row)
	_, _, err = exec.evaluateRowValue(t.Context(), expr, row)
	return err
}

// renderTemporalParityValue renders a value for parity comparison: a temporal
// value by its type and text, a list or map element by element, anything else
// by its type and value.
func renderTemporalParityValue(value interface{}) string {
	switch typed := value.(type) {
	case []interface{}:
		rendered := make([]string, len(typed))
		for index, item := range typed {
			rendered[index] = renderTemporalParityValue(item)
		}
		return "[" + strings.Join(rendered, ", ") + "]"
	case map[string]interface{}:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		rendered := make([]string, 0, len(keys))
		for _, key := range keys {
			rendered = append(rendered, key+": "+renderTemporalParityValue(typed[key]))
		}
		return "{" + strings.Join(rendered, ", ") + "}"
	}
	return fmt.Sprintf("%T:%v", value, value)
}

func temporalParityRows(rows [][]interface{}) [][]string {
	rendered := make([][]string, len(rows))
	for index, row := range rows {
		rendered[index] = make([]string, len(row))
		for column, value := range row {
			rendered[index][column] = renderTemporalParityValue(value)
		}
	}
	return rendered
}

func temporalParityError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
