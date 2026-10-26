package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// queryBodyWindows is every braced body of source, as windows.
func queryBodyWindows(t *testing.T, source string) [][2]int {
	t.Helper()
	var windows [][2]int
	require.NoError(t, forEachQueryBody(t.Context(), originalBraceView(source), queryBodyContainerKeywords[:], func(_ string, bodyView braceView) error {
		start, end := testBodyWindow(bodyView)
		windows = append(windows, [2]int{start, end})
		return nil
	}))
	return windows
}

// The body reader gives every branch, clause, USE and FINISH its span in the
// one source read, nested bodies included: what the conclusion rules and the
// statement facts both read.
func TestReadQueryBodySpans(t *testing.T) {
	source := "EXPLAIN CALL { USE g MATCH (n) RETURN n UNION ALL WITH 1 AS x FINISH } RETURN 1 AS a;"
	start, end, mode, ok := queryStatementWindow(t.Context(), source)
	require.True(t, ok)
	require.Equal(t, ModeExplain, mode)
	require.Equal(t, "CALL { USE g MATCH (n) RETURN n UNION ALL WITH 1 AS x FINISH } RETURN 1 AS a", source[start:end])

	// Leading comments are trivia, whatever they hold.
	decoy := " /* decoy CALL { RETURN 0 AS decoy } */ CYPHER 25 EXPLAIN USE comp CALL () { RETURN 1 AS a } RETURN *;"
	decoyStart, decoyEnd, decoyMode, decoyRead := queryStatementWindow(t.Context(), decoy)
	require.True(t, decoyRead)
	require.Equal(t, ModeExplain, decoyMode)
	require.Equal(t, "USE comp CALL () { RETURN 1 AS a } RETURN *", decoy[decoyStart:decoyEnd])

	statement := readQueryBody(t.Context(), originalBraceView(source), start, end)
	require.Equal(t, queryBodyRead, statement.status)
	require.Len(t, statement.branches, 1)
	call := statement.branches[0].clauses[0]
	from, to, ok := callClauseBody(t.Context(), call)
	require.True(t, ok)
	require.Equal(t, [][2]int{{from, to}}, queryBodyWindows(t, source))
	_, _, ok = callClauseBody(t.Context(), statement.branches[0].clauses[1])
	require.False(t, ok, "RETURN has no body")

	body := readQueryBody(t.Context(), originalBraceView(source), from, to)
	require.Len(t, body.branches, 2)
	first, second := body.branches[0], body.branches[1]
	require.Equal(t, "USE g MATCH (n) RETURN n", source[first.start:first.end])
	require.Equal(t, "USE g", source[first.useStart:first.useEnd])
	require.False(t, first.unionAll)
	require.False(t, first.finished)
	require.Equal(t, -1, first.finishStart)
	require.Len(t, first.clauses, 2)
	require.Equal(t, pipelineClauseMatch, first.clauses[0].kind)
	for _, clause := range append(first.clauses, second.clauses...) {
		require.Equal(t, clause.text, source[clause.start:clause.end])
	}

	// FINISH after RETURN stays in the text: execution's rule
	// (stripTrailingFinish), which validation rejects.
	require.Equal(t, "WITH 1 AS x FINISH", source[second.start:second.end])
	require.True(t, second.unionAll)
	require.True(t, second.finished)
	require.Equal(t, -1, second.useStart)
}

// A legacy importing WITH before USE keeps its WITH among the clauses, the
// USE excluded; a stripped FINISH has its span; an OPTIONAL CALL spans its
// CALL and is marked optional; a branch the splitter declines is a typed
// outcome, not an empty body.
func TestReadQueryBodyUseFinishOptionalAndDecline(t *testing.T) {
	source := "CALL { WITH outer USE comp.remote MATCH (n) WITH n FINISH } RETURN 1 AS a"
	windows := queryBodyWindows(t, source)
	require.Len(t, windows, 1)
	body := readQueryBody(t.Context(), originalBraceView(source), windows[0][0], windows[0][1])
	require.Equal(t, queryBodyRead, body.status)
	branch := body.branches[0]
	require.Equal(t, "USE comp.remote", source[branch.useStart:branch.useEnd])
	require.Len(t, branch.clauses, 3)
	require.Equal(t, pipelineClauseWith, branch.clauses[0].kind)
	require.Equal(t, "WITH outer", source[branch.clauses[0].start:branch.clauses[0].end])
	require.Equal(t, pipelineClauseMatch, branch.clauses[1].kind)
	require.True(t, branch.finished)
	require.Equal(t, "FINISH", source[branch.finishStart:branch.finishEnd])

	source = "UNWIND [] AS s OPTIONAL CALL { RETURN 1 AS b } RETURN b"
	statement := readQueryBody(t.Context(), originalBraceView(source), 0, len(source))
	require.Equal(t, queryBodyRead, statement.status)
	optional := statement.branches[0].clauses[1]
	require.True(t, optional.optional)
	require.Equal(t, optional.text, source[optional.start:optional.end])
	from, to, ok := callClauseBody(t.Context(), optional)
	require.True(t, ok)
	require.Equal(t, " RETURN 1 AS b ", source[from:to])

	// A body of FINISH alone (CALL { FINISH }) reads as finished with no
	// clauses.
	source = "FINISH"
	empty := readQueryBody(t.Context(), originalBraceView(source), 0, len(source))
	require.Equal(t, queryBodyRead, empty.status)
	require.True(t, empty.branches[0].finished)
	require.Empty(t, empty.branches[0].clauses)

	source = "RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a"
	require.Equal(t, queryBodyMixedUnion, readQueryBody(t.Context(), originalBraceView(source), 0, len(source)).status)

	source = "USE 1 RETURN 1 AS a"
	declined := readQueryBody(t.Context(), originalBraceView(source), 0, len(source))
	require.Equal(t, queryBodyDeclined, declined.status)
	require.True(t, declined.branches[0].declined)
	require.Nil(t, declined.branches[0].clauses)
}

// A CALL body is the token after CALL and its scope list. Later braces must
// never turn a procedure into a subquery with a different import scope.
func TestReadQueryBodyCallFrame(t *testing.T) {
	for _, query := range []string{
		"CALL db.labels() YIELD label CALL { WITH label RETURN label AS value } RETURN value",
		"CALL custom.proc({value: 1}) YIELD value RETURN value",
		"CALL /* decoy { } */ db.labels() YIELD label RETURN label",
	} {
		frame := readCallSubqueryScope(t.Context(), originalBraceView(query))
		require.True(t, strings.HasPrefix(query[frame.bodyOffset:], "db.") || strings.HasPrefix(query[frame.bodyOffset:], "custom."))
		_, _, body := callSubqueryBraces(t.Context(), query)
		require.False(t, body, query)
	}
	for _, query := range []string{
		"CALL /* decoy { } */ { RETURN 1 AS value }",
		"CALL () /* decoy { } */ { RETURN 1 AS value }",
		"CALL (`name { decoy`) { RETURN 1 AS value }",
	} {
		frame := readCallSubqueryScope(t.Context(), originalBraceView(query))
		open, close, ok := callSubqueryBraces(t.Context(), query)
		require.True(t, ok, query)
		require.Equal(t, frame.bodyOffset, open)
		require.Equal(t, " RETURN 1 AS value ", query[open+1:close])
	}
}
