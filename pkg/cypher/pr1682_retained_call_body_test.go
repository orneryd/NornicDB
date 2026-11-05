package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A CALL subquery's empty-input metadata is the CALL clause's own declared
// columns from the bound statement program, in the same order and with the same
// names the rows>0 path reports. These controls pin both boundaries of the
// same statement shape, so neither an empty input nor an admitted row may read
// its columns from a second compile of the CALL text.
func TestRetainedCallBodyReportsDeclaredColumns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "retained_call_columns"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Record {id: 1, reading_id: 'reading-1'})", nil)
	require.NoError(t, err)
	cases := []struct {
		name, query string
		columns     []string
	}{
		{"scoped return", "MATCH (d:%s) CALL (d) { RETURN d.id AS x } RETURN x", []string{"x"}},
		{"unit body with finish", "MATCH (d:%s) CALL (d) { WITH 1 AS x FINISH } RETURN d.id AS id", []string{"id"}},
		{"union body", "MATCH (d:%s) CALL (d) { RETURN 1 AS a UNION RETURN 2 AS a } RETURN a", []string{"a"}},
		{"write body without return", "MATCH (d:%s) CALL (d) { SET d.seen = true } RETURN d.id AS id", []string{"id"}},
		{"optional call", "MATCH (d:%s) OPTIONAL CALL (d) { RETURN d.id AS y } RETURN y", []string{"y"}},
		{"unordered wildcard body", "MATCH (d:%s) CALL (d) { WITH d.id AS z RETURN * } RETURN z", []string{"z"}},
		{"two distinct bodies", "MATCH (d:%s) CALL (d) { WITH 1 AS a RETURN a AS p } CALL (d) { WITH 2 AS b RETURN b AS q } RETURN p, q", []string{"p", "q"}},
		{"two equal bodies", "MATCH (d:%s) CALL (d) { SET d.n = 1 } CALL (d) { SET d.n = 1 } RETURN d.n AS n", []string{"n"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// No node carries the empty label: the outer MATCH admits no row, so
			// every CALL below reports metadata without ever running its body.
			empty := strings.Replace(tc.query, "%s", "Missing", 1)
			result, err := exec.Execute(ctx, empty, nil)
			require.NoError(t, err, empty)
			require.Equal(t, tc.columns, result.Columns, empty)
			require.Empty(t, result.Rows, empty)
			// The same statement over an admitted row must agree, and its rows
			// must be the body's own result.
			admitted := strings.Replace(tc.query, "%s", "Record", 1)
			result, err = exec.Execute(ctx, admitted, nil)
			require.NoError(t, err, admitted)
			require.Equal(t, tc.columns, result.Columns, admitted)
			require.NotEmpty(t, result.Rows, admitted)
		})
	}
}

// The rows>0 controls: the retained body's clauses execute with the columns the
// same statement declares for an empty input.
func TestRetainedCallBodyExecutesRetainedClauses(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "retained_call_rows"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Record {id: 1, reading_id: 'reading-1'})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, query string
		columns     []string
		rows        [][]interface{}
	}{
		{
			"scoped return",
			"MATCH (d:Record {id: 1}) CALL (d) { RETURN d.reading_id AS x } RETURN x",
			[]string{"x"},
			[][]interface{}{{"reading-1"}},
		},
		{
			"two equal bodies",
			"MATCH (d:Record {id: 1}) CALL (d) { SET d.n = 1 } CALL (d) { SET d.n = 1 } RETURN d.n AS n",
			[]string{"n"},
			[][]interface{}{{int64(1)}},
		},
		{
			"two distinct bodies",
			"MATCH (d:Record {id: 1}) CALL (d) { WITH 1 AS a RETURN a AS p } CALL (d) { WITH 2 AS b RETURN b AS q } RETURN p, q",
			[]string{"p", "q"},
			[][]interface{}{{int64(1), int64(2)}},
		},
		{
			"union body",
			"MATCH (d:Record {id: 1}) CALL (d) { RETURN 1 AS a UNION RETURN 2 AS a } RETURN a ORDER BY a",
			[]string{"a"},
			[][]interface{}{{int64(1)}, {int64(2)}},
		},
		{
			"body with finish",
			"MATCH (d:Record {id: 1}) CALL (d) { WITH 1 AS x FINISH } RETURN d.id AS id",
			[]string{"id"},
			[][]interface{}{{int64(1)}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.columns, result.Columns, tc.query)
			require.Equal(t, tc.rows, result.Rows, tc.query)
		})
	}
}

// The retained body projection and the text split it replaces must agree on
// every admission decision, clause and column export, so a CALL body is read
// once and never re-derived from its text.
func TestRetainedBodySplitMatchesTextSplit(t *testing.T) {
	for _, body := range []string{
		"OPTIONAL MATCH (n:Reading {id: d.reading_id}) WITH collect(n.id) AS ids RETURN ids AS ids0",
		"WITH 1 AS x RETURN x",
		"SET d.n = 1",
		"WITH 1 AS x FINISH",
		"RETURN 1 AS a UNION RETURN 2 AS a",
		"USE g RETURN 1 AS z",
		"CALL db.labels() YIELD label RETURN label",
		"MATCH (n) LOAD CSV FROM 'file:///x' AS line RETURN line",
	} {
		t.Run(body, func(t *testing.T) {
			query := "MATCH (d:Record) CALL (d) { " + body + " } RETURN d.id"
			clauses, ok := splitPipelineClausesAllowingProcedureCalls(t.Context(), query)
			require.True(t, ok, query)
			require.Len(t, clauses, 3, query)
			retained, okBody := callBodyFromSyntax(clauses[1])
			require.True(t, okBody, query)
			retainedClauses, retainedOK := pipelineClausesForBody(t.Context(), retained)
			retainedSplit := cachedPipelineClausesBody(t.Context(), retained, true)
			textSplit := cachedPipelineClauses(t.Context(), queryBodyText(retained), true)
			require.Equal(t, textSplit.ok, retainedOK, body)
			require.Equal(t, textSplit.firstReturn, retainedSplit.firstReturn, body)
			require.Len(t, retainedClauses, len(textSplit.clauses), body)
			for index, textClause := range textSplit.clauses {
				require.Equal(t, textClause.kind, retainedClauses[index].kind, body)
				require.Equal(t, textClause.text, retainedClauses[index].text, body)
				require.Equal(t, textClause.optional, retainedClauses[index].optional, body)
				require.Equal(t, textClause.embeddingCandidate, retainedClauses[index].embeddingCandidate, body)
			}
		})
	}
}
