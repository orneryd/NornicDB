// Author: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/config"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestRetainedConsumersKeepRepeatedCallOccurrences(t *testing.T) {
	const query = "CALL () { RETURN 1 AS x } CALL () { RETURN 1 AS x } RETURN x"
	ctx := cypherantlr.WithStatementSyntax(t.Context(), query)
	syntax, err := cypherantlr.ParseSyntax(ctx, query, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	body := readQueryBody(statementSyntaxQuery(syntax))
	require.Len(t, body.branches, 1)
	clauses := body.branches[0].clauses
	require.Len(t, clauses, 3)
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	first, _, _, _ := exec.parseCallSubquery(ctx, clauses[0].pipelineClause)
	second, _, _, _ := exec.parseCallSubquery(ctx, clauses[1].pipelineClause)
	require.Equal(t, first.executionText, second.executionText)
	require.NotEqual(t, first.syntax.ID(), second.syntax.ID())
	require.Equal(t, syntax.Origin(), first.syntax.Origin())
	require.Equal(t, syntax.Origin(), second.syntax.Origin())
	one := cachedPipelineClauses(ctx, first, true)
	two := cachedPipelineClauses(ctx, second, true)
	require.True(t, one.ok)
	require.True(t, two.ok)
	require.NotEqual(t, one.clauses[0].syntax.ID(), two.clauses[0].syntax.ID())
	require.Equal(t, first.syntax.Source(), one.clauses[0].syntax.Source())
}

func TestRetainedCallBranchSourceWindows(t *testing.T) {
	const query = "CALL () { RETURN /* one */ 1 AS x UNION ALL RETURN 2 AS x } RETURN x"
	ctx := cypherantlr.WithStatementSyntax(t.Context(), query)
	syntax, err := cypherantlr.ParseSyntax(ctx, query, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	root := readQueryBody(statementSyntaxQuery(syntax))
	body, _, _, _ := (&StorageExecutor{}).parseCallSubquery(ctx, root.branches[0].clauses[0].pipelineClause)
	branches, all, mixed, ok := parseTopLevelUnionBranches(ctx, body)
	require.True(t, ok)
	require.True(t, all)
	require.False(t, mixed)
	require.Len(t, branches, 2)
	require.Equal(t, "RETURN   1 AS x", branches[0].executionText)
	require.Equal(t, "RETURN 2 AS x", branches[1].executionText)
	require.NotEqual(t, branches[0].syntax.ID(), branches[1].syntax.ID())
	require.False(t, strings.Contains(branches[0].source.Text, "UNION"))
}

func TestRetainedConsumersPublicProjectionAndCallWindows(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, tc := range []struct {
				name    string
				query   string
				columns []string
				rows    [][]interface{}
			}{
				{"distinct_ordered_call", "UNWIND [2,1,2] AS point WITH DISTINCT point ORDER BY point CALL (point) { RETURN point + 1 AS next } RETURN point, next ORDER BY point", []string{"point", "next"}, [][]interface{}{{int64(1), int64(2)}, {int64(2), int64(3)}}},
				{"map_alias", "WITH {} AS m, 1 AS n RETURN m, n", []string{"m", "n"}, [][]interface{}{{map[string]interface{}{}, int64(1)}}},
				{"unit_finish", "CALL () { WITH 1 AS x FINISH } RETURN 7 AS result", []string{"result"}, [][]interface{}{{int64(7)}}},
				{"empty_call_header", "UNWIND [] AS seed CALL (seed) { RETURN seed AS value } RETURN value", []string{"value"}, [][]interface{}{}},
				{"union", "CALL () { RETURN 1 AS x UNION ALL RETURN 2 AS x } RETURN x ORDER BY x", []string{"x"}, [][]interface{}{{int64(1)}, {int64(2)}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					exec := NewStorageExecutor(newTestMemoryEngine(t))
					result, err := exec.Execute(context.Background(), tc.query, nil)
					require.NoError(t, err, tc.query)
					require.Equal(t, tc.columns, result.Columns, tc.query)
					if len(tc.rows) == 0 {
						require.Empty(t, result.Rows, tc.query)
					} else {
						require.Equal(t, tc.rows, result.Rows, tc.query)
					}
				})
			}
		})
	}
}

func TestRetainedCallClassifierKeepsAcceptedWindowOffsets(t *testing.T) {
	const source = "  WITH 1 AS x CALL (x) { RETURN x AS y } RETURN y"
	ctx := cypherantlr.WithStatementSyntax(t.Context(), source)
	_, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	call := strings.Index(source, "CALL")
	require.Equal(t, call, firstTopLevelCallSubquery(ctx, source))
	require.True(t, callSubqueryAt(ctx, source, call))
	suffix := source[call:]
	require.Zero(t, firstTopLevelCallSubquery(ctx, suffix))
	require.True(t, startsWithCallSubquery(ctx, suffix))
	require.True(t, callSubqueryAt(ctx, suffix, 0))
}
