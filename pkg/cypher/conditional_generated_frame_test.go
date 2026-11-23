package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConditionalFixPreservesUserNestedCallRejection(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "conditional_user_frames"))
		for _, query := range []string{
			"WITH 1 AS n CALL (n) { CALL (n) { RETURN n } RETURN n } RETURN n",
			"CYPHER 25 WITH 1 AS n CALL (n) { WHEN true THEN CALL (n) { RETURN n } RETURN n } RETURN n",
		} {
			_, err := exec.Execute(t.Context(), query, nil)
			require.Error(t, err, query)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
		}
	})
}

// Generated frames keep the arm guard outside aggregates and preserve inherited
// outputs through later quoted-name rewrites and nested conditional lowering.
func TestConditionalGeneratedFramesPreserveExecutionAndUserPolicy(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "conditional_generated_frames"))
		for _, tc := range []struct {
			query   string
			columns []string
			rows    [][]interface{}
		}{
			{"RETURN 7 AS `kept name` NEXT WHEN true THEN RETURN `kept name`, 2 AS extra", []string{"kept name", "extra"}, [][]interface{}{{int64(7), int64(2)}}},
			{"WITH 7 AS kept CALL (kept) { WHEN true THEN RETURN kept, 2 AS extra ELSE RETURN kept, 3 AS extra } RETURN kept, extra", []string{"kept", "extra"}, [][]interface{}{{int64(7), int64(2)}}},
			{"RETURN 7 AS kept NEXT WHEN false THEN RETURN count(*) AS total ELSE RETURN 9 AS total", []string{"total"}, [][]interface{}{{int64(9)}}},
			{"RETURN 7 AS kept NEXT WHEN true THEN RETURN kept NEXT WHEN true THEN RETURN kept, 2 AS extra", []string{"kept", "extra"}, [][]interface{}{{int64(7), int64(2)}}},
		} {
			result, err := exec.Execute(t.Context(), "CYPHER 25 "+tc.query, nil)
			require.NoError(t, err, tc.query)
			require.Equal(t, tc.columns, result.Columns, tc.query)
			require.Equal(t, tc.rows, result.Rows, tc.query)
		}
	})
}

func TestConditionalGeneratedFramesDoNotAuthorizePastedLowering(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		for _, generatedFirst := range []bool{false, true} {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "conditional_origin_cache"))
			raw := "RETURN 7 AS kept NEXT WHEN true THEN RETURN kept, 2 AS extra"
			lowered, _, err := desugarQueryStructure(t.Context(), raw, exec)
			require.NoError(t, err)
			generated := func() {
				result, err := exec.Execute(t.Context(), "CYPHER 25 "+raw, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(7), int64(2)}}, result.Rows)
			}
			pasted := func() {
				_, err := exec.Execute(t.Context(), "CYPHER 25 "+lowered, nil)
				require.Error(t, err, "user-supplied lowering has no generated-frame provenance")
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
			}
			if generatedFirst {
				generated()
				pasted()
				generated()
			} else {
				pasted()
				generated()
				pasted()
			}
		}
	})
}

func TestConditionalFrameProvenanceSurvivesRewriteWindow(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "conditional_provenance"))
	query, rewrite, err := desugarQueryStructure(t.Context(), "WHEN true THEN CALL { RETURN 1 AS other } RETURN other", exec)
	require.NoError(t, err)
	ctx := withSourceRewrite(t.Context(), rewrite)
	// A later rewrite shifts the generated frames; trimming then makes an
	// exact source window. Neither operation should change their provenance.
	shifted := &queryRewrite{Source: cyphertext.Source{Original: query, Text: "  " + query + "  ", Edits: []cyphertext.SourceEdit{
		{Original: cyphertext.Span{Start: 0, End: 0}, Accepted: cyphertext.Span{Start: 0, End: 2}},
		{Original: cyphertext.Span{Start: len(query), End: len(query)}, Accepted: cyphertext.Span{Start: len(query) + 2, End: len(query) + 4}},
	}}}
	ctx = withSourceRewrite(ctx, shifted)
	ctx, query = trimSourceText(ctx, shifted.Text, trimSourceWhitespace)
	syntax, err := cypherantlr.ParseSyntax(ctx, query, cypherantlr.StatementEntry, true, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	generated, user := 0, 0
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if node.Rule() == cypherantlr.CypherParserRULE_callSubquery {
			clause := pipelineClause{kind: pipelineClauseCallSubquery, syntax: node, text: node.Text()}
			if generatedConditionalCall(ctx, clause) {
				generated++
			} else {
				user++
			}
			require.False(t, generatedConditionalCall(t.Context(), clause), "same cached syntax without execution provenance is untrusted")
		}
		return nil
	})
	require.Equal(t, 2, generated)
	require.Equal(t, 1, user, "the arm's user-written CALL must not be marked")
}
