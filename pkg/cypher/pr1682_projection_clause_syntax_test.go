package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPR1682RetainedAggregateAliasTrivia(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate_alias"))
	for _, alias := range []string{"AS n", "AS\nn", "AS\tn", "AS/* alias gap */n"} {
		t.Run(alias, func(t *testing.T) {
			clauses, ok := pipelineClausesFor(t.Context(), "WITH count(x) "+alias)
			require.True(t, ok)
			require.Len(t, clauses, 1)
			rows, handled := exec.pipelineApplyWith(t.Context(), []pipelineRow{{"x": int64(1)}}, clauses[0], nil, false)
			require.True(t, handled, "retained WITH must use its grammar alias")
			require.Equal(t, []pipelineRow{{"n": int64(1)}}, rows)
			query := "CALL () { UNWIND [1] AS x WITH count(x) " + alias + " RETURN n } RETURN n"
			result, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"n"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}

// A WITH clause is read from its own occurrence: statement splitting and the
// clauses the executor composes itself both retain the syntax of that text, so
// the canonical readers take its items from the grammar rather than scanning
// the clause text, and both provenances read the same items.
func TestPR1682LoneWithClauseRetainsSyntax(t *testing.T) {
	items := func(clause pipelineClause) [][2]string {
		var read [][2]string
		for _, item := range projectionItemsFromSyntax(clause.syntax) {
			read = append(read, [2]string{item.expression, item.alias})
		}
		return read
	}
	for _, source := range []string{
		"WITH 1 AS x",
		"WITH *",
		"WITH a AS b WHERE a > 1",
		"WITH n.name AS name, {}",
		"WITH * ORDER BY x SKIP 1 LIMIT 2",
	} {
		clauses, ok := pipelineClausesFor(t.Context(), source)
		require.Truef(t, ok, "statement %q does not split", source)
		require.Len(t, clauses, 1, source)
		require.Equal(t, pipelineClauseWith, clauses[0].kind, source)
		require.Truef(t, clauses[0].syntax.Valid(), "statement %q retains no syntax", source)
		composed := composedPipelineClause(t.Context(), pipelineClauseWith, source)
		require.Truef(t, composed.syntax.Valid(), "composed clause %q retains no syntax", source)
		require.Equal(t, items(clauses[0]), items(composed), source)
	}
}
