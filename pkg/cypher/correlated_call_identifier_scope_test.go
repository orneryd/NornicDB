package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A name first declared by MATCH in an unscoped CALL is local (#907), even
// when escaped. Importing it implicitly is deferred with #1385 / #1516.
func TestCorrelatedCallEscapedIdentifierScopeParity(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-probe"))
	_, err := exec.Execute(ctx, "CREATE (:A), (:A)", nil)
	require.NoError(t, err)
	for _, name := range []string{"`a``b`", "`a b`", "a"} {
		t.Run(name, func(t *testing.T) {
			prefix := "MATCH (a:A) WITH a AS " + name + " "
			body := "MATCH (" + name + ":A) RETURN count(" + name + ") AS c"
			query := prefix + "CALL { " + body + " } RETURN c"
			canonical, _ := canonicalizeQuotedVariables(query)
			clauses, ok := canExecuteAsPipeline(canonical)
			require.True(t, ok)
			// Bypass executePipeline and every statement shortcut entirely.
			fixtureAdmission184Context, fixtureAdmission184 := statementTestContext(ctx, canonical)
			_ = fixtureAdmission184Context
			general, handled, err := exec.runPipelineClauseRows(fixtureAdmission184, fixtureAdmission184Context, []pipelineRow{{}}, map[string]struct{}{}, clauses, clauses, nil)
			require.NoError(t, err)
			require.True(t, handled)
			public, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(2)}, {int64(2)}}, general.Rows)
			require.Equal(t, general.Rows, public.Rows)
			explicit, err := exec.Execute(ctx, prefix+"CALL { WITH "+name+" "+body+" } RETURN c", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}, {int64(1)}}, explicit.Rows)
			t.Logf("canonical=%s; public=%v; general without shortcuts=%v; explicit WITH=%v", canonical, public.Rows, general.Rows, explicit.Rows)
		})
	}
}
