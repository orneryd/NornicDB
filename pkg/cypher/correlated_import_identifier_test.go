package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCorrelatedSubqueryEscapedImportShortcutParity(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "escaped-import"))
	_, err := exec.Execute(ctx, "CREATE (:A {n:1})-[:R]->(:B), (:A {n:2})-[:R]->(:B)", nil)
	require.NoError(t, err)

	// An importing WITH exercises the general row path with the same binding.
	// Unscoped CALL's local declarations have a different scope contract; see
	// TestCorrelatedCallEscapedIdentifierScopeParity.
	for _, name := range []string{"`a``b`", "`a b`"} {
		for _, form := range []struct {
			name  string
			query string
			want  interface{}
		}{
			{"COLLECT", "RETURN COLLECT { %s MATCH (%s:A) RETURN count(%s) AS c } AS c", []interface{}{int64(1)}},
			{"COUNT", "RETURN COUNT { %s MATCH (%s:A) } AS c", int64(1)},
		} {
			t.Run(form.name+"/"+name, func(t *testing.T) {
				prefix := "MATCH (a:A) WITH a AS " + name + " "
				body := func(imports string) string {
					if form.name == "COUNT" {
						return fmt.Sprintf(form.query, imports, name)
					}
					return fmt.Sprintf(form.query, imports, name, name)
				}
				general, err := exec.Execute(ctx, prefix+body("WITH "+name), nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{form.want}, {form.want}}, general.Rows)
				actual, err := exec.Execute(ctx, prefix+body(""), nil)
				require.NoError(t, err)
				require.Equal(t, general.Columns, actual.Columns)
				require.Equal(t, general.Rows, actual.Rows)
			})
		}
	}
}

func TestCorrelatedPipelineImportedIdentifierShortcutParity(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "imported-pipeline"))
	_, err := exec.Execute(ctx, "CREATE (:A {n:1})-[:R]->(:B), (:A {n:2})-[:R]->(:B)", nil)
	require.NoError(t, err)
	seeds, err := exec.Execute(ctx, "MATCH (a:A) RETURN a ORDER BY a.n", nil)
	require.NoError(t, err)

	for _, source := range []string{"fabric", "context"} {
		for _, written := range []string{"`a``b`", "`a b`", "`a`", "a"} {
			name := symbolicNameValue(written)
			for _, body := range []string{
				"MATCH (" + written + ":A) RETURN count(" + written + ") AS c",
				"MATCH (" + written + ")-[r:R]->() RETURN count(r) AS c",
				"MATCH (" + written + ") RETURN " + written + ".n AS n LIMIT 5",
			} {
				t.Run(source+"/"+body, func(t *testing.T) {
					clauses, ok := canExecuteAsPipeline(t.Context(), body)
					require.True(t, ok)
					for _, seed := range seeds.Rows {
						bindings := pipelineRow{name: seed[0]}
						runCtx := ctx
						if source == "fabric" {
							exec.fabricRecordBindings = bindings
							defer func() { exec.fabricRecordBindings = nil }()
						} else {
							runCtx = withValueBindings(ctx, bindings)
						}
						require.True(t, exec.pipelineReadsImportedBinding(runCtx, clauses))
						// These direct callers supply clause text before the public
						// quoted-variable rewrite. Preserve the general path's existing
						// name/scope behavior and compare dispatch against it verbatim.
						general, handled, err := exec.runPipelineClauseRows(runCtx, []pipelineRow{bindings}, map[string]struct{}{name: {}}, clauses, clauses, nil)
						require.NoError(t, err)
						require.True(t, handled)
						require.NotEmpty(t, general.Rows)
						actual := exec.executePipeline(runCtx, body)
						require.NoError(t, actual.err)
						require.True(t, actual.handled())
						require.Equal(t, general.Columns, actual.result.Columns)
						require.ElementsMatch(t, general.Rows, actual.result.Rows)
					}
					unrelated := "MATCH (other:A) RETURN count(other) AS c"
					unrelatedClauses, ok := canExecuteAsPipeline(t.Context(), unrelated)
					require.True(t, ok)
					require.False(t, exec.pipelineReadsImportedBinding(withValueBindings(ctx, pipelineRow{name: seeds.Rows[0][0]}), unrelatedClauses))
					unrelatedResult := exec.executePipeline(ctx, unrelated)
					require.NoError(t, unrelatedResult.err)
					require.True(t, unrelatedResult.handled())
					require.Equal(t, [][]interface{}{{int64(2)}}, unrelatedResult.result.Rows)
				})
			}
		}
	}
}

func TestImportedIdentifierReferenceTokens(t *testing.T) {
	for _, tc := range []struct {
		query string
		name  string
		want  bool
	}{
		{"MATCH (`a``b`:A) RETURN count(`a``b`)", "a`b", true},
		{"MATCH (`a b`:A) RETURN `a b`", "a b", true},
		{"RETURN `a`", "a", true},
		{"RETURN`a``b`", "a`b", true},
		{"RETURN 1e3", "e3", false},
		{"RETURN a", "a", true},
		{"RETURN `a``b`", "a", false},
		{"RETURN `a b`", "a", false},
		{"RETURN prefix_a, a_suffix", "a", false},
		{"RETURN 'a', \"a\" /* a */ // a\n", "a", false},
		{"RETURN a", "A", false},
		{"RETURN a", "", false},
	} {
		t.Run(tc.query+"/"+tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isIdentifierReferenced(tc.query, tc.name))
		})
	}
}

func BenchmarkImportedIdentifierRecognition(b *testing.B) {
	for _, tc := range []struct {
		name, query, binding string
	}{
		{"plain", "MATCH (a:A) RETURN count(a)", "a"},
		{"escaped", "MATCH (`a``b`:A) RETURN count(`a``b`)", "a`b"},
		{"unrelated", "MATCH (other:A) RETURN count(other)", "a"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				isIdentifierReferenced(tc.query, tc.binding)
			}
		})
	}
}
