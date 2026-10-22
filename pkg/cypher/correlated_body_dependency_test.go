package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Generate the projection/import variants from the physical shortcut shapes,
// then compare dispatch with the row pipeline on exactly the same statement
// and initial row. Index presence must not change columns or statement validity.
func TestCorrelatedBodyDependencyDifferential(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "body-dependencies"))
	for _, query := range []string{
		"CREATE (:Outer {id:1}), (:Target {n:1,m:9})-[:R]->(:Target {n:2,m:8})",
		"CREATE INDEX target_n FOR (b:Target) ON (b.n)",
		"CREATE INDEX target_m FOR (b:Target) ON (b.m)",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	params := map[string]interface{}{"keys": []interface{}{int64(1)}}
	seed, err := exec.Execute(ctx, "MATCH (a:Outer) RETURN a", nil)
	require.NoError(t, err)
	shapes := []struct{ name, match, projection string }{
		{"node-count", "MATCH (b:Target)", "count(b) AS c"},
		{"filtered-count", "MATCH (b:Target) WHERE b.n > 0", "count(b) AS c"},
		{"indexed-read", "MATCH (b:Target) WHERE b.n IN $keys OR b.m IN $keys", "b.n AS n"},
		{"bounded-read", "MATCH (b:Target)", "b.n AS n LIMIT 5"},
		{"relationship-count", "MATCH ()-[r:R]->()", "count(r) AS c"},
		{"unwind-count", "UNWIND [1] AS k MATCH (b:Target) WHERE b.n = k", "count(b) AS c"},
	}
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			base := shape.match + " RETURN " + shape.projection
			clauses, ok := canExecuteAsPipeline(base)
			require.True(t, ok)
			// Prove the generated family starts at a real physical shortcut.
			runCtx := withParams(ctx, params)
			if shape.name == "unwind-count" {
				plan, err := exec.prepareTopLevelUnwind(runCtx, base)
				require.NoError(t, err)
				_, batch, err := exec.executeUnwindBatchOperator(runCtx, plan)
				require.NoError(t, err)
				require.True(t, batch, base)
			} else {
				_, node, nodeErr := exec.tryExecutePipelineSimpleNodeReadPlan(runCtx, clauses, params)
				_, relationship, relErr := exec.tryExecutePipelineSimpleRelationshipCountPlan(runCtx, clauses, params)
				require.NoError(t, nodeErr)
				require.NoError(t, relErr)
				require.True(t, node || relationship, base)
			}
			variants := []string{
				base,
				shape.match + " RETURN *",
				shape.match + " RETURN DISTINCT *",
				shape.match + " RETURN *, 1 AS marker",
				shape.match + " RETURN *, count(*) AS total",
				shape.match + " WITH * RETURN *",
				"WITH * " + shape.match + " RETURN *",
				"WITH a " + base,
				shape.match + " RETURN a AS imported",
				shape.match + " RETURN COLLECT { RETURN * } AS nested",
				shape.match + " RETURN COUNT { WITH * RETURN a } AS nested",
				shape.match + " RETURN EXISTS { MATCH (c:Target) RETURN * } AS nested",
			}
			for _, source := range []string{"fabric", "context", "both"} {
				for _, value := range []interface{}{seed.Rows[0][0], int64(7), nil} {
					for index, query := range variants {
						t.Run(fmt.Sprintf("%s/%T/%d", source, value, index), func(t *testing.T) {
							imports := pipelineRow{"a": value}
							exec.fabricRecordBindings = nil
							defer func() { exec.fabricRecordBindings = nil }()
							runCtx := withParams(ctx, params)
							if source != "context" {
								exec.fabricRecordBindings = imports
							}
							if source != "fabric" {
								runCtx = withValueBindings(runCtx, imports)
							}
							clauses, ok := canExecuteAsPipeline(exec.substituteParams(query, params))
							require.True(t, ok, query)
							row := pipelineRow{"a": value}
							bindParameterRow(runCtx, row)
							generalCtx := withExpressionFailureSlot(runCtx)
							general, handled, generalErr := exec.runPipelineClauses(generalCtx, []pipelineRow{row}, map[string]struct{}{"a": {}}, clauses, clauses)
							if failure := getExpressionFailure(generalCtx); failure != nil && generalErr == nil {
								generalErr = failure
							}
							require.True(t, handled, query)
							actual := exec.executePipeline(runCtx, query)
							require.True(t, actual.terminal(), query)
							if generalErr != nil {
								require.Error(t, actual.err, query)
								require.Equal(t, generalErr.Error(), actual.err.Error(), query)
							} else {
								require.NoError(t, actual.err, query)
								require.Equal(t, general.Columns, actual.result.Columns, query)
								require.ElementsMatch(t, general.Rows, actual.result.Rows, query)
							}
						})
					}
				}
			}
		})
	}
}

func TestCorrelatedBodyWildcardIndexValidity(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "wildcard-validity"))
	_, err := exec.Execute(ctx, "CREATE (:Outer {id:1}), (:Target {n:1,m:9}), (:Target {n:2,m:8})", nil)
	require.NoError(t, err)
	query := "MATCH (a:Outer) RETURN COLLECT { MATCH (b:Target) WHERE b.n IN $keys OR b.m IN $keys RETURN * } AS v"
	params := map[string]interface{}{"keys": []interface{}{int64(1)}}
	_, generalErr := exec.Execute(ctx, query, params)
	require.Error(t, generalErr, "COLLECT must reject the two-column body before indexes")
	for _, property := range []string{"n", "m"} {
		_, err := exec.Execute(ctx, fmt.Sprintf("CREATE INDEX target_%s FOR (b:Target) ON (b.%s)", property, property), nil)
		require.NoError(t, err)
	}
	_, indexedErr := exec.Execute(ctx, query, params)
	require.Error(t, indexedErr, "indexes must preserve COLLECT body validity")
	require.Equal(t, generalErr.Error(), indexedErr.Error())
}

func BenchmarkCorrelatedBodyDependencyProof(b *testing.B) {
	for _, query := range []string{
		"MATCH (b:Target) RETURN count(b) AS c",
		"MATCH (b:Target) RETURN *",
		"MATCH (b:Target) RETURN COUNT { RETURN * } AS c",
	} {
		b.Run(query, func(b *testing.B) {
			store := storage.NewMemoryEngine()
			b.Cleanup(func() { _ = store.Close() })
			exec := NewStorageExecutor(store)
			exec.fabricRecordBindings = map[string]interface{}{"a": int64(1)}
			clauses, ok := canExecuteAsPipeline(query)
			if !ok {
				b.Fatal(query)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exec.pipelineReadsImportedBinding(context.Background(), clauses)
			}
		})
	}
}
