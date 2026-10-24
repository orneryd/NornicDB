package cypher

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// EXISTS and COUNT share the existing statement plan for the same body. A
// second statement gets its own syntax plan and reads current row values.
func TestExistsSubqueryEvaluationOwnerPlanReuse(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "exists_plan"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (:Source {id:'present'})", nil)
	require.NoError(t, err)
	body := "MATCH (s:Source {id:key})"
	var builds atomic.Int64
	hook := func(kind pipelineClauseAnalysis, text string) {
		if kind == clauseAnalysisBoundNode && text == body {
			builds.Add(1)
		}
	}
	old := pipelineClausePlanBuildHook.Swap(&hook)
	defer pipelineClausePlanBuildHook.Store(old)
	query := "UNWIND ['present','missing',null,'present'] AS key WITH key WHERE $nonce IS NOT NULL RETURN EXISTS { " + body + " } AS e, COUNT { " + body + " } AS c, COLLECT { " + body + " RETURN s.id } AS ids"
	for nonce := int64(1); nonce <= 2; nonce++ {
		result, err := e.Execute(ctx, query, map[string]any{"nonce": nonce})
		require.NoError(t, err)
		require.Equal(t, [][]any{
			{true, int64(1), []any{"present"}},
			{false, int64(0), []any{}},
			{false, int64(0), []any{}},
			{true, int64(1), []any{"present"}},
		}, result.Rows)
		require.Equal(t, nonce, builds.Load(), "parse once per statement, not once per row or expression kind")
	}
}

// A statement may have concurrent readers. All readers publish and use one
// parsed shape, and separate statements never share mutable plan state.
func TestExistsSubqueryEvaluationOwnerConcurrentPlan(t *testing.T) {
	ctx := withStatementClausePlans(context.Background(), "RETURN EXISTS { MATCH (s:Source {id:key}) }")
	var parses atomic.Int64
	var readers sync.WaitGroup
	for i := 0; i < 16; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			shape := clauseAnalysisFor(ctx, clauseAnalysisBoundNode, "MATCH (s:Source {id:key})", func(text string) boundNodeShape {
				parses.Add(1)
				return parseBoundNodeShape(text)
			})
			if !shape.ok || shape.variable != "s" {
				t.Error("reader observed an incomplete node lookup plan")
			}
		}()
	}
	readers.Wait()
	require.Equal(t, int64(1), parses.Load())
}
