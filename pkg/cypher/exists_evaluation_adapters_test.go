package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The legacy context adapter must preserve changing parameters and bound
// entities when it hands exists(pattern) to the shared subquery evaluator.
func TestExistsSubqueryEvaluationOwnerLegacyAdapter(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "exists_adapter"))
	result, err := e.Execute(context.Background(), "CREATE (n:Adapter)-[:REL {key:'present'}]->(:Adapter) RETURN n", nil)
	require.NoError(t, err)
	node, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	nodes := map[string]*storage.Node{"n": node}
	for _, key := range []string{"present", "missing", "present"} {
		ctx := withExpressionFailureSlot(withParams(context.Background(), map[string]any{"key": key}))
		value := e.evaluateExpressionWithContextFullFunctions(ctx, "exists((n)-[:REL {key:$key}]->())", nodes, nil, nil, nil, nil, 0)
		require.NoError(t, getExpressionFailure(ctx))
		require.Equal(t, key == "present", value)
	}
}

// A compiled subquery cannot invent a statement context or swallow a body
// error. These are the same contracts as the ordinary row value evaluator.
func TestExistsSubqueryEvaluationOwnerCompiledContextAndErrors(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "exists_compiled"))
	expression := "COLLECT { RETURN 1 / 0 AS invalid }"
	root, _ := (&rowValueCompiler{}).compile(expression)
	row := map[string]any{}
	want, wantOK, wantErr := e.evaluateRowValueText(expression, row)
	value, resolved, err := root.eval(e, row, nil)
	require.Equal(t, want, value)
	require.Equal(t, wantOK, resolved)
	require.Equal(t, wantErr, err)
	row[rowStatementContextKey] = withExpressionFailureSlot(context.Background())
	_, _, err = root.eval(e, row, nil)
	require.Error(t, err, "an inner arithmetic error must reach the caller")
}

// A cached lookup still spends the current statement's memory budget. A
// rejected evaluation cannot poison a later lookup; statement completion returns its charge.
func TestExistsSubqueryEvaluationOwnerMemoryBudget(t *testing.T) {
	resetTransactionMemory(t)
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "exists_budget"))
	_, err := e.Execute(context.Background(), "CREATE (:Source {id:'present'})", nil)
	require.NoError(t, err)
	for _, limit := range []int64{1, 1 << 20} {
		ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: limit})
		tracker := newTestMemoryTracker("exists_budget")
		t.Cleanup(tracker.end)
		ctx := withExpressionFailureSlot(context.WithValue(context.Background(), memoryScopeKey{}, &tracker.root))
		value, resolved, evalErr := e.rowSubqueryValue(ctx, "EXISTS", "MATCH (s:Source {id:key})", map[string]any{"key": "present"})
		require.NoError(t, evalErr)
		require.True(t, resolved)
		if limit == 1 {
			requireMemoryError(t, memoryFailureFromContext(ctx), transactionMemoryLimitCode, transactionMemoryMaxSetting)
		} else {
			require.NoError(t, getExpressionFailure(ctx))
			require.Equal(t, true, value)
		}
		tracker.end()
		requirePoolsEmpty(t, "exists_budget")
	}
}
