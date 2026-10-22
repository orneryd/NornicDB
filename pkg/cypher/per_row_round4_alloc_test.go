package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"runtime"
	"runtime/debug"
	"testing"
)

func TestPerRowRound4PredicateAllocationBudget(t *testing.T) {
	previous := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previous)
	runtime.GC()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "r4alloc"))
	ctx := withExpressionFailureSlot(context.Background())
	row := map[string]interface{}{"x": "a"}
	for _, tc := range []struct {
		expression string
		maximum    float64
	}{
		{"toLower(x) = 'a'", 10},
		{"toLower(x) = 'a' OR toLower(x) = 'b'", 10},
		{"toLower(x) = 'a' OR EXISTS { MATCH (n) }", 12},
	} {
		statement, fixtureAdmission801 := expressionTestContext(ctx, tc.expression)
		_ = fixtureAdmission801
		for range 10 {
			require.True(t, exec.evaluateRowPredicate(fixtureAdmission801, statement, tc.expression, row))
			require.NoError(t, getExpressionFailure(ctx))
		}
		allocations := testing.AllocsPerRun(100, func() { require.True(t, exec.evaluateRowPredicate(fixtureAdmission801, statement, tc.expression, row)) })
		t.Logf("%s: %g allocations per row; baseline limit %g", tc.expression, allocations, tc.maximum)
		if allocations > tc.maximum {
			t.Errorf("%s: got %g allocations, maximum %g", tc.expression, allocations, tc.maximum)
		}
	}
}
