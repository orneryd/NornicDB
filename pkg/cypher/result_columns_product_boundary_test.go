package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Effect-only observers may inspect unresolved syntax. That must never make
// its inputs bound, authorize execution, or substitute another source's plan.
func TestResultHeaderBoundProductBoundary(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "product-boundary"))
	ctx := context.Background()
	clauses, planned := pipelineClausesFor("RETURN value")
	require.True(t, planned)
	_, err := executionClauseEffects(ctx, clauses[0])
	require.NoError(t, err, "effect observation does not require a bound variable")
	_, err = executionReturnProjection(ctx, clauses[0])
	require.Error(t, err, "effect observation is not binding admission")
	require.Nil(t, executionCallFacts(ctx, clauses[0]))
	callClauses, planned := pipelineClausesFor("CALL { CREATE (:BoundProductGuard) }")
	require.True(t, planned)
	callContext, err := executor.bindPipelineProgram(ctx, callClauses, map[string]struct{}{})
	require.NoError(t, err)
	callFacts := executionCallFacts(callContext, callClauses[0])
	require.NotNil(t, callFacts)
	require.Equal(t, resultmetadata.CallUnit, callFacts.Kind)
	require.Nil(t, executionCallFacts(callContext, clauses[0]), "CALL facts cannot cross source products")
	_, _, callHandled, failure := executor.pipelineApplyCallSubqueryWithMetadata(ctx, []pipelineRow{{}}, callClauses[0].text, nil)
	require.True(t, callHandled)
	require.Error(t, failure, "CALL execution requires admitted facts before effects")
	empty, err := executor.bindPipelineProgram(ctx, nil, nil)
	require.NoError(t, err)
	require.Equal(t, ctx, empty)

	bound, err := executor.bindPipelineProgram(ctx, clauses, map[string]struct{}{"value": {}})
	require.NoError(t, err)
	projection, err := executionReturnProjection(bound, clauses[0])
	require.NoError(t, err)
	require.Equal(t, []string{"value"}, projection.columns)
	other, planned := pipelineClausesFor("RETURN 2 AS otherValue")
	require.True(t, planned)
	_, err = executionReturnProjection(bound, other[0])
	require.Error(t, err, "node identities cannot cross source products")
	unit, planned := pipelineClausesFor("CREATE (:BoundProductGuard)")
	require.True(t, planned)
	unitContext, err := executor.bindPipelineProgram(ctx, unit, map[string]struct{}{})
	require.NoError(t, err)
	_, err = executionReturnProjection(unitContext, unit[0])
	require.Error(t, err, "a unit clause has no projection program")

	raw := []pipelineClause{{kind: pipelineClauseCreate, text: "CREATE (:BoundProductGuard)"}}
	_, handled, err := executor.runPipelineClauseRows(ctx, []pipelineRow{{}}, map[string]struct{}{}, raw, raw, nil)
	require.True(t, handled)
	require.Error(t, err, "an unbound operator cannot run from raw text")
	query := "CREATE (:BoundProductGuard) RETURN canonicalBoundaryMissing"
	_, err = pipelineStatementEffectsFor(ctx, query)
	require.NoError(t, err)
	executor.StatementFacts(query, resultmetadata.KnownScope(nil), false).Visit(func(resultmetadata.Statement) {
		t.Fatal("a warm effect cache became complete scope facts")
	}, func() { t.Fatal("binding declined without a continuation") }, func(reason resultmetadata.Unresolved) {
		require.Equal(t, resultmetadata.ReasonUndefinedVariable, reason.Reason)
	})
	for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
		_, err = executor.Execute(ctx, mode+query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	result, err := executor.Execute(ctx, "MATCH (n:BoundProductGuard) RETURN count(n) AS total", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

// Legacy scope consumers preserve unknown versus known-empty inputs instead
// of expanding a wildcard into a falsely authoritative empty scope.
func TestResultHeaderUnknownProjectionScope(t *testing.T) {
	star := returnProjectionPlanFor("RETURN *")
	require.Nil(t, projectionPlanScope(star, nil))
	knownEmpty := projectionPlanScope(star, map[string]struct{}{})
	require.NotNil(t, knownEmpty)
	require.Empty(t, knownEmpty)
	require.Nil(t, pipelineProjectionScope(nil, "WITH *", nil))
	invalid := returnProjectionPlanFor("RETURN )")
	require.Nil(t, projectionPlanScope(invalid, map[string]struct{}{}))
}
