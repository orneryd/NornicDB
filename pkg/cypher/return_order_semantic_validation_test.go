// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"testing"
)

func TestReturnOrderRejectsAggregateIntroducedAfterProjection(t *testing.T) {
	err := validateReturnOrderBySemanticScope(t.Context(), retainedClauseForTest(t.Context(), "RETURN node.value ORDER BY max(node.other)"))
	requireSemanticDetail(t, err, "InvalidAggregation")
}

func TestReturnDistinctOrderRejectsRemovedVariable(t *testing.T) {
	err := validateReturnOrderBySemanticScope(t.Context(), retainedClauseForTest(t.Context(), "RETURN DISTINCT node.name ORDER BY node.age"))
	requireSemanticDetail(t, err, "UndefinedVariable")
}

func TestReturnDistinctOrderAllowsPropertyOfProjectedEntity(t *testing.T) {
	err := validateReturnOrderBySemanticScope(t.Context(), retainedClauseForTest(t.Context(), "RETURN DISTINCT node ORDER BY node.age"))
	if err != nil {
		t.Fatalf("unexpected semantic error: %v", err)
	}
}

func TestReturnOrderAggregationRequiresDirectGroupingExpression(t *testing.T) {
	err := validateReturnOrderBySemanticScope(t.Context(), retainedClauseForTest(t.Context(), "RETURN left.age + right.age, count(*) AS total ORDER BY left.age + right.age + count(*)"))
	requireSemanticDetail(t, err, "AmbiguousAggregationExpression")
}

func TestReturnRejectsNestedAndNondeterministicAggregation(t *testing.T) {
	requireSemanticDetail(t, validateReturnAggregationSemantics(t.Context(), retainedClauseForTest(t.Context(), "RETURN "+"count(count(*))")), "NestedAggregation")
	requireSemanticDetail(t, validateReturnAggregationSemantics(t.Context(), retainedClauseForTest(t.Context(), "RETURN "+"count(rand())")), "NonConstantExpression")
}

func TestReturnAggregationRequiresExplicitGroupingProjection(t *testing.T) {
	requireSemanticDetail(t, validateReturnAggregationSemantics(t.Context(), retainedClauseForTest(t.Context(), "RETURN "+"node.age + count(other.age)")), "AmbiguousAggregationExpression")
	if err := validateReturnAggregationSemantics(t.Context(), retainedClauseForTest(t.Context(), "RETURN "+"node.age, node.age + count(other.age)")); err != nil {
		t.Fatalf("direct grouping projection should be valid: %v", err)
	}
}

func TestPipelinePreservesParameterSpellingInImplicitColumnName(t *testing.T) {
	columns := pipelineReturnSourceColumns(t.Context(), retainedClauseForTest(t.Context(), "RETURN $age + avg(person.age) - 1000"))
	if len(columns) != 1 || columns[0] != "$age + avg(person.age) - 1000" {
		t.Fatalf("unexpected source columns: %#v", columns)
	}
	executor := setupTestExecutor(t)
	ctx := withParams(context.Background(), map[string]interface{}{"age": int64(38)})
	query := "MATCH (person) RETURN $age + avg(person.age) - 1000"
	if clauses, ok := canExecuteAsPipeline(t.Context(), query); !ok {
		t.Fatalf("query was not accepted by pipeline decomposition: %#v", clauses)
	}
	outcome := executor.executePipeline(ctx, query)
	if !outcome.handled() || outcome.err != nil {
		t.Fatalf("pipeline execution failed: outcome=%+v", outcome)
	}
	result := outcome.result
	if len(result.Columns) != 1 || result.Columns[0] != "$age + avg(person.age) - 1000" {
		t.Fatalf("unexpected result columns: %#v", result.Columns)
	}
}
