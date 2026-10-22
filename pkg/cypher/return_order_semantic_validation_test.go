package cypher

import (
	"context"
	"testing"
)

func TestReturnOrderRejectsAggregateIntroducedAfterProjection(t *testing.T) {
	err := validateReturnOrderBySemanticScope("RETURN node.value ORDER BY max(node.other)")
	requireSemanticDetail(t, err, "InvalidAggregation")
}

func TestReturnDistinctOrderRejectsRemovedVariable(t *testing.T) {
	err := validateReturnOrderBySemanticScope("RETURN DISTINCT node.name ORDER BY node.age")
	requireSemanticDetail(t, err, "UndefinedVariable")
}

func TestReturnDistinctOrderAllowsPropertyOfProjectedEntity(t *testing.T) {
	err := validateReturnOrderBySemanticScope("RETURN DISTINCT node ORDER BY node.age")
	if err != nil {
		t.Fatalf("unexpected semantic error: %v", err)
	}
}

func TestReturnOrderAggregationRequiresDirectGroupingExpression(t *testing.T) {
	err := validateReturnOrderBySemanticScope("RETURN left.age + right.age, count(*) AS total ORDER BY left.age + right.age + count(*)")
	requireSemanticDetail(t, err, "AmbiguousAggregationExpression")
}

func TestReturnRejectsNestedAndNondeterministicAggregation(t *testing.T) {
	requireSemanticDetail(t, validateReturnAggregationSemantics("count(count(*))"), "NestedAggregation")
	requireSemanticDetail(t, validateReturnAggregationSemantics("count(rand())"), "NonConstantExpression")
}

func TestReturnAggregationRequiresExplicitGroupingProjection(t *testing.T) {
	requireSemanticDetail(t, validateReturnAggregationSemantics("node.age + count(other.age)"), "AmbiguousAggregationExpression")
	if err := validateReturnAggregationSemantics("node.age, node.age + count(other.age)"); err != nil {
		t.Fatalf("direct grouping projection should be valid: %v", err)
	}
}

func TestReturnSourceColumnsPreserveParameterSpelling(t *testing.T) {
	columns := pipelineReturnSourceColumns("RETURN $age + avg(person.age) - 1000")
	if len(columns) != 1 || columns[0] != "$age + avg(person.age) - 1000" {
		t.Fatalf("unexpected source columns: %#v", columns)
	}
}

func TestPipelinePreservesParameterSpellingInImplicitColumnName(t *testing.T) {
	executor := setupTestExecutor(t)
	ctx := withParams(context.Background(), map[string]interface{}{"age": int64(38)})
	query := "MATCH (person) RETURN $age + avg(person.age) - 1000"
	if clauses, ok := canExecuteAsPipeline(query); !ok {
		t.Fatalf("query was not accepted by pipeline decomposition: %#v", clauses)
	}
	fixtureAdmission833Context, fixtureAdmission833 := statementTestContext(ctx, query)
	_ = fixtureAdmission833
	outcome := executor.executePipeline(fixtureAdmission833, fixtureAdmission833Context, query)
	if !outcome.handled() || outcome.err != nil {
		t.Fatalf("pipeline execution failed: outcome=%+v", outcome)
	}
	result := outcome.result
	if len(result.Columns) != 1 || result.Columns[0] != "$age + avg(person.age) - 1000" {
		t.Fatalf("unexpected result columns: %#v", result.Columns)
	}
}
