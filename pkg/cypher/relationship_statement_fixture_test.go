package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"strconv"
	"strings"
	"testing"
)

// statementTestContext supplies the same dependency public dispatch supplies to
// private kernels. The fixture admits its actual source before invoking a
// kernel; no production reader can manufacture a missing program. Prepare this
// context before allocation/benchmark measurement, as public admission does.
func statementTestContext(ctx context.Context, query string) (context.Context, admittedRelationshipStatement) {
	return compileRelationshipStatement(ctx, query, nil)
}

func expressionTestContext(ctx context.Context, expression string) (context.Context, admittedRelationshipStatement) {
	return statementTestContext(ctx, "RETURN "+expression)
}

func expressionTestRow(ctx context.Context, expression string, values map[string]interface{}) (map[string]interface{}, admittedRelationshipStatement) {
	row := make(map[string]interface{}, len(values)+1)
	for name, value := range values {
		row[name] = value
	}
	ctx, statement := expressionTestContext(ctx, expression)
	row[rowStatementContextKey] = ctx
	return row, statement
}

// relationshipFixtureRow keeps each measured expression paired with the
// admission of that exact source. Preparation stays outside benchmark timing.
type relationshipFixtureRow struct {
	values    map[string]interface{}
	statement admittedRelationshipStatement
}

func expressionTestRows(ctx context.Context, expressions []string, values map[string]interface{}) []relationshipFixtureRow {
	rows := make([]relationshipFixtureRow, len(expressions))
	for index, expression := range expressions {
		rows[index].values, rows[index].statement = expressionTestRow(ctx, expression, values)
	}
	return rows
}

// Admission rejection is checked against real package declarations and bodies.
func requireRelationshipAdmissionBuildRejected(t *testing.T, expression, target string) {
	t.Helper()
	problems := relationshipAdmissionCompileErrors(t, "var _ = "+expression)
	for _, problem := range problems {
		if strings.Contains(problem.Msg, target) {
			return
		}
	}
	t.Fatalf("missing admission must fail the real %s build contract: %v", target, problems)
}
func traversalTestContext(ctx context.Context, pattern, where string, items []returnItem) (context.Context, admittedRelationshipStatement) {
	query := "MATCH " + pattern
	if where != "" {
		query += " WHERE " + where
	}
	if len(items) != 0 {
		expressions := make([]string, len(items))
		for index, item := range items {
			expressions[index] = item.expr
		}
		query += " RETURN " + strings.Join(expressions, ", ")
	}
	return statementTestContext(ctx, query)
}

func nodePatternTestContext(ctx context.Context, pattern string) (context.Context, admittedRelationshipStatement) {
	pattern = strings.TrimSpace(pattern)
	if !strings.HasPrefix(pattern, "(") {
		pattern = "(" + pattern + ")"
	}
	return statementTestContext(ctx, "MATCH "+pattern+" RETURN 1")
}

func relationshipPatternTestContext(ctx context.Context, pattern string) (context.Context, admittedRelationshipStatement) {
	return statementTestContext(ctx, "MATCH ()"+pattern+"() RETURN 1")
}

// pipelineTestContext admits the complete clause source supplied to a raw
// pipeline fixture, including the reference path that bypasses dispatch.
func pipelineTestContext(ctx context.Context, clauses []pipelineClause) (context.Context, admittedRelationshipStatement) {
	text := make([]string, len(clauses))
	for index, clause := range clauses {
		text[index] = clause.text
	}
	return statementTestContext(ctx, strings.Join(text, " "))
}

// shortestTestContext supplies the source of a parsed path fixture before its
// cancellation/error kernel is entered, just as statement dispatch does.
func shortestTestContext(ctx context.Context, match *shortestPathMatch) (context.Context, admittedRelationshipStatement) {
	patterns := append([]string(nil), match.others...)
	patterns = append(patterns, match.pattern)
	query := "MATCH " + strings.Join(patterns, ", ")
	var predicates []string
	for _, predicate := range []string{match.endpointWhere, match.pathWhere, match.postWhere} {
		if predicate != "" {
			predicates = append(predicates, "("+predicate+")")
		}
	}
	if len(predicates) != 0 {
		query += " WHERE " + strings.Join(predicates, " AND ")
	}
	return statementTestContext(ctx, query)
}

// relationshipTestExecution prepares the declared source of a manually built
// traversal fixture. Both its admission and its access identity use that source.
func relationshipTestExecution(exec *StorageExecutor, ctx context.Context, pattern, where string, items []returnItem) relationshipAccessExecution {
	ctx, statement := traversalTestContext(ctx, pattern, where, items)
	return exec.relationshipExecutionFor(statement, ctx, pattern, where)
}

// procedureTestContext declares the same CALL source as an invocation whose
// arguments have already been resolved by the fixture. Values stay with the
// runtime argument owner; source admission never evaluates them.
func procedureTestContext(ctx context.Context, procedure string, arguments []interface{}) (context.Context, admittedRelationshipStatement) {
	slots := make([]string, len(arguments))
	for index := range slots {
		slots[index] = "$fixtureArgument" + strconv.Itoa(index)
	}
	return statementTestContext(ctx, "CALL "+procedure+"("+strings.Join(slots, ", ")+")")
}

// traverseChainTest declares a chain from the fixture's resolved hop model.
// Admission and execution use the same direction and relationship type set.
func traverseChainTest(exec *StorageExecutor, node *storage.Node, hops []relationshipHop, hopIndex int) bool {
	pattern := "()"
	for _, hop := range hops {
		types := make([]string, len(hop.relTypes))
		for index, name := range hop.relTypes {
			types[index] = "`" + strings.ReplaceAll(name, "`", "``") + "`"
		}
		relationship := "[]"
		if len(types) != 0 {
			relationship = "[:" + strings.Join(types, "|") + "]"
		}
		if hop.outgoing {
			pattern += "-" + relationship + "->()"
		} else {
			pattern += "<-" + relationship + "-()"
		}
	}
	ctx, statement := traversalTestContext(context.Background(), pattern, "", nil)
	return exec.relationshipExecutionFor(statement, ctx, pattern, "").traverseChain(node, hops, hopIndex)
}

// revenueTest admits the same declared pattern and projection operands that
// the raw aggregation fixture passes to the private kernel.
func revenueTest(exec *StorageExecutor, ctx context.Context, pattern string, match *TraversalMatch, withClause, returnClause, orderByClause string, skip, limit int) (*ExecuteResult, bool, error) {
	query := ""
	if pattern != "" {
		query = "MATCH " + pattern + " "
	}
	if withClause != "" {
		query += "WITH " + withClause + " "
	}
	query += "RETURN " + returnClause
	if orderByClause != "" {
		query += " ORDER BY " + orderByClause
	}
	ctx, statement := statementTestContext(ctx, query)
	return exec.relationshipExecutionFor(statement, ctx, pattern, "").tryFastRevenueByProduct(match, withClause, returnClause, orderByClause, skip, limit)
}

// relationshipAggregationTest admits the actual pattern and projection together.
// Aggregation fixtures enter through the same prepared execution frame as MATCH.
func relationshipAggregationTest(exec *StorageExecutor, ctx context.Context, pattern, where string, match *TraversalMatch, items []returnItem) ([][]interface{}, bool, error) {
	return relationshipTestExecution(exec, ctx, pattern, where, items).tryFastRelationshipAggregations(match, items)
}

// Each predicate fixture keeps its already admitted source and original context.
func relationshipPredicateTest(exec *StorageExecutor, statement admittedRelationshipStatement, ctx context.Context, node *storage.Node, variable, pattern string) bool {
	return exec.relationshipExecutionFor(statement, ctx, pattern, "").evaluateRelationshipPatternInWhere(ctx, node, variable, pattern)
}

// Optimized fixtures use the same whole-query admission and site selection as
// public optimized dispatch; no empty frame can stand in for a query source.
func relationshipOptimizedAggregationTest(t testing.TB, exec *StorageExecutor, ctx context.Context, query string, info PatternInfo) (*ExecuteResult, error) {
	t.Helper()
	ctx, statement := statementTestContext(ctx, query)
	run, admitted := exec.relationshipExecutionForQuery(statement, ctx, query)
	if !admitted {
		t.Fatal("optimized fixture requires its actual admitted relationship site")
	}
	return run.executeEdgePropertyAggOptimized(ctx, query, info)
}
