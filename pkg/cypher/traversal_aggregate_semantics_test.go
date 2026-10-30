package cypher

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// traversalAggregateRoutes is one relationship-pattern aggregation in three
// statement shapes: plain, behind a WHERE, and behind a WITH. match is the
// MATCH clause, returns the RETURN body.
func traversalAggregateRoutes(match, returns string) map[string]string {
	return map[string]string{
		"plain": match + " RETURN " + returns,
		"where": match + " WHERE true RETURN " + returns,
		"with":  match + " WITH * RETURN " + returns,
	}
}

// traversalAggregateRowKeys renders rows as sorted, typed text, so results
// compare without depending on group order, collect order or on 3 and 3.0
// looking alike.
func traversalAggregateRowKeys(rows [][]interface{}) []string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		parts := make([]string, len(row))
		for index, value := range row {
			parts[index] = traversalAggregateValueKey(value)
		}
		keys = append(keys, strings.Join(parts, ", "))
	}
	sort.Strings(keys)
	return keys
}

func traversalAggregateValueKey(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return "null"
	case string:
		return fmt.Sprintf("%q", typed)
	case int64:
		return fmt.Sprintf("%d", typed)
	case float64:
		return fmt.Sprintf("%gf", typed)
	case []interface{}:
		items := make([]string, len(typed))
		for index, item := range typed {
			items[index] = traversalAggregateValueKey(item)
		}
		sort.Strings(items)
		return "[" + strings.Join(items, ", ") + "]"
	default:
		return fmt.Sprintf("%T(%v)", value, value)
	}
}

func requireTraversalAggregateRows(t *testing.T, exec *StorageExecutor, match, returns string, want ...string) {
	t.Helper()
	want = append([]string{}, want...)
	sort.Strings(want)
	for route, query := range traversalAggregateRoutes(match, returns) {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err, "%s: %s", route, query)
		require.Equal(t, want, traversalAggregateRowKeys(result.Rows), "%s: %s", route, query)
	}
}

// Each aggregate of a grouped relationship pattern is calculated on its own,
// and a group whose rows have no value for the aggregated property is still
// returned, with the aggregates' empty values (Neo4j 5.26.30, NOR-70).
func TestTraversalAggregatesAreIndependentAndKeepEmptyGroups(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "traversal_aggregates"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (q:AggOwner {g: 'g'}), (p1:AggValue {y: 1}), (p2:AggValue {y: 2}), "+
		"(q)-[:AGG {x: 1, z: 10}]->(p1), (q)-[:AGG {x: 2, z: 20}]->(p2)", nil)
	require.NoError(t, err)

	match := "MATCH (q:AggOwner)-[r:AGG]->(p:AggValue)"
	requireTraversalAggregateRows(t, exec, match, "q.g, sum(r.x), avg(p.y)", `"g", 3, 1.5f`)
	requireTraversalAggregateRows(t, exec, match, "q.g, sum(r.x), sum(r.z), count(p), collect(p.y)", `"g", 3, 30, 2, [1, 2]`)
	requireTraversalAggregateRows(t, exec, match, "q.g, avg(p.y), sum(r.x), count(*)", `"g", 1.5f, 3, 2`)
	requireTraversalAggregateRows(t, exec, match, "q.g, min(p.y), max(r.x), count(DISTINCT p.y), stDev(p.y)", `"g", 1, 2, 2, 0.7071067811865476f`)
	requireTraversalAggregateRows(t, exec, "MATCH (q:AggOwner)<-[r:AGG]-(p:AggValue)", "q.g, count(p)")

	_, err = exec.Execute(ctx, "MATCH (p:AggValue) REMOVE p.y", nil)
	require.NoError(t, err)
	requireTraversalAggregateRows(t, exec, match, "q.g, avg(p.y), collect(p.y)", `"g", null, []`)
	requireTraversalAggregateRows(t, exec, match, "q.g, sum(p.y), collect(p.y), count(p.y), min(p.y)", `"g", 0, [], 0, null`)
	requireTraversalAggregateRows(t, exec, match, "q.g, sum(r.missing), avg(r.missing), count(p)", `"g", 0, null, 2`)
}

// Rows are grouped by the grouping values, not by the node that holds them:
// owners sharing a value form one group, 1 and '1' are different groups, and
// owners without the property share the null group (Neo4j 5.26.30, NOR-70).
func TestTraversalAggregatesGroupByValue(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "traversal_aggregate_groups"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (q:AggOwner {g: 'g'}), (p1:AggValue {y: 1}), (p2:AggValue {y: 2}), "+
		"(q)-[:AGG {x: 1}]->(p1), (q)-[:AGG {x: 2}]->(p2), "+
		"(:AggOwner {g: 'g'})-[:AGG {x: 5}]->(:AggValue {y: 7}), "+
		"(:AggOwner {g: 1})-[:AGG {x: 1}]->(:AggValue {y: 1}), "+
		"(:AggOwner {g: '1'})-[:AGG {x: 1}]->(:AggValue {y: 1}), "+
		"(:AggOwner)-[:AGG {x: 3}]->(:AggValue {y: 3}), "+
		"(:AggOwner)-[:AGG {x: 4}]->(:AggValue {y: 4})", nil)
	require.NoError(t, err)

	requireTraversalAggregateRows(t, exec, "MATCH (q:AggOwner)-[r:AGG]->(p:AggValue)", "q.g, count(p), sum(r.x), collect(p.y)",
		`"g", 3, 8, [1, 2, 7]`, `1, 1, 1, [1]`, `"1", 1, 1, [1]`, `null, 2, 7, [3, 4]`)
	requireTraversalAggregateRows(t, exec, "MATCH (p:AggValue)<-[r:AGG]-(q:AggOwner)", "p.y, count(q), sum(r.x)",
		`1, 3, 3`, `2, 1, 2`, `3, 1, 3`, `4, 1, 4`, `7, 1, 5`)
}

// The multi-hop shortcut groups by the projected values too, and counts an
// order reached from several customers once for each of them (Neo4j
// 5.26.30, NOR-70).
func TestTraversalChainedAggregatesGroupByValue(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "traversal_chained_groups"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Cust {name: 'C'})-[:BUY]->(o1:Ord)-[:HAS]->(pr:Prod {name: 'P'})-[:IN]->(:Cat {name: 'K'}), "+
		"(:Cust {name: 'C'})-[:BUY]->(o2:Ord)-[:HAS]->(pr), (:Cust {name: 'D'})-[:BUY]->(o1)", nil)
	require.NoError(t, err)

	requireTraversalAggregateRows(t, exec, "MATCH (c:Cust)-[:BUY]->(o:Ord)-[:HAS]->(p:Prod)-[:IN]->(k:Cat)", "c.name, k.name, count(DISTINCT o)",
		`"C", "K", 2`, `"D", "K", 1`)
	requireTraversalAggregateRows(t, exec, "MATCH (c:Cust)-[:BUY]->(o:Ord)-[:HAS]->(p:Prod)", "c.name, p.name, count(o)",
		`"C", "P", 2`, `"D", "P", 1`)
}

// sum() and avg() over a value that is neither a number nor a duration fail
// the statement in every shape, as Neo4j 5.26.30 does.
func TestTraversalAggregateRejectsNonNumericSum(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "traversal_aggregate_errors"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:SOwner {g: 's'})-[:SAGG {x: 'text'}]->(:SValue {y: 'text'})", nil)
	require.NoError(t, err)
	for _, returns := range []string{"q.g, sum(p.y)", "q.g, sum(r.x)", "q.g, avg(p.y)"} {
		for route, query := range traversalAggregateRoutes("MATCH (q:SOwner)-[r:SAGG]->(p:SValue)", returns) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err, "%s: %s", route, query)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.TypeError", code, "%s: %s", route, query)
		}
	}
}

// requireTraversalOperatorRows hands the RETURN items straight to the
// relationship traversal operator, with and without a WHERE: its aggregates
// are the row pipeline's, as the statements' (NOR-70).
func requireTraversalOperatorRows(t *testing.T, exec *StorageExecutor, pattern, returns string, want ...string) {
	t.Helper()
	want = append([]string{}, want...)
	sort.Strings(want)
	for _, where := range []string{"", "true"} {
		ctx := withExpressionFailureSlot(context.Background())
		result, err := exec.executeMatchWithRelationshipsWithPathSeeded(ctx, pattern, where, returnItemsForTest(t.Context(), "RETURN "+returns), nil, nil, "", nil, -1)
		require.NoError(t, err, "%s WHERE %q RETURN %s", pattern, where, returns)
		require.Equal(t, want, traversalAggregateRowKeys(result.Rows), "%s WHERE %q RETURN %s", pattern, where, returns)
	}
}

func TestTraversalOperatorAggregatesThroughRowPipeline(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "traversal_operator_aggregates"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (q:AggOwner {g: 'g'}), (p1:AggValue {y: 1}), (p2:AggValue {y: 2}), "+
		"(q)-[:AGG {x: 1, z: 10}]->(p1), (q)-[:AGG {x: 2, z: 20}]->(p2)", nil)
	require.NoError(t, err)
	pattern := "(q:AggOwner)-[r:AGG]->(p:AggValue)"
	requireTraversalOperatorRows(t, exec, pattern, "q.g, sum(r.x), avg(p.y)", `"g", 3, 1.5f`)
	requireTraversalOperatorRows(t, exec, pattern, "q.g, sum(r.x), sum(r.z), count(p), collect(p.y)", `"g", 3, 30, 2, [1, 2]`)
	requireTraversalOperatorRows(t, exec, pattern, "q.g, min(p.y), max(r.x), count(DISTINCT p.y), stDev(p.y)", `"g", 1, 2, 2, 0.7071067811865476f`)
	requireTraversalOperatorRows(t, exec, pattern, "sum(r.x), count(*)", `3, 2`)

	_, err = exec.Execute(ctx, "MATCH (p:AggValue) REMOVE p.y", nil)
	require.NoError(t, err)
	requireTraversalOperatorRows(t, exec, pattern, "q.g, avg(p.y), collect(p.y)", `"g", null, []`)
	requireTraversalOperatorRows(t, exec, pattern, "q.g, sum(p.y), count(p.y), min(p.y)", `"g", 0, 0, null`)

	_, err = exec.Execute(ctx, "CREATE (:AggOwner {g: 'g'})-[:AGG {x: 5}]->(:AggValue {y: 7}), "+
		"(:AggOwner {g: 1})-[:AGG {x: 1}]->(:AggValue), (:AggOwner {g: '1'})-[:AGG {x: 1}]->(:AggValue), "+
		"(:AggOwner)-[:AGG {x: 3}]->(:AggValue), (:AggOwner)-[:AGG {x: 4}]->(:AggValue)", nil)
	require.NoError(t, err)
	requireTraversalOperatorRows(t, exec, pattern, "q.g, count(p), sum(r.x)",
		`"g", 3, 8`, `1, 1, 1`, `"1", 1, 1`, `null, 2, 7`)

	_, err = exec.Execute(ctx, "CREATE (:Cust {name: 'C'})-[:BUY]->(o1:Ord)-[:HAS]->(pr:Prod {name: 'P'})-[:IN]->(:Cat {name: 'K'}), "+
		"(:Cust {name: 'C'})-[:BUY]->(o2:Ord)-[:HAS]->(pr), (:Cust {name: 'D'})-[:BUY]->(o1)", nil)
	require.NoError(t, err)
	requireTraversalOperatorRows(t, exec, "(c:Cust)-[:BUY]->(o:Ord)-[:HAS]->(p:Prod)-[:IN]->(k:Cat)", "c.name, k.name, count(DISTINCT o)",
		`"C", "K", 2`, `"D", "K", 1`)
	requireTraversalOperatorRows(t, exec, "(c:Cust)-[:BUY]->(o:Ord)-[:HAS]->(p:Prod)", "c.name, p.name, count(o)",
		`"C", "P", 2`, `"D", "P", 1`)

	_, err = exec.Execute(ctx, "CREATE (:SOwner {g: 's'})-[:SAGG {x: 'text'}]->(:SValue {y: 'text'})", nil)
	require.NoError(t, err)
	for _, where := range []string{"", "true"} {
		_, err := exec.executeMatchWithRelationshipsWithPathSeeded(withExpressionFailureSlot(ctx), "(q:SOwner)-[r:SAGG]->(p:SValue)", where,
			returnItemsForTest(t.Context(), "RETURN q.g, sum(r.x)"), nil, nil, "", nil, -1)
		require.Error(t, err, "WHERE %q", where)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.TypeError", code, "WHERE %q", where)
	}
}

// A grouped WITH over a relationship pattern keeps a group whose aggregated
// expression is null on every row: its sum is 0 (Neo4j 5.26.30, NOR-70).
func TestRelationshipRevenueAggregateKeepsEmptyGroups(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "relationship_revenue"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:Product {productName: 'A', unitPrice: 2.0}), (b:Product {productName: 'B'}), "+
		"(c:Product {productName: 'C', unitPrice: 2}), "+
		"(:Order)-[:ORDERS {quantity: 3}]->(a), (:Order)-[:ORDERS {quantity: 4}]->(a), "+
		"(:Order)-[:ORDERS {quantity: 5}]->(b), (:Order)-[:ORDERS {quantity: 3}]->(c)", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (p:Product)<-[r:ORDERS]-(:Order) WITH p, sum(p.unitPrice * r.quantity) AS revenue "+
		"RETURN p.productName, revenue", nil)
	require.NoError(t, err)
	require.Equal(t, []string{`"A", 14f`, `"B", 0`, `"C", 6`}, traversalAggregateRowKeys(result.Rows))
}

// Relationship patterns are aggregated by the row pipeline alone. Inventory
// from source: the relationship traversal operator's callers, each of which
// passes it bindings and never an aggregate; the aggregate calls of the
// operator itself, which only hand aggregate items to the pipeline; and the
// functions that create aggregate states. A new caller or a new aggregate
// owner must be added here on purpose, after checking it does not bring back
// a second implementation.
func TestRelationshipAggregationHasOneOwner(t *testing.T) {
	wantCallers := map[string]bool{"pipelineApplySeededTraversalMatch": true, "tryExecuteTraversalSeedOrderLimit": true}
	wantOperatorCalls := map[string]bool{"containsAggregateFunc": true, "aggregateTraversalOptionalRows": true}
	wantStates := map[string]bool{"pipelineAggregateGroups": true, "evaluatePipelineAggregateWithContext": true}
	callers, operatorCalls, states := map[string]bool{}, map[string]bool{}, map[string]bool{}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.CallExpr:
					called := ""
					switch target := typed.Fun.(type) {
					case *ast.Ident:
						called = target.Name
					case *ast.SelectorExpr:
						called = target.Sel.Name
					}
					if fn.Name.Name == "executeMatchWithRelationshipsWithPathSeeded" && strings.Contains(strings.ToLower(called), "agg") {
						operatorCalls[called] = true
					}
				case *ast.SelectorExpr:
					if typed.Sel.Name == "executeMatchWithRelationshipsWithPathSeeded" {
						callers[fn.Name.Name] = true
					}
				case *ast.CompositeLit:
					if ident, ok := typed.Type.(*ast.Ident); ok && ident.Name == "pipelineAggregateState" {
						states[fn.Name.Name] = true
					}
				}
				return true
			})
		}
	}
	require.Equal(t, wantCallers, callers, "the relationship traversal operator's callers changed; it must receive bindings only")
	require.Equal(t, wantOperatorCalls, operatorCalls, "the relationship traversal operator aggregates itself; hand aggregate items to aggregateTraversalOptionalRows")
	require.Equal(t, wantStates, states, "aggregate states are created outside the row pipeline's owner")
}
