package cypher

// Root-cause diagnostics round 3, campaign lane B (test-only; no product code
// changed): G-aggregate-groups, G-result-columns, G-static-clause-types.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestRCBEdgeAggOptimizerSteps(t *testing.T) {
	ctx := context.Background()
	query := "MATCH (c:Customer)-[r:REVIEWED]->(p:Product) RETURN p.name, avg(r.rating), count(r) ORDER BY p.name"
	info, err := DetectQueryPattern(ctx, query)
	t.Logf("RCB70 DETECT pattern=%v aggProperty=%q aggFunctions=%v err=%v", info.Pattern, info.AggProperty, info.AggFunctions, err)
	for index, fixture := range []struct{ name, create string }{
		{"with-unrated-relationships", `CREATE (c1:Customer {id: 1}), (c2:Customer {id: 2}), (c3:Customer {id: 3}), (c4:Customer {id: 4}),
			(p1:Product {name: 'P1'}), (p2:Product {name: 'P2'}), (p3:Product {name: 'P3'}),
			(c1)-[:REVIEWED {rating: 4}]->(p1), (c2)-[:REVIEWED {rating: 6}]->(p1), (c3)-[:REVIEWED]->(p2),
			(c1)-[:REVIEWED {rating: 2}]->(p3), (c4)-[:REVIEWED]->(p3)`},
		{"control-every-relationship-rated", `CREATE (c1:Customer {id: 1}), (c2:Customer {id: 2}), (c3:Customer {id: 3}), (c4:Customer {id: 4}),
			(p1:Product {name: 'P1'}), (p2:Product {name: 'P2'}), (p3:Product {name: 'P3'}),
			(c1)-[:REVIEWED {rating: 4}]->(p1), (c2)-[:REVIEWED {rating: 6}]->(p1), (c3)-[:REVIEWED {rating: 1}]->(p2),
			(c1)-[:REVIEWED {rating: 2}]->(p3), (c4)-[:REVIEWED {rating: 2}]->(p3)`},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), fmt.Sprintf("rcb70_%d", index)))
		if _, err := exec.Execute(ctx, fixture.create, nil); err != nil {
			t.Fatal(err)
		}
		general, err := exec.Execute(ctx, query, nil)
		if err != nil {
			t.Fatal(err)
		}
		detected, _ := exec.ExecuteOptimized(ctx, query, info)
		withCount := info
		withCount.AggFunctions = append(append([]string(nil), info.AggFunctions...), "count")
		variant, _ := exec.ExecuteOptimized(ctx, query, withCount)
		t.Logf("RCB70 %s general=%v", fixture.name, general.Rows)
		if detected != nil {
			t.Logf("RCB70 %s optimized(detected info)=%v", fixture.name, detected.Rows)
		}
		if variant != nil {
			t.Logf("RCB70 %s optimized(variant: count added to AggFunctions)=%v", fixture.name, variant.Rows)
		}
	}
}

func TestRCBEscapedHeaderRestoration(t *testing.T) {
	ctx := context.Background()
	for _, q := range []string{
		"RETURN {`a\\u0062`: 1} AS m, 1 AS `a\\u0062`",
		"WITH 'value' AS `a\\u0062` RETURN `a\\u0062`",
		"RETURN {`ab`: 1} AS m, 1 AS `ab`",
	} {
		_, source := cyphertext.WithSource(ctx, q)
		rewrite := sourceQueryRewrite(source)
		mapped, restored := "<no source rewrite>", "<no source rewrite>"
		if rewrite != nil {
			mapped = rewrite.originalText("ab")
			result, _ := (resultRestoration{source: rewrite}).restore(ctx, &ExecuteResult{Columns: []string{"m", "ab"}}, nil, nil)
			if result != nil {
				restored = fmt.Sprintf("%q", result.Columns)
			}
		}
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rcb167"))
		result, err := exec.Execute(ctx, q, nil)
		var columns, rows interface{}
		if result != nil {
			columns, rows = fmt.Sprintf("%q", result.Columns), result.Rows
		}
		t.Logf("RCB167 %q acceptedText=%q edits=%d originalText(ab)=%q restore([m ab])=%s | Execute columns=%v rows=%v err=%v",
			q, source.Text, len(source.Edits), mapped, restored, columns, rows, err)
	}
}

func TestRCBStaticCheckOwners(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		item, stmt string
		params     map[string]interface{}
	}{
		{"NOR-79 trigger (Neo4j accepts)", "UNWIND [] AS r MATCH (n) WITH n, [x IN $f | x] AS v RETURN v[0] AS a", map[string]interface{}{"f": 1.5}},
		{"NOR-79 control list parameter", "UNWIND [] AS r MATCH (n) WITH n, [x IN $f | x] AS v RETURN v[0] AS a", map[string]interface{}{"f": []interface{}{1.5}}},
		{"NOR-81 trigger (Neo4j accepts)", "UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a", nil},
		{"NOR-81 control literal integer (Neo4j rejects)", "UNWIND [] AS s RETURN any(x IN 5 WHERE true) AS a", nil},
		{"NOR-81 control list", "UNWIND [] AS s RETURN any(x IN [1] WHERE true) AS a", nil},
	} {
		err := validateListOperands(ctx, tc.stmt, tc.params)
		t.Logf("RCBSTATIC validateListOperands %s: %q -> %v", tc.item, tc.stmt, err)
	}
	for _, expression := range []string{
		"CASE WHEN COUNT { MATCH (m) } > 0 THEN n END",
		"CASE WHEN true THEN n END",
		"n",
	} {
		t.Logf("RCBSTATIC NOR-80 deleteExpressionRootIdentifier(%q)=%q", expression, deleteExpressionRootIdentifier(expression))
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "rcbstatic"))
	for _, tc := range []struct {
		stmt   string
		params map[string]interface{}
	}{
		{"UNWIND [] AS r MATCH (n) WITH n, [x IN $f | x] AS v RETURN v[0] AS a", map[string]interface{}{"f": 1.5}},
		{"UNWIND [] AS s MATCH (n) DELETE CASE WHEN COUNT { MATCH (m) } > 0 THEN n END", nil},
		{"UNWIND [] AS s MATCH (n) DELETE CASE WHEN true THEN n END", nil},
		{"UNWIND [] AS s RETURN any(x IN timestamp() WHERE true) AS a", nil},
		{"UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + date())) AS a", nil},
		{"UNWIND [] AS s MATCH (n) RETURN toUpper(head(n.k + [date()])) AS a", nil},
		{"UNWIND [] AS r MATCH (n) WITH n, $i AS v RETURN toUpper(v) AS a", map[string]interface{}{"i": int64(1)}},
		{"UNWIND [] AS r MATCH (n) RETURN toUpper($i) AS a", map[string]interface{}{"i": int64(1)}},
	} {
		_, err := exec.Execute(ctx, tc.stmt, tc.params)
		t.Logf("RCBSTATIC EXEC %q params=%v -> err=%v", tc.stmt, tc.params, err)
	}
}
