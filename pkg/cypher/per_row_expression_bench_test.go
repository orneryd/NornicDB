package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// perRowKeys: 200 strings none of the relationships' keys match.
func perRowKeys() []interface{} {
	keys := make([]interface{}, 200)
	for i := range keys {
		keys[i] = fmt.Sprintf("nomatch-%03d", i)
	}
	return keys
}

// perRowDocGraph: (:Doc)-[:REL]->(:Mention)-[:REL]->(:Thing) for 150
// documents, and every document -[:REL]-> one (:Case): 450 relationships.
func perRowDocGraph(b *testing.B) *StorageExecutor {
	b.Helper()
	base := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "bench")
	nodes := []*storage.Node{{ID: "case", Labels: []string{"Case"}, Properties: map[string]interface{}{"id": "case"}}}
	edges := make([]*storage.Edge, 0, 450)
	for i := 0; i < 150; i++ {
		doc, mention, thing := fmt.Sprintf("d%d", i), fmt.Sprintf("m%d", i), fmt.Sprintf("t%d", i)
		nodes = append(nodes,
			&storage.Node{ID: storage.NodeID(doc), Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": doc}},
			&storage.Node{ID: storage.NodeID(mention), Labels: []string{"Mention"}, Properties: map[string]interface{}{"id": mention}},
			&storage.Node{ID: storage.NodeID(thing), Labels: []string{"Thing"}, Properties: map[string]interface{}{"id": thing}},
		)
		origin := "manual"
		if i%3 == 0 {
			origin = "automatic"
		}
		edges = append(edges,
			&storage.Edge{ID: storage.EdgeID("dm" + doc), Type: "REL", StartNode: storage.NodeID(doc), EndNode: storage.NodeID(mention), Properties: map[string]interface{}{"kind": "mentions", "origin": origin, "stage": "extraction"}},
			&storage.Edge{ID: storage.EdgeID("mt" + doc), Type: "REL", StartNode: storage.NodeID(mention), EndNode: storage.NodeID(thing), Properties: map[string]interface{}{"kind": "refers", "stage": "interpretation"}},
			&storage.Edge{ID: storage.EdgeID("dc" + doc), Type: "REL", StartNode: storage.NodeID(doc), EndNode: "case", Properties: map[string]interface{}{"kind": "in_case", "origin": origin}},
		)
	}
	if err := store.BulkCreateNodes(nodes); err != nil {
		b.Fatal(err)
	}
	if err := store.BulkCreateEdges(edges); err != nil {
		b.Fatal(err)
	}
	return NewStorageExecutor(store)
}

// perRowWideGraph: 990 nodes, each with two outgoing :T: 1,980 relationships.
func perRowWideGraph(b *testing.B) *StorageExecutor {
	b.Helper()
	base := storage.NewMemoryEngine()
	b.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "bench")
	const n = 990
	nodes := make([]*storage.Node, 0, n)
	edges := make([]*storage.Edge, 0, 2*n)
	for i := 0; i < n; i++ {
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"N"}, Properties: map[string]interface{}{"name": fmt.Sprintf("n%d", i), "v": int64(i % 17)}})
	}
	for i := 0; i < n; i++ {
		for j, step := range []int{1, 7} {
			edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d_%d", i, j)), Type: "T", StartNode: storage.NodeID(fmt.Sprintf("n%d", i)), EndNode: storage.NodeID(fmt.Sprintf("n%d", (i+step)%n)), Properties: map[string]interface{}{"kind": fmt.Sprintf("k%d", i%5), "w": int64(i % 11), "origin": []string{"manual", "automatic"}[i%2], "stage": []string{"extraction", "interpretation", "review"}[i%3]}})
		}
	}
	if err := store.BulkCreateNodes(nodes); err != nil {
		b.Fatal(err)
	}
	if err := store.BulkCreateEdges(edges); err != nil {
		b.Fatal(err)
	}
	return NewStorageExecutor(store)
}

// perRowLongPredicate builds a predicate of about 10,000 characters out of
// EXISTS subqueries, list comprehensions and CASE, that is false on every row.
func perRowLongPredicate() string {
	var parts []string
	total := 0
	for i := 0; total < 10000; i++ {
		part := fmt.Sprintf("(EXISTS { MATCH (s)-[:T]->(x) WHERE x.v = %d AND x.name = 'zz%d' } OR size([k IN $keys WHERE k = r.kind + '-%d' | k]) > 0 OR CASE WHEN r.w = %d THEN s.name = 'zz' ELSE false END)", 100+i, i, i, 100+i)
		parts = append(parts, part)
		total += len(part) + 4
	}
	return strings.Join(parts, " OR ")
}

func runPerRowBench(b *testing.B, exec *StorageExecutor, query string, params map[string]interface{}, want int64) {
	b.Helper()
	ctx := context.Background()
	run := func(nonce int) {
		p := make(map[string]interface{}, len(params)+1)
		for k, v := range params {
			p[k] = v
		}
		// A different parameter on every run: the result cache can't serve it.
		p["nonce"] = int64(nonce)
		res, err := exec.Execute(ctx, query, p)
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
			b.Fatalf("unexpected rows %v", res.Rows)
		}
		if want >= 0 && res.Rows[0][0] != want {
			b.Fatalf("got %v, want %d", res.Rows[0][0], want)
		}
	}
	run(-1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run(i)
	}
}

// The predicates of #1214 over 450 relationships with a 200-string $keys.
func BenchmarkPerRowExpression_Rel450(b *testing.B) {
	exec := perRowDocGraph(b)
	key := "startNode(r).id + '|' + endNode(r).id + '|' + r.kind"
	params := map[string]interface{}{"keys": perRowKeys()}
	for _, tc := range []struct {
		name, query string
	}{
		{"true", "MATCH ()-[r:REL]->() WHERE true RETURN count(*)"},
		{"property", "MATCH ()-[r:REL]->() WHERE r.kind <> 'zz' RETURN count(*)"},
		{"startNode", "MATCH ()-[r:REL]->() WHERE startNode(r).id <> 'zz' RETURN count(*)"},
		{"concat", "MATCH ()-[r:REL]->() WHERE " + key + " <> 'zz' RETURN count(*)"},
		{"any_const", "MATCH ()-[r:REL]->() WHERE NOT any(k IN $keys WHERE k = 'zz') RETURN count(*)"},
		{"any_startNode", "MATCH ()-[r:REL]->() WHERE NOT any(k IN $keys WHERE k = startNode(r).id) RETURN count(*)"},
		{"any_concat", "MATCH ()-[r:REL]->() WHERE NOT any(k IN $keys WHERE k = " + key + ") RETURN count(*)"},
		{"bound_key", "MATCH ()-[r:REL]->() WITH r, " + key + " AS key WHERE NOT any(k IN $keys WHERE k = key) RETURN count(*)"},
		{"not_in", "MATCH ()-[r:REL]->() WHERE NOT " + key + " IN $keys RETURN count(*)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runPerRowBench(b, exec, tc.query, params, 450)
		})
	}
}

// The predicates of #1214 over 1,980 relationships.
func BenchmarkPerRowExpression_Rel1980(b *testing.B) {
	exec := perRowWideGraph(b)
	params := map[string]interface{}{"keys": perRowKeys()}
	long := perRowLongPredicate()
	ors := make([]string, 200)
	for i := range ors {
		ors[i] = "r.zz = 1"
	}
	for _, tc := range []struct {
		name, predicate string
		want            int64
	}{
		{"few_properties", "(r.kind <> 'zz' AND s.name <> 'zz' AND t.name <> 'zz' AND coalesce(r.w, 0) >= 0 AND r.missing IS NULL AND s.v >= 0 AND t.v >= 0 AND r.kind STARTS WITH 'k')", 1980},
		{"or_200", strings.Join(ors, " OR "), 0},
		{"long", long, 0},
		{"true_or_long", "true OR (" + long + ")", 1980},
		{"false_and_long", "false AND (" + long + ")", 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			query := "MATCH (s)-[r:T]->(t) WHERE " + tc.predicate + " RETURN count(r)"
			runPerRowBench(b, exec, query, params, tc.want)
		})
	}
}

// A shortest-path search over a quantified path pattern, with and without an
// inline relationship predicate.
func BenchmarkPerRowExpression_QPPShortest(b *testing.B) {
	exec := perRowDocGraph(b)
	for _, shape := range []struct{ name, head, tail string }{
		{"from_one", "MATCH (s:Doc {id: 'd1'}) MATCH p = SHORTEST 1 (s)", "(t) RETURN count(p)"},
		{"pair", "MATCH (s:Doc {id: 'd1'}), (t:Thing {id: 't7'}) MATCH p = SHORTEST 1 (s)", "(t) RETURN count(p)"},
		{"all_docs_things", "MATCH p = SHORTEST 1 (s:Doc)", "(t:Thing) RETURN count(p)"},
	} {
		for _, tc := range []struct{ name, rel string }{
			{"plain", "[r:REL]"},
			{"inline_where", "[r:REL WHERE coalesce(r.origin,'') <> 'automatic' OR r.stage = 'interpretation']"},
		} {
			b.Run(shape.name+"/"+tc.name, func(b *testing.B) {
				query := shape.head + "(()-" + tc.rel + "-()){1,4}" + shape.tail
				runPerRowBench(b, exec, query, nil, -1)
			})
		}
	}
}

// The same searches over the 1,980-relationship graph.
func BenchmarkPerRowExpression_QPPShortestWide(b *testing.B) {
	exec := perRowWideGraph(b)
	for _, shape := range []struct{ name, head, tail string }{
		{"from_one", "MATCH (s:N {name: 'n0'}) MATCH p = SHORTEST 1 (s)", "(t) RETURN count(p)"},
		{"pair", "MATCH (s:N {name: 'n0'}), (t:N {name: 'n30'}) MATCH p = SHORTEST 1 (s)", "(t) RETURN count(p)"},
	} {
		for _, tc := range []struct{ name, rel string }{
			{"plain", "[r:T]"},
			{"inline_where", "[r:T WHERE coalesce(r.origin,'') <> 'automatic' OR r.stage = 'interpretation']"},
		} {
			b.Run(shape.name+"/"+tc.name, func(b *testing.B) {
				query := shape.head + "(()-" + tc.rel + "-()){1,4}" + shape.tail
				runPerRowBench(b, exec, query, nil, -1)
			})
		}
	}
}

// The list predicate of #1214 in other clause positions, which other
// evaluators run: a node match's WHERE, a projection, a comprehension in an
// aggregate, and EXISTS.
func BenchmarkPerRowExpression_Positions(b *testing.B) {
	exec := perRowDocGraph(b)
	params := map[string]interface{}{"keys": perRowKeys()}
	for _, tc := range []struct {
		name, query string
		want        int64
	}{
		{"node_where", "MATCH (d:Doc) WHERE NOT any(k IN $keys WHERE k = d.id + '|') RETURN count(*)", 150},
		{"return_case", "MATCH ()-[r:REL]->() RETURN count(CASE WHEN NOT any(k IN $keys WHERE k = startNode(r).id) THEN 1 END)", 450},
		{"return_comprehension", "MATCH ()-[r:REL]->() RETURN sum(size([k IN $keys WHERE k = r.kind]))", 0},
		{"with_where", "MATCH ()-[r:REL]->() WITH r WHERE NOT any(k IN $keys WHERE k = startNode(r).id) RETURN count(*)", 450},
		{"exists_where", "MATCH (d:Doc) WHERE EXISTS { MATCH (d)-[r:REL]->(m) WHERE NOT any(k IN $keys WHERE k = m.id) } RETURN count(*)", 150},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runPerRowBench(b, exec, tc.query, params, tc.want)
		})
	}
}

// The context evaluator's list predicate (evaluateQuantifierWithContext),
// which the shared evaluator runs for a list predicate over node and
// relationship scopes.
func BenchmarkPerRowExpression_ContextEvaluatorQuantifier(b *testing.B) {
	exec := perRowDocGraph(b)
	ctx := withQueryParams(context.Background(), map[string]interface{}{"keys": perRowKeys()})
	node, err := exec.storage.GetNode("d1")
	if err != nil {
		b.Fatal(err)
	}
	nodes := map[string]*storage.Node{"n": node}
	const expr = "any(k IN $keys WHERE k = n.id + '|')"
	if value := exec.evaluateExpressionWithContext(ctx, expr, nodes, nil); value != false {
		b.Fatalf("got %v", value)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.evaluateExpressionWithContext(ctx, expr, nodes, nil)
	}
}

// The list predicates of #1214 other than any(), over 450 relationships.
func BenchmarkPerRowExpression_ListForms(b *testing.B) {
	exec := perRowDocGraph(b)
	params := map[string]interface{}{"keys": perRowKeys()}
	for _, tc := range []struct {
		name, predicate string
	}{
		{"none", "none(k IN $keys WHERE k = startNode(r).id + '|' + r.kind)"},
		{"all", "all(k IN $keys WHERE k <> startNode(r).id + '|' + r.kind)"},
		{"single", "NOT single(k IN $keys WHERE k = r.kind)"},
		{"comprehension", "size([k IN $keys WHERE k = startNode(r).id + '|' + r.kind | k]) = 0"},
		{"reduce", "NOT reduce(found = false, k IN $keys | found OR k = startNode(r).id + '|' + r.kind)"},
		{"concat_compare", "startNode(r).id + '|' + endNode(r).id + '|' + r.kind <> r.kind"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runPerRowBench(b, exec, "MATCH ()-[r:REL]->() WHERE "+tc.predicate+" RETURN count(*)", params, 450)
		})
	}
	// Projections, which the projection validators walk for every row.
	for _, tc := range []struct {
		name, query string
		want        int64
	}{
		{"with_comprehension", "MATCH ()-[r:REL]->() WITH r, [k IN $keys WHERE k = r.kind] AS hits RETURN count(hits)", 450},
		{"return_reduce", "MATCH ()-[r:REL]->() RETURN sum(reduce(n = 0, k IN $keys | n + size(k)))", 450 * 200 * 11},
		{"return_conversion", "MATCH ()-[r:REL]->() RETURN sum(size([k IN $keys WHERE toString(k) = r.kind]))", 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runPerRowBench(b, exec, tc.query, params, tc.want)
		})
	}
}

// One evaluation of each operation of #1214 against one row, compiled: the
// cost and allocations per evaluation, next to a property comparison.
func BenchmarkPerRowExpression_Operation(b *testing.B) {
	exec := perRowDocGraph(b)
	edge, err := exec.storage.GetEdge("dmd1")
	if err != nil {
		b.Fatal(err)
	}
	row := map[string]interface{}{"r": edge, "$keys": perRowKeys()}
	for _, tc := range []struct{ name, expr string }{
		{"property_compare", "r.kind <> 'zz'"},
		{"startNode", "startNode(r)"},
		{"startNode_property", "startNode(r).id <> 'zz'"},
		{"concat", "startNode(r).id + '|' + endNode(r).id + '|' + r.kind"},
		{"any_200", "any(k IN $keys WHERE k = startNode(r).id + '|' + r.kind)"},
		{"none_200", "none(k IN $keys WHERE k = r.kind)"},
		{"all_200", "all(k IN $keys WHERE k <> r.kind)"},
		{"comprehension_200", "[k IN $keys WHERE k = r.kind | k]"},
		{"reduce_200", "reduce(found = false, k IN $keys | found OR k = r.kind)"},
		{"in_200", "startNode(r).id + '|' + r.kind IN $keys"},
		{"and_short_circuit", "false AND any(k IN $keys WHERE k = startNode(r).id)"},
		{"or_short_circuit", "true OR any(k IN $keys WHERE k = startNode(r).id)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			// The first evaluation marks the text, the second compiles it.
			for i := 0; i < 2; i++ {
				if _, ok, err := exec.evaluateRowValue(tc.expr, row); !ok || err != nil {
					b.Fatalf("%s: unresolved or %v", tc.expr, err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exec.evaluateRowValue(tc.expr, row)
			}
		})
	}
}
