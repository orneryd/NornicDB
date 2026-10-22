package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// sweepEntityHelper runs pass once to count its context observations, then
// cancels it at each of them: wherever the cancellation lands, the pass
// either reports it or returns the incomplete value its caller fails.
func sweepEntityHelper(t *testing.T, name string, pass func(context.Context) error) {
	t.Helper()
	counting := newSweepCancellationContext(0)
	require.NoError(t, pass(counting), name)
	for at := 1; at <= counting.calls; at++ {
		ctx := newSweepCancellationContext(at)
		err := pass(ctx)
		if err != nil {
			require.True(t, errors.Is(err, context.Canceled), "%s: observation %d of %d: %v", name, at, counting.calls, err)
		}
	}
}

// The entity populations' owners observe the statement at every point a
// pass can stop: candidate seeks, joins, MERGE lookups, fast aggregations,
// chained matches and fast-path scans.
func TestMaterializedEntityPopulationHelpersSweepCancellation(t *testing.T) {
	executor, store, nodes, _ := entityFailureFixture(t)
	background := context.Background()
	for _, ddl := range []string{
		"CREATE INDEX entity_sweep_id FOR (n:P) ON (n.id)",
		"CREATE INDEX entity_sweep_k FOR (n:P) ON (n.k)",
		"CREATE CONSTRAINT entity_sweep_q_id FOR (n:Q) REQUIRE n.id IS UNIQUE",
	} {
		_, err := executor.Execute(background, ddl, nil)
		require.NoError(t, err, ddl)
	}
	ps := []*storage.Node{nodes[0], nodes[2], nodes[4], nodes[6]}
	qs := []*storage.Node{nodes[1], nodes[3], nodes[5], nodes[7]}
	pattern := nodePatternInfo{variable: "n", labels: []string{"P"}}
	params := map[string]interface{}{"ids": []interface{}{int64(1), int64(2), int64(3)}, "one": int64(1), "two": int64(2)}

	type joinInput = []struct {
		variable string
		nodes    []*storage.Node
	}
	for _, join := range []struct {
		where string
		parts joinInput
	}{
		{"a.k = b.k", joinInput{{"a", ps}, {"b", ps}}},
		{"a.id = b.id + 1", joinInput{{"a", ps}, {"b", qs}}},
		{"a.k = b.k AND b.id = c.id", joinInput{{"a", ps}, {"b", ps}, {"c", qs}}},
		{"a.k = b.k", joinInput{{"a", ps}, {"b", ps}, {"c", qs}}},
	} {
		sweepEntityHelper(t, "join "+join.where, func(ctx context.Context) error {
			_, _, err := executor.buildCombinationsUsingWhereJoin(ctx, join.parts, join.where)
			return err
		})
	}

	for _, items := range [][]returnItem{
		{{expr: "q.name"}, {expr: "count(p)", alias: "c"}},
		{{expr: "q.name"}, {expr: "sum(p.id)", alias: "s"}},
		{{expr: "q.name"}, {expr: "avg(p.id)", alias: "a"}},
		{{expr: "q.name"}, {expr: "collect(p.id)", alias: "ids"}},
	} {
		for _, shape := range []string{"(q:Q)<-[:R]-(p:P)", "(p:P)-[:R]->(q:Q)"} {
			matches := executor.parseTraversalPattern(background, shape)
			require.NotNil(t, matches)
			sweepEntityHelper(t, "fast aggregation "+shape+" "+items[1].expr, func(ctx context.Context) error {
				_, _, err := executor.tryFastRelationshipAggregations(ctx, matches, items)
				return err
			})
		}
	}

	outgoing := executor.parseTraversalPattern(background, "(p:P)-[:R]->(q:Q)")
	require.NotNil(t, outgoing)
	for _, items := range [][]returnItem{
		{{expr: "p.name"}, {expr: "count(q)", alias: "c"}},
		{{expr: "p.name"}, {expr: "sum(q.id)", alias: "s"}},
		{{expr: "p.name"}, {expr: "avg(q.id)", alias: "a"}},
		{{expr: "p.name"}, {expr: "collect(q.id)", alias: "ids"}},
	} {
		sweepEntityHelper(t, "fast aggregation outgoing "+items[1].expr, func(ctx context.Context) error {
			_, _, err := executor.tryFastRelationshipAggregations(ctx, outgoing, items)
			return err
		})
	}
	sweepEntityHelper(t, "merge lookup without labels", func(ctx context.Context) error {
		_, err := executor.findMergeNode(ctx, store, nil, nil)
		return err
	})
	for _, join := range []struct {
		where string
		parts joinInput
	}{
		{"a.k = b.k", joinInput{{"a", ps}}},
		{"a.k = b.k", joinInput{{"", ps}, {"b", ps}}},
		{"a.k = b.k", joinInput{{"a", ps}, {"a", ps}}},
		{"a.k > b.k", joinInput{{"a", ps}, {"b", ps}}},
	} {
		_, ok, err := executor.buildCombinationsUsingWhereJoin(background, join.parts, join.where)
		require.NoError(t, err)
		require.False(t, ok, join.where)
	}
	paths := executor.traverseGraph(background, outgoing)
	require.NotEmpty(t, paths)
	sweepEntityHelper(t, "path aggregates", func(ctx context.Context) error {
		executor.aggregatePathCount(ctx, paths, outgoing, "count(DISTINCT q.id)")
		executor.aggregatePathCount(ctx, paths, outgoing, "count(q)")
		executor.filterPathsByWhere(ctx, paths, outgoing, "q.id > x.id", map[string]interface{}{"x": nodes[0], "e": paths[0].Relationships[0], "v": int64(1)})
		return nil
	})

	for _, props := range []map[string]interface{}{nil, {"id": int64(2)}, {"k": int64(1)}, {"id": int64(9)}} {
		sweepEntityHelper(t, "merge lookup", func(ctx context.Context) error {
			_, err := executor.findMergeNode(ctx, store, []string{"P"}, props)
			return err
		})
		sweepEntityHelper(t, "merge scan", func(ctx context.Context) error {
			_, _, err := executor.findMergeNodesScanned(ctx, store, []string{"P"}, props)
			return err
		})
	}
	sweepEntityHelper(t, "merge unique constraint", func(ctx context.Context) error {
		_, err := executor.findMergeNode(ctx, store, []string{"Q"}, map[string]interface{}{"id": int64(3)})
		return err
	})
	sweepEntityHelper(t, "inline index candidates", func(ctx context.Context) error {
		_, _, err := executor.lookupPatternCandidatesUsingPropertyIndex(ctx, nodePatternInfo{variable: "n", labels: []string{"P"}, properties: map[string]interface{}{"k": int64(1), "id": int64(1)}}, store)
		return err
	})

	for _, where := range []string{"n.id IN $ids OR n.k IN $ids", "n.id = $one OR n.k = $two", "n.id = 1 OR n.id = 2"} {
		sweepEntityHelper(t, "or seek "+where, func(ctx context.Context) error {
			if _, _, err := executor.tryCollectNodesFromPropertyIndexInOrParam(ctx, pattern, where, params); err != nil {
				return err
			}
			_, _, err := executor.tryCollectNodesFromPropertyIndexOrEquality(ctx, pattern, where, params)
			return err
		})
	}
	sweepEntityHelper(t, "in-literal seek", func(ctx context.Context) error {
		_, _, err := executor.tryCollectNodesFromPropertyIndexInLiteralAnyOf(ctx, pattern, "n.id IN [1, 2, 3]", nil)
		return err
	})
	sweepEntityHelper(t, "equality seek", func(ctx context.Context) error {
		_, _, err := executor.tryCollectNodesFromPropertyIndexAnyOf(ctx, pattern, "n.k = 1", nil)
		return err
	})
	sweepEntityHelper(t, "delete limit candidates", func(ctx context.Context) error {
		if _, _, err := executor.collectDeleteWithLimitCandidates(ctx, "MATCH (n:P) WHERE n.k = 1", "n", 2, nil); err != nil {
			return err
		}
		_, _, err := executor.collectDeleteWithLimitCandidates(ctx, "MATCH (n:P) WHERE n.k IN [0, 1]", "n", 2, nil)
		return err
	})
	sweepEntityHelper(t, "batch node index", func(ctx context.Context) error {
		rows := []map[string]interface{}{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}}
		_, _, err := executor.buildRelationshipBatchNodeMatchIndex(ctx, store, rows, []matchClauseSpec{{variable: "n", label: "P", propName: "id", rowField: "id"}, {variable: "m", label: "Q", propName: "id", rowField: "id"}})
		return err
	})
	sweepEntityHelper(t, "top-k", func(ctx context.Context) error {
		_, _, err := executor.selectTopKNodesByOrder(ctx, append(append([]*storage.Node{}, ps...), qs...), "n", "n.id DESC, n.k", 3)
		return err
	})
	for _, run := range []func(context.Context) (*ExecuteResult, error){
		func(ctx context.Context) (*ExecuteResult, error) {
			return executor.executeIncomingCountOptimized(ctx, "MATCH (a)-[:R]->(b) RETURN b.name, count(a)", PatternInfo{RelType: "R", Limit: 2})
		},
		func(ctx context.Context) (*ExecuteResult, error) {
			return executor.executeOutgoingCountOptimized(ctx, "MATCH (a)-[:R]->(b) RETURN a.name, count(b)", PatternInfo{RelType: "R"})
		},
		func(ctx context.Context) (*ExecuteResult, error) {
			return executor.executeEdgePropertyAggOptimized(ctx, "MATCH (a)-[r:R]->(b) RETURN b.name, avg(r.w)", PatternInfo{RelType: "R", AggProperty: "w", AggFunctions: []string{"avg", "sum", "min", "max", "count"}})
		},
	} {
		sweepEntityHelper(t, "fast path", func(ctx context.Context) error {
			_, err := run(ctx)
			return err
		})
	}

	bindings := []binding{{"p": nodes[0]}, {"p": nodes[2]}, {"q": nodes[1]}}
	for _, chained := range []string{"(p)-[:R]->(q)", "(p:P)-[:R]->(q:Q)-[:R]-(x)", "(q:Q)"} {
		sweepEntityHelper(t, "chained "+chained, func(ctx context.Context) error {
			executor.executeChainedMatch(ctx, chained, bindings, nil)
			executor.pipelineApplyChainedMatch(ctx, []pipelineRow{{"p": nodes[0], "e": paths[0].Relationships[0]}, {"p": nodes[2], "e": paths[1].Relationships[0]}}, "MATCH "+chained)
			return nil
		})
	}
	sweepEntityHelper(t, "streaming projection", func(ctx context.Context) error {
		var collected []*storage.Node
		if err := executor.visitNodesWithStreamingProjection(ctx, []string{"P"}, nil, "n", "n.id > 0", -1, nil, &collected, nil); err != nil {
			return err
		}
		return executor.visitNodesWithStreamingProjection(ctx, nil, map[string]interface{}{"k": int64(1)}, "n", "", 2, nil, &collected, nil)
	})
}

// The chained fast aggregations (two and three hops, linear and converging)
// stop at every observation of their whole-type relationship scans.
func TestMaterializedChainedAggregationSweepCancellation(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "chained_sweep")
	executor := NewStorageExecutor(store)
	create := func(id, label, key, value string) storage.NodeID {
		created, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{label}, Properties: map[string]interface{}{key: value}})
		require.NoError(t, err)
		return created
	}
	link := func(id, relType string, start, end storage.NodeID) {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(id), Type: relType, StartNode: start, EndNode: end, Properties: map[string]interface{}{}}))
	}
	for _, n := range []string{"1", "2", "3"} {
		customer := create("cust-"+n, "Customer", "companyName", "Cust"+n)
		order := create("ord-"+n, "Order", "orderNo", n)
		supplier := create("sup-"+n, "Supplier", "companyName", "Sup"+n)
		product := create("prod-"+n, "Product", "productName", "P"+n)
		category := create("cat-"+n, "Category", "categoryName", "C"+n)
		link("e-purchased-"+n, "PURCHASED", customer, order)
		link("e-orders-"+n, "ORDERS", order, product)
		link("e-supplies-"+n, "SUPPLIES", supplier, product)
		link("e-partof-"+n, "PART_OF", product, category)
	}
	// prod-1 joins several right nodes, so the inner passes have populations
	// of their own; the :Misc node fails every label filter it meets.
	extra := create("cat-x", "Category", "categoryName", "CX")
	misc := create("misc", "Misc", "companyName", "Misc")
	link("e-partof-x", "PART_OF", "prod-1", extra)
	link("e-partof-misc", "PART_OF", "prod-1", misc)
	link("e-supplies-misc", "SUPPLIES", misc, "prod-1")
	link("e-orders-misc", "ORDERS", "ord-1", misc)
	background := context.Background()
	for _, shape := range []struct {
		pattern string
		items   []returnItem
		rows    int
	}{
		{"(s:Supplier)-[:SUPPLIES]->(p:Product)-[:PART_OF]->(c:Category)", []returnItem{{expr: "s.companyName"}, {expr: "c.categoryName"}, {expr: "count(p)"}}, 4},
		{"(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)", []returnItem{{expr: "c.companyName"}, {expr: "cat.categoryName"}, {expr: "count(DISTINCT o)"}}, 4},
		{"(c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)<-[:SUPPLIES]-(s:Supplier)", []returnItem{{expr: "c.companyName"}, {expr: "s.companyName"}, {expr: "count(DISTINCT o)"}}, 3},
	} {
		matches := executor.parseTraversalPattern(background, shape.pattern)
		require.NotNil(t, matches)
		rows, ok, err := executor.tryFastRelationshipAggregations(background, matches, shape.items)
		require.NoError(t, err)
		require.True(t, ok, shape.pattern)
		require.Len(t, rows, shape.rows, shape.pattern)
		for _, row := range rows {
			require.NotEqual(t, "Misc", row[0], shape.pattern)
			require.NotEqual(t, "Misc", row[1], shape.pattern)
		}
		sweepEntityHelper(t, "chained aggregation "+shape.pattern, func(ctx context.Context) error {
			_, _, err := executor.tryFastRelationshipAggregations(ctx, matches, shape.items)
			return err
		})
	}
}

// The single-hop fast aggregation skips endpoints its labels reject and
// values that are missing or not numeric, and stops at every observation of
// its edge and group passes.
func TestMaterializedSingleHopAggregationSweepCancellation(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "single_hop_sweep")
	executor := NewStorageExecutor(store)
	create := func(id, label string, props map[string]interface{}) storage.NodeID {
		created, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{label}, Properties: props})
		require.NoError(t, err)
		return created
	}
	link := func(id string, start, end storage.NodeID, props map[string]interface{}) {
		require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(id), Type: "PART_OF", StartNode: start, EndNode: end, Properties: props}))
	}
	drinks := create("c1", "Category", map[string]interface{}{"categoryName": "Drinks"})
	empty := create("c2", "Category", map[string]interface{}{"categoryName": "Empty"})
	shelf := create("s1", "Shelf", map[string]interface{}{"categoryName": "Shelf"})
	p1 := create("p1", "Product", map[string]interface{}{"name": "a", "unitPrice": int64(1)})
	p2 := create("p2", "Product", map[string]interface{}{"name": "b", "unitPrice": float32(2)})
	p3 := create("p3", "Product", map[string]interface{}{"name": "c", "unitPrice": "n/a"})
	p4 := create("p4", "Product", map[string]interface{}{})
	other := create("o1", "Other", map[string]interface{}{"name": "o", "unitPrice": int64(100)})
	link("e1", p1, drinks, map[string]interface{}{"w": int64(1)})
	link("e2", p2, drinks, map[string]interface{}{"w": float32(2)})
	link("e3", p3, drinks, map[string]interface{}{"w": "heavy"})
	link("e4", p4, drinks, map[string]interface{}{})
	link("e5", other, drinks, map[string]interface{}{"w": int64(100)})
	link("e6", p1, shelf, map[string]interface{}{"w": int64(100)})
	link("e7", p4, empty, map[string]interface{}{})

	background := context.Background()
	matches := executor.parseTraversalPattern(background, "(c:Category)<-[r:PART_OF]-(p:Product)")
	require.NotNil(t, matches)
	aggregate := func(items ...string) map[interface{}][]interface{} {
		returnItems := []returnItem{{expr: "c.categoryName"}}
		for _, item := range items {
			returnItems = append(returnItems, returnItem{expr: item})
		}
		rows, ok, err := executor.tryFastRelationshipAggregations(background, matches, returnItems)
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, rows, 2)
		byName := map[interface{}][]interface{}{}
		for _, row := range rows {
			byName[row[0]] = row[1:]
		}
		sweepEntityHelper(t, "single-hop aggregation", func(ctx context.Context) error {
			_, _, err := executor.tryFastRelationshipAggregations(ctx, matches, returnItems)
			return err
		})
		return byName
	}
	sums := aggregate("count(p)", "sum(r.w)")
	require.Equal(t, []interface{}{int64(4), float64(3)}, sums["Drinks"])
	require.Equal(t, []interface{}{int64(1), float64(0)}, sums["Empty"])
	values := aggregate("count(p)", "avg(p.unitPrice)", "collect(p.name)")
	require.Equal(t, []interface{}{int64(4), float64(1.5)}, values["Drinks"][:2])
	require.ElementsMatch(t, []interface{}{"a", "b", "c"}, values["Drinks"][2])
	require.Equal(t, []interface{}{int64(1), nil}, values["Empty"][:2])
	require.Empty(t, values["Empty"][2])
}
