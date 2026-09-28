// SPDX-License-Identifier: MIT
package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Issue #638 e2e regression: every relationship-count shape the issue
// measured must return correct rows and be served by the O(1) counters —
// never by materializing edges of the store.

// getEdgesByTypeProbe records GetEdgesByType calls so the counter fast path
// can be asserted structurally (the materializing path would show up here).
type getEdgesByTypeProbe struct {
	storage.Engine
	calls int
}

type forwardingNamespacedEngine struct {
	storage.Engine
}

func (engine *forwardingNamespacedEngine) Namespace() string {
	return engine.Engine.(*storage.NamespacedEngine).Namespace()
}

func (engine *forwardingNamespacedEngine) GetInnerEngine() storage.Engine {
	return engine.Engine
}

func (p *getEdgesByTypeProbe) GetEdgesByType(edgeType string) ([]*storage.Edge, error) {
	p.calls++
	return p.Engine.GetEdgesByType(edgeType)
}

func newIssue638E2EStore(t *testing.T) (*storage.NamespacedEngine, *storage.BadgerEngine, *StorageExecutor, *getEdgesByTypeProbe) {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	probe := &getEdgesByTypeProbe{Engine: store}
	exec := NewStorageExecutor(probe)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		_, err = store.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("f%d", i)),
			Labels: []string{"Function"},
		})
		require.NoError(t, err)
	}
	_, err = exec.Execute(ctx, "UNWIND range(0,19) AS i CREATE (:Tick {n:i})-[:TICK]->(:Tick {n:i})", nil)
	require.NoError(t, err)

	const n = 8000
	callsEdges := make([]*storage.Edge, 0, n)
	linksEdges := make([]*storage.Edge, 0, n)
	calNodes := make([]*storage.Node, 0, n)
	xNodes := make([]*storage.Node, 0, n)
	yNodes := make([]*storage.Node, 0, n)
	for i := 0; i < n; i++ {
		callsEdges = append(callsEdges, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("c%d", i)),
			StartNode: storage.NodeID(fmt.Sprintf("f%d", i%20)),
			EndNode:   storage.NodeID(fmt.Sprintf("cal%d", i)),
			Type:      "CALLS",
		})
		linksEdges = append(linksEdges, &storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("l%d", i)),
			StartNode: storage.NodeID(fmt.Sprintf("x%d", i)),
			EndNode:   storage.NodeID(fmt.Sprintf("y%d", i)),
			Type:      "LINKS",
		})
		calNodes = append(calNodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("cal%d", i)), Labels: []string{"Callee"}})
		xNodes = append(xNodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("x%d", i))})
		yNodes = append(yNodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("y%d", i))})
	}
	for start := 0; start < n; start += 2000 {
		end := min(start+2000, n)
		require.NoError(t, store.BulkCreateNodes(calNodes[start:end]))
		require.NoError(t, store.BulkCreateNodes(xNodes[start:end]))
		require.NoError(t, store.BulkCreateNodes(yNodes[start:end]))
		require.NoError(t, store.BulkCreateEdges(callsEdges[start:end]))
		require.NoError(t, store.BulkCreateEdges(linksEdges[start:end]))
	}
	return store, base, exec, probe
}

func TestIssue638_RelationshipCountShapes_ExactRows(t *testing.T) {
	_, _, exec, probe := newIssue638E2EStore(t)
	ctx := context.Background()

	const n = 8000
	shapes := []struct {
		name string
		q    string
		want int64
	}{
		{"typed few", "MATCH ()-[r:TICK]->() RETURN count(r) AS c", 20},
		{"typed many", "MATCH ()-[r:CALLS]->() RETURN count(r) AS c", n},
		{"untyped", "MATCH ()-[r]->() RETURN count(r) AS c", 2*n + 20},
		{"start label", "MATCH (s:Function)-[r:CALLS]->() RETURN count(r) AS c", n},
		{"end label", "MATCH ()-[r:CALLS]->(t:Callee) RETURN count(r) AS c", n},
		{"start label count star", "MATCH (s:Function)-[:CALLS]->() RETURN count(*) AS c", n},
		{"incoming start label", "MATCH (s:Function)<-[r:CALLS]-() RETURN count(r) AS c", 0},
		{"incoming end label", "MATCH ()<-[r:CALLS]-(t:Callee) RETURN count(r) AS c", 0},
		{"multi type", "MATCH ()-[r:CALLS|LINKS]->() RETURN count(r) AS c", 2 * n},
	}

	before := probe.calls
	for _, shape := range shapes {
		result, err := exec.Execute(ctx, shape.q, nil)
		require.NoError(t, err, shape.name)
		require.Len(t, result.Rows, 1, shape.name)
		require.Equal(t, shape.want, result.Rows[0][0], shape.name)
	}
	// None of the counter-answered shapes may have materialized edges.
	require.Zero(t, probe.calls-before, "counter shapes must not call GetEdgesByType")

	// Both-labeled endpoints stay a real traversal (Neo4j plans
	// NodeByLabelScan -> Expand -> Filter for this shape).
	result, err := exec.Execute(ctx, "MATCH (s:Function)-[r:CALLS]->(t:Callee) RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(n), result.Rows[0][0])

	// Mutations keep the counters exact (create + delete + relabel).
	_, err = exec.Execute(ctx, "CREATE (:Tick {n:99})-[:TICK]->(:Tick {n:100})", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "MATCH ()-[r:TICK]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(21), result.Rows[0][0])

	// Deleting one TICK edge decrements the counter.
	_, err = exec.Execute(ctx, "MATCH ()-[r:TICK]->() WITH r LIMIT 1 DELETE r", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "MATCH ()-[r:TICK]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(20), result.Rows[0][0])

	// Relabeling a start node moves the positional bucket. SET adds the new
	// label (the node still counts under Function); REMOVE drops the old one.
	_, err = exec.Execute(ctx, "MATCH (s:Function) WITH s LIMIT 1 SET s:Helper", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "MATCH (s:Helper)-[r:CALLS]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(n/20), result.Rows[0][0])
	result, err = exec.Execute(ctx, "MATCH (s:Function)-[r:CALLS]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(n), result.Rows[0][0])

	_, err = exec.Execute(ctx, "MATCH (s:Helper) WITH s LIMIT 1 REMOVE s:Function", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "MATCH (s:Function)-[r:CALLS]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(n-n/20), result.Rows[0][0])
	result, err = exec.Execute(ctx, "MATCH (s:Helper)-[r:CALLS]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(n/20), result.Rows[0][0])
}

func TestOptionalOrderCountAfterBulkEdges(t *testing.T) {
	store, _, exec, _ := newIssue638E2EStore(t)
	ctx := context.Background()
	const products = 200
	for index := 0; index < products; index++ {
		_, err := store.CreateNode(&storage.Node{
			ID: storage.NodeID(fmt.Sprintf("product%d", index)), Labels: []string{"Product"},
			Properties: map[string]interface{}{"productName": fmt.Sprintf("Product %d", index)},
		})
		require.NoError(t, err)
	}
	_, err := store.CreateNode(&storage.Node{ID: "order", Labels: []string{"Order"}})
	require.NoError(t, err)
	edges := make([]*storage.Edge, products)
	for index := range edges {
		edges[index] = &storage.Edge{
			ID: storage.EdgeID(fmt.Sprintf("order-line%d", index)), Type: "ORDERS",
			StartNode: "order", EndNode: storage.NodeID(fmt.Sprintf("product%d", max(1, index))),
		}
	}
	require.NoError(t, store.BulkCreateEdges(edges))
	result, err := exec.Execute(ctx, `MATCH (p:Product)
		OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
		RETURN p.productName AS productName, count(o) AS orderCount
		ORDER BY orderCount DESC, productName ASC LIMIT 200`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, products)
	require.Equal(t, int64(2), result.Rows[0][1])
	require.Equal(t, int64(0), result.Rows[products-1][1])
	expectedRows := result.Rows
	txExec := NewStorageExecutor(store)
	_, err = txExec.Execute(WithTransactionControl(ctx), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = txExec.Execute(WithTransactionControl(ctx), "ROLLBACK", nil) })
	result, err = txExec.Execute(ctx, `MATCH (p:Product)
		OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
		RETURN p.productName AS productName, count(o) AS orderCount
		ORDER BY orderCount DESC, productName ASC LIMIT 200`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, products)
	require.Equal(t, int64(2), result.Rows[0][1])
	require.Equal(t, int64(0), result.Rows[products-1][1])
	require.Equal(t, expectedRows, result.Rows)
}

func TestOptionalOrderCountThroughAsyncStorage(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	async := storage.NewAsyncEngine(base, nil)
	t.Cleanup(func() { require.NoError(t, async.Close()) })
	store := storage.NewNamespacedEngine(async, "nornic")
	for _, node := range []*storage.Node{
		{ID: "p1", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "With orders"}},
		{ID: "p2", Labels: []string{"Product"}, Properties: map[string]interface{}{"productName": "No orders"}},
		{ID: "o1", Labels: []string{"Order"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "line1", Type: "ORDERS", StartNode: "o1", EndNode: "p1"}))
	async.Flush()
	exec := NewStorageExecutor(&forwardingNamespacedEngine{Engine: store})
	result, err := exec.Execute(context.Background(), `MATCH (p:Product)
		OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
		RETURN p.productName AS productName, count(o) AS orderCount
		ORDER BY orderCount DESC, productName ASC LIMIT 100`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"With orders", int64(1)}, {"No orders", int64(0)}}, result.Rows)
}

func TestIssue638_RelationshipCountShapes_ColdExecutionIsFast(t *testing.T) {
	_, base, exec, _ := newIssue638E2EStore(t)
	ctx := context.Background()

	// Cold execution (edge-type cache invalidated, unique statement text) of
	// every issue shape must complete far below the materializing costs the
	// issue measured (234ms-680ms in-process, seconds on the 300k store).
	// The bound is intentionally generous for CI hardware.
	shapes := []string{
		"MATCH ()-[r:TICK]->() RETURN count(r) AS c%d",
		"MATCH ()-[r:CALLS]->() RETURN count(r) AS c%d",
		"MATCH ()-[r]->() RETURN count(r) AS c%d",
		"MATCH (s:Function)-[r:CALLS]->() RETURN count(r) AS c%d",
		"MATCH ()-[r:CALLS]->(t:Callee) RETURN count(r) AS c%d",
	}
	for i, shape := range shapes {
		base.InvalidateEdgeTypeCache()
		start := time.Now()
		result, err := exec.Execute(ctx, fmt.Sprintf(shape, i), nil)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Less(t, elapsed, 2*time.Second,
			"cold typed relationship count must be O(1), got %s", elapsed)
	}
}
