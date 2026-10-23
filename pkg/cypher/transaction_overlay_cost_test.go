package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An isolated node's degree does not change when unrelated relationships are
// added to the same transaction. Repeated integrity reads must not allocate a
// shadow map containing every unrelated relationship.
func TestTransactionOverlayIncidentCostIsLocal(t *testing.T) {
	base := newTestMemoryEngine(t)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for _, id := range []storage.NodeID{"test:isolated", "test:a", "test:b"} {
		_, err := tx.CreateNode(&storage.Node{ID: id, Labels: []string{"Link"}})
		require.NoError(t, err)
	}
	read := func() {
		edges, err := tx.IncidentEdges("test:isolated")
		if err != nil || len(edges) != 0 {
			t.Fatalf("isolated node: edges=%d, err=%v", len(edges), err)
		}
	}
	before := testing.AllocsPerRun(5, read)
	for i := 0; i < 5000; i++ {
		require.NoError(t, tx.CreateEdge(&storage.Edge{
			ID: storage.EdgeID(fmt.Sprintf("test:e%d", i)), StartNode: "test:a", EndNode: "test:b",
			Type: "LINK", Properties: map[string]interface{}{"weight": float64(i + 1)},
		}))
	}
	after := testing.AllocsPerRun(5, read)
	t.Logf("isolated incident read allocations: empty=%g, unrelated_5000=%g", before, after)
	require.LessOrEqual(t, after, before+16, "unrelated pending edges must not enlarge each incident read")
}

// A complete unindexed candidate lookup must retain every matching node, but
// repeated equality probes should allocate for their result rather than decode
// and copy the complete committed label set on every row.
func TestTransactionOverlayCommittedMergeCostIsLocal(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(context.Background(), `UNWIND range(0,105) AS i CREATE (:Probe {id: toString(i)})`, nil)
	require.NoError(t, err)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	wrapper := &transactionStorageWrapper{tx: tx, underlying: store, namespace: "test", separator: ":", mutatedNodeIDs: make(map[string]struct{})}
	reader := exec.cloneWithStorage(wrapper)
	read := func() {
		nodes, _, err := reader.findMergeNodesScanned(wrapper, []string{"Probe"}, map[string]interface{}{"id": "42"})
		if err != nil || len(nodes) != 1 || nodes[0].Properties["id"] != "42" {
			t.Fatalf("committed MERGE candidates: nodes=%v, err=%v", nodes, err)
		}
	}
	allocations := testing.AllocsPerRun(5, read)
	t.Logf("repeated committed MERGE candidate allocations=%g", allocations)
	require.Less(t, allocations, float64(100), "a repeated one-node match must not copy the whole label set")
}

const overlayCostCreate = `UNWIND range(1,$edges) AS i
MERGE (a:Probe {graph: $graph, id: toString(i % 106)})
MERGE (b:Probe {graph: $graph, id: toString((i*7) % 106)})
CREATE (a)-[:PROBE {weight: 1.0/i}]->(b)`

const overlayCostDelete = `WITH DISTINCT $graph AS graph
MATCH (n:Probe {graph: graph}) WITH graph, collect(n) AS nodes
CALL (graph) { MATCH (n:Probe {graph: graph}) DETACH DELETE n RETURN count(*) AS removed }
RETURN removed`

func TestTransactionOverlayScaledRepros(t *testing.T) {
	for _, size := range []int{1000, 5000} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			ctx := context.Background()
			params := map[string]interface{}{"edges": int64(size), "graph": "overlay-cost"}
			start := time.Now()
			result, err := exec.Execute(ctx, overlayCostCreate+"\n"+overlayCostDelete, params)
			t.Logf("pending create+delete: %s", time.Since(start))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(106)}}, result.Rows)
			result, err = exec.Execute(ctx, `MATCH (n:Probe) RETURN count(n)`, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			for run := 0; run < 3; run++ {
				start = time.Now()
				_, err = exec.Execute(ctx, overlayCostCreate, params)
				t.Logf("MERGE run %d (0=fresh): %s", run, time.Since(start))
				require.NoError(t, err)
			}
			start = time.Now()
			result, err = exec.Execute(ctx, `MATCH (n:Probe) DETACH DELETE n RETURN count(*) AS removed`, nil)
			t.Logf("committed delete (%d relationships): %s", 3*size, time.Since(start))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(106)}}, result.Rows)
		})
	}
}
