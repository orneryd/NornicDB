package cypher

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An isolated node's outgoing degree does not change when unrelated
// relationships are added to the same transaction. Repeated adjacency reads
// must not allocate a shadow map containing every unrelated relationship.
func TestTransactionOverlayOutgoingCostIsLocal(t *testing.T) {
	base := newTestMemoryEngine(t)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for _, id := range []storage.NodeID{"test:isolated", "test:a", "test:b"} {
		_, err := tx.CreateNode(&storage.Node{ID: id, Labels: []string{"Link"}})
		require.NoError(t, err)
	}
	read := func() {
		edges, err := tx.GetOutgoingEdges("test:isolated")
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
	peer := NewStorageExecutor(storage.NewNamespacedEngine(base, "other"))
	_, err = peer.Execute(context.Background(), `CREATE (:Probe {id:'42'})`, nil)
	require.NoError(t, err)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	wrapper := &transactionStorageWrapper{tx: tx, underlying: store, namespace: "test", separator: ":", mutatedNodeIDs: make(map[string]struct{})}
	reader := exec.cloneWithStorage(wrapper)
	read := func() {
		nodes, _, err := reader.findMergeNodesScanned(context.Background(), wrapper, []string{"Probe"}, map[string]interface{}{"id": "42"})
		if err != nil || len(nodes) != 1 || nodes[0].Properties["id"] != "42" {
			t.Fatalf("committed MERGE candidates: nodes=%v, err=%v", nodes, err)
		}
	}
	allocations := testing.AllocsPerRun(5, read)
	t.Logf("repeated committed MERGE candidate allocations=%g", allocations)
	require.Less(t, allocations, float64(100), "a repeated one-node match must not copy the whole label set")
	nodes, complete, err := reader.findMergeNodesScanned(context.Background(), wrapper, []string{"Probe"}, map[string]interface{}{"id": "42"})
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, nodes, 1, "peer namespace cannot join the pinned equality population")
	require.False(t, strings.HasPrefix(string(nodes[0].ID), "test:"), "query candidates retain user-facing IDs")
}

const overlayCostCreate = `UNWIND range(1,$edges) AS i
MERGE (a:Probe {graph: $graph, id: toString(i % 106)})
MERGE (b:Probe {graph: $graph, id: toString((i*7) % 106)})
CREATE (a)-[:PROBE {weight: 1.0/i}]->(b)`

const overlayCostDelete = `WITH DISTINCT $graph AS graph
MATCH (n:Probe {graph: graph}) WITH graph, collect(n) AS nodes
CALL (graph) { MATCH (n:Probe {graph: graph}) DETACH DELETE n RETURN count(*) AS removed }
RETURN removed`

const overlayCostMerge = `UNWIND range(1,$edges) AS i
MERGE (a:Probe {id: toString(i % 106)})
MERGE (b:Probe {id: toString((i*7) % 106)})
RETURN count(*) AS processed`

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

// BenchmarkTransactionOverlayDetachCost measures one create-and-cascade
// statement with every edge pending at delete time. Fixture setup and engine
// cleanup happen outside the timed region.
func BenchmarkTransactionOverlayDetachCost(b *testing.B) {
	for _, size := range []int{1000, 5000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				engine := fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				exec := NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
				b.StartTimer()
				result, err := exec.Execute(context.Background(), overlayCostCreate+"\n"+overlayCostDelete, map[string]interface{}{
					"edges": int64(size), "graph": "overlay-cost",
				})
				b.StopTimer()
				if err != nil {
					_ = engine.Close()
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(106) {
					_ = engine.Close()
					b.Fatalf("removed rows = %v, want [[106]]", result.Rows)
				}
				if err := engine.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkTransactionOverlayMergeCost compares the same row workload when
// its 106 nodes are pending (created by the first MERGE in that statement)
// and when those nodes were committed by a prior statement.
func BenchmarkTransactionOverlayMergeCost(b *testing.B) {
	for _, size := range []int{1000, 5000} {
		for _, committed := range []bool{false, true} {
			name := "pending"
			if committed {
				name = "committed"
			}
			b.Run(fmt.Sprintf("%s/%d", name, size), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					engine := fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed))
					store := storage.NewNamespacedEngine(engine, "test")
					exec := NewStorageExecutor(store)
					if committed {
						_, err := exec.Execute(context.Background(), `UNWIND range(0,105) AS i CREATE (:Probe {id: toString(i)})`, nil)
						if err != nil {
							_ = engine.Close()
							b.Fatal(err)
						}
					}
					b.StartTimer()
					result, err := exec.Execute(context.Background(), overlayCostMerge, map[string]interface{}{"edges": int64(size)})
					b.StopTimer()
					if err != nil {
						_ = engine.Close()
						b.Fatal(err)
					}
					if len(result.Rows) != 1 || len(result.Rows[0]) != 1 || result.Rows[0][0] != int64(size) {
						_ = engine.Close()
						b.Fatalf("processed rows = %v, want %d", result.Rows, size)
					}
					if err := engine.Close(); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			})
		}
	}
}

// Candidate pruning must be no stricter than Cypher equality: a temporal
// value reconstructed from storage can have a different location pointer.
func TestTransactionOverlayTemporalEqualityIsNotPruned(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(store)
	value := CypherDateTime{Time: time.Date(2026, 1, 2, 3, 4, 5, 42, time.FixedZone("offset", 4500))}
	_, err := exec.Execute(context.Background(), "CREATE (:TemporalOverlay {at:$at, id:1})", map[string]interface{}{"at": value})
	require.NoError(t, err)
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	wrapper := &transactionStorageWrapper{tx: tx, underlying: store, namespace: "test", separator: ":", mutatedNodeIDs: make(map[string]struct{})}
	candidates, err := wrapper.GetNodesByLabel("TemporalOverlay")
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	props := map[string]interface{}{"at": value}
	require.True(t, nodePropertiesMatch(candidates[0], props), "baseline Cypher equality accepts the stored value")
	reader := exec.cloneWithStorage(wrapper)
	nodes, _, err := reader.findMergeNodesScanned(context.Background(), wrapper, []string{"TemporalOverlay"}, props)
	require.NoError(t, err)
	require.Len(t, nodes, 1, "candidate keys cannot discard a Cypher-equal temporal value")
	props["id"] = int64(1)
	nodes, _, err = reader.findMergeNodesScanned(context.Background(), wrapper, []string{"TemporalOverlay"}, props)
	require.NoError(t, err)
	require.Len(t, nodes, 1, "scalar pruning leaves temporal equality to Cypher")
}
