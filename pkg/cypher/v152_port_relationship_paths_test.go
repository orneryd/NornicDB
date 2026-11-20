package cypher

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func v152PortRelationshipStore(t *testing.T) *storage.NamespacedEngine {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	return storage.NewNamespacedEngine(base, "port")
}

func v152PortRelationshipNode(t *testing.T, store *storage.NamespacedEngine, id, label string, properties map[string]any) {
	t.Helper()
	_, err := store.CreateNode(t.Context(), &storage.Node{
		ID: storage.NodeID(id), Labels: []string{label}, Properties: properties,
	})
	require.NoError(t, err)
}

func v152PortRelationshipEdge(t *testing.T, store *storage.NamespacedEngine, id, start, end, kind string, properties map[string]any) {
	t.Helper()
	require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{
		ID: storage.EdgeID(id), StartNode: storage.NodeID(start), EndNode: storage.NodeID(end),
		Type: kind, Properties: properties,
	}))
}

func v152PortRelationshipRun(t *testing.T, exec *StorageExecutor, query string, params map[string]any) *ExecuteResult {
	t.Helper()
	result, err := exec.Execute(WithTransactionControl(t.Context()), query, params)
	require.NoError(t, err, query)
	return result
}

// Section 38.4. Exercise the same OPTIONAL MATCH and population as TestPDRelID.
func TestV152PortReversedRelationshipElementID(t *testing.T) {
	store := v152PortRelationshipStore(t)
	for _, id := range []string{"a", "b"} {
		v152PortRelationshipNode(t, store, id, "Held", map[string]any{"id": id})
	}
	for i := range 10000 {
		v152PortRelationshipEdge(t, store, fmt.Sprintf("r%d", i), "a", "b", "R", map[string]any{"v": i})
	}

	seed := NewStorageExecutor(store)
	result := v152PortRelationshipRun(t, seed, "MATCH ()-[r:R {v:0}]->() RETURN elementId(r)", nil)
	require.Len(t, result.Rows, 1)
	spy := &adjacencyReadSpy{Engine: store}
	exec := NewStorageExecutor(spy)
	exec.cache = nil
	for _, predicate := range []string{"elementId(r) = $r", "$r = elementId(r)"} {
		t.Run(predicate, func(t *testing.T) {
			spy.reset()
			start := time.Now()
			query := "MATCH (a:Held {id:'a'}),(b:Held {id:'b'}) WITH a,b " +
				"OPTIONAL MATCH (a)-[r:R]->(b) WHERE " + predicate + " RETURN r.v"
			got := v152PortRelationshipRun(t, exec, query, map[string]any{"r": result.Rows[0][0]})
			require.Len(t, got.Rows, 1)
			require.Equal(t, "0", fmt.Sprint(got.Rows[0][0]))
			t.Logf("PORT_RELID predicate=%q adjacent_rows=%d streams=%d elapsed=%s", predicate, spy.entries, spy.streams, time.Since(start))
			require.Zero(t, spy.entries, "exact relationship lookup read adjacency")
			require.Zero(t, spy.streams, "exact relationship lookup started a whole-type edge stream")
		})
	}
}

// Section 36, reproduction 1. Keep the original two queries and 5,101-pair
// population. Compare results before recording same-process elapsed times.
func TestV152PortRelationshipPairReads(t *testing.T) {
	store := v152PortRelationshipStore(t)
	for i := range 200 {
		id := fmt.Sprintf("h%d", i)
		v152PortRelationshipNode(t, store, id, "PDHeld", map[string]any{"thing_id": id, "holders": 1})
	}

	pairs := make([]any, 0, 5101)
	for i := range 5101 {
		a, b := fmt.Sprintf("h%d", i/128), fmt.Sprintf("h%d", 50+i%128)
		pairs = append(pairs, map[string]any{"i": a, "t": b})
		if i < 1390 {
			v152PortRelationshipEdge(t, store, fmt.Sprintf("co%d", i), a, b, "PD_COHELD", map[string]any{"n": 1})
		}
	}

	exec := NewStorageExecutor(store)
	exec.cache = nil
	v152PortRelationshipRun(t, exec, "CREATE CONSTRAINT held_id FOR (h:PDHeld) REQUIRE h.thing_id IS UNIQUE", nil)
	for _, shape := range []struct{ name, statement string }{
		{"inline", "UNWIND $rows AS row MATCH (:PDHeld {thing_id: row.i})-[co:PD_COHELD]->(:PDHeld {thing_id: row.t}) RETURN row.i AS i,row.t AS t,co.n AS value"},
		{"prebound", "UNWIND $rows AS row MATCH (a:PDHeld {thing_id: row.i}),(b:PDHeld {thing_id: row.t}) WITH row,a,b MATCH (a)-[co:PD_COHELD]->(b) RETURN row.i AS i,row.t AS t,co.n AS value"},
	} {
		t.Run(shape.name, func(t *testing.T) {
			params := map[string]any{"rows": pairs}
			start := time.Now()
			auto := v152PortRelationshipRun(t, exec, shape.statement, params)
			autoTime := time.Since(start)
			require.Len(t, auto.Rows, 1390)

			v152PortRelationshipRun(t, exec, "BEGIN", nil)
			defer v152PortRelationshipRun(t, exec, "ROLLBACK", nil)
			v152PortRelationshipRun(t, exec, "UNWIND ['h0','h50'] AS id MATCH (h:PDHeld {thing_id:id}) RETURN id,h.holders AS value", nil)
			start = time.Now()
			explicit := v152PortRelationshipRun(t, exec, shape.statement, params)
			explicitTime := time.Since(start)
			require.ElementsMatch(t, auto.Rows, explicit.Rows)
			t.Logf("PORT_PAIR shape=%s pairs=%d rows=%d autocommit=%s explicit=%s", shape.name, len(pairs), len(auto.Rows), autoTime, explicitTime)
		})
	}
}
