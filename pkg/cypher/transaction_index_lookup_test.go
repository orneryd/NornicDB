package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// transactionIndexExecutor is an executor on a namespaced engine with a
// property index on (:P).k and count committed :P nodes, k = "k<i>", v = i.
func transactionIndexExecutor(t *testing.T, count int) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "txindex"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE INDEX p_k FOR (n:P) ON (n.k)", nil)
	require.NoError(t, err)
	for start := 0; start < count; start += 500 {
		rows := make([]interface{}, 0, 500)
		for i := start; i < start+500 && i < count; i++ {
			rows = append(rows, map[string]interface{}{"k": fmt.Sprintf("k%d", i), "v": int64(i)})
		}
		_, err := exec.Execute(ctx, "UNWIND $rows AS row CREATE (n:P) SET n = row", map[string]interface{}{"rows": rows})
		require.NoError(t, err)
	}
	return exec
}

// TestTransactionIndexedReadDoesNotScanTheLabel: a read by an indexed
// property inside an explicit transaction is an index lookup, as it is in
// auto-commit — its cost doesn't grow with the number of nodes of the label.
// This holds for a transaction that has written nothing and for one that has
// written nodes (whose writes are merged into the lookup), on each form of
// the read.
func TestTransactionIndexedReadDoesNotScanTheLabel(t *testing.T) {
	ctx := context.Background()
	reads := []struct {
		name  string
		query string
	}{
		{"where equality", "MATCH (n:P) WHERE n.k = $k RETURN n.v AS v"},
		{"property map", "MATCH (n:P {k: $k}) RETURN n.v AS v"},
		{"in parameter list", "MATCH (n:P) WHERE n.k IN $ks RETURN n.v AS v"},
		{"unwind where", "UNWIND $ks AS k MATCH (n:P) WHERE n.k = k RETURN n.v AS v"},
		{"unwind property map", "UNWIND $ks AS k MATCH (n:P {k: k}) RETURN n.v AS v"},
	}
	// allocations of one read in a transaction over count nodes; every run
	// asks for another key, so no result is served from the result cache.
	allocations := func(t *testing.T, count int, query string, writeFirst bool) float64 {
		exec := transactionIndexExecutor(t, count)
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
		if writeFirst {
			_, err := exec.Execute(ctx, "CREATE (:P {k: 'pending', v: -1})", nil)
			require.NoError(t, err)
		}
		next := 0
		return testing.AllocsPerRun(20, func() {
			next++
			key := fmt.Sprintf("k%d", next%count)
			result, err := exec.Execute(ctx, query, map[string]interface{}{"k": key, "ks": []interface{}{key}})
			if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(next%count) {
				t.Fatalf("%s: rows %v, err %v", query, result, err)
			}
		})
	}
	for _, writeFirst := range []bool{false, true} {
		for _, read := range reads {
			t.Run(fmt.Sprintf("%s/own writes=%v", read.name, writeFirst), func(t *testing.T) {
				small := allocations(t, 100, read.query, writeFirst)
				large := allocations(t, 3000, read.query, writeFirst)
				// A label scan allocates per node: 30 times more at 30 times
				// the nodes.
				require.Less(t, large, small*2, "allocations at 100 nodes: %v, at 3000 nodes: %v", small, large)
			})
		}
	}
}

// TestTransactionIndexLookupSeesOwnWrites: every read that goes through a
// property index sees the transaction's own writes (#809) — nodes it
// created, nodes whose indexed value it changed (under the new value, not
// the old one), nodes it deleted, and nodes whose label it added or removed.
func TestTransactionIndexLookupSeesOwnWrites(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "txindex"))
	run := func(query string, params map[string]interface{}) [][]interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		return result.Rows
	}
	run("CREATE INDEX p_k FOR (n:P) ON (n.k)", nil)
	run("CREATE (:P {k: 'a', name: 'kept'}), (:P {k: 'a', name: 'moved'}), (:P {k: 'a', name: 'deleted'}), (:P {k: 'a', name: 'unlabeled'}), (:Q {k: 'a', name: 'labeled'}), (:P {k: 'z', name: 'into'})", nil)

	run("BEGIN", nil)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
	run("CREATE (:P {k: 'a', name: 'created'})", nil)
	run("MATCH (n:P {name: 'moved'}) SET n.k = 'b'", nil)
	run("MATCH (n:P {name: 'deleted'}) DELETE n", nil)
	run("MATCH (n:P {name: 'unlabeled'}) REMOVE n:P", nil)
	run("MATCH (n:Q {name: 'labeled'}) SET n:P", nil)
	run("MATCH (n:P {name: 'into'}) SET n.k = 'a'", nil)

	withA := [][]interface{}{{"created"}, {"into"}, {"kept"}, {"labeled"}}
	withB := [][]interface{}{{"moved"}}
	params := map[string]interface{}{"a": "a", "b": "b", "list": []interface{}{"a"}, "both": []interface{}{"a", "b"}}
	for _, read := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (n:P) WHERE n.k = 'a' RETURN n.name AS name ORDER BY name", withA},
		{"MATCH (n:P) WHERE n.k = $a RETURN n.name AS name ORDER BY name", withA},
		{"MATCH (n:P {k: 'a'}) RETURN n.name AS name ORDER BY name", withA},
		{"MATCH (n:P) WHERE n.k IN $list RETURN n.name AS name ORDER BY name", withA},
		{"MATCH (n:P) WHERE n.k IN ['a'] RETURN n.name AS name ORDER BY name", withA},
		{"MATCH (n:P) WHERE n.k = 'a' OR n.k = 'missing' RETURN n.name AS name ORDER BY name", withA},
		{"UNWIND $list AS k MATCH (n:P) WHERE n.k = k RETURN n.name AS name ORDER BY name", withA},
		{"UNWIND $list AS k MATCH (n:P {k: k}) RETURN n.name AS name ORDER BY name", withA},
		{"MATCH (n:P) WHERE n.k = 'b' RETURN n.name AS name ORDER BY name", withB},
		{"MATCH (n:P {k: $b}) RETURN n.name AS name ORDER BY name", withB},
		{"MATCH (n:P) WHERE n.k IN $both RETURN n.name AS name ORDER BY name", append(append([][]interface{}{}, withA...), withB...)},
		{"MATCH (n:P) WHERE n.k = 'z' RETURN n.name AS name", nil},
		{"MATCH (n:P) WHERE n.k IS NOT NULL RETURN n.name AS name ORDER BY n.k, name LIMIT 3", [][]interface{}{{"created"}, {"into"}, {"kept"}}},
		{"MATCH (n:P) WHERE n.k IS NOT NULL RETURN n.name AS name ORDER BY n.k DESC, name LIMIT 2", [][]interface{}{{"moved"}, {"created"}}},
		{"MATCH (n:P) WHERE n.k IS NOT NULL RETURN count(n) AS c", [][]interface{}{{int64(5)}}},
	} {
		got := run(read.query, params)
		if len(read.want) == 0 {
			require.Empty(t, got, read.query)
			continue
		}
		require.Equal(t, read.want, got, read.query)
	}

	// MERGE finds the node the transaction created rather than creating a
	// second one.
	run("CREATE (:P {k: 'merged', name: 'first'})", nil)
	run("MERGE (n:P {k: 'merged'}) ON CREATE SET n.name = 'second'", nil)
	require.Equal(t, [][]interface{}{{"first"}}, run("MATCH (n:P {k: 'merged'}) RETURN n.name AS name", nil))

	// A traversal that starts from an indexed property finds a pending start
	// node.
	run("MATCH (n:P {name: 'created'}) CREATE (n)-[:R]->(:T {name: 'target'})", nil)
	require.Equal(t, [][]interface{}{{"created", "target"}},
		run("MATCH (n:P {k: 'a'})-[:R]->(t:T) RETURN n.name AS name, t.name AS target", nil))

	// A pattern without a label lists every node with the value, whatever
	// its labels: the per-label indexes can't answer it.
	require.Equal(t, [][]interface{}{{"created"}, {"into"}, {"kept"}, {"labeled"}, {"unlabeled"}},
		run("MATCH (n {k: 'a'}) RETURN n.name AS name ORDER BY name", nil))

	require.Equal(t, [][]interface{}{{"created"}, {"into"}, {"kept"}, {"labeled"}, {"unlabeled"}},
		run("MATCH (n) WHERE n.k = 'a' RETURN n.name AS name ORDER BY name", nil))

	// shortestPath finds pending endpoints by their indexed property.
	run("CREATE (:P {k: 'from'})-[:R]->(:P {k: 'to'})", nil)
	for _, query := range []string{
		"MATCH p = shortestPath((a:P {k: 'from'})-[:R*]->(b:P {k: 'to'})) RETURN length(p) AS hops",
		"MATCH (a:P {k: 'from'}), (b:P {k: 'to'}) MATCH p = shortestPath((a)-[:R*]->(b)) RETURN length(p) AS hops",
		"MATCH p = allShortestPaths((a:P {k: 'from'})-[:R*]->(b:P {k: 'to'})) RETURN length(p) AS hops",
	} {
		require.Equal(t, [][]interface{}{{int64(1)}}, run(query, nil), query)
	}

	// After commit the same reads give the same rows from the index alone.
	run("COMMIT", nil)
	require.Equal(t, withA, run("MATCH (n:P) WHERE n.k = 'a' RETURN n.name AS name ORDER BY name", map[string]interface{}{"after": "commit"}))
	require.Equal(t, withB, run("MATCH (n:P) WHERE n.k = 'b' RETURN n.name AS name ORDER BY name", map[string]interface{}{"after": "commit"}))
}

// TestTransactionOrderedIndexRead: an ordered index scan inside a transaction
// gives the rows auto-commit gives while the transaction has written no
// nodes, and includes the transaction's nodes once it has.
func TestTransactionOrderedIndexRead(t *testing.T) {
	ctx := context.Background()
	exec := transactionIndexExecutor(t, 50)
	const query = "MATCH (n:P) WHERE n.k IS NOT NULL RETURN n.v AS v ORDER BY n.k LIMIT 3"
	want, err := exec.Execute(ctx, query, map[string]interface{}{"mode": "auto"})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}, {int64(1)}, {int64(10)}}, want.Rows)
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
	got, err := exec.Execute(ctx, query, map[string]interface{}{"mode": "explicit"})
	require.NoError(t, err)
	require.Equal(t, want.Rows, got.Rows)
	_, err = exec.Execute(ctx, "CREATE (:P {k: 'a', v: -1})", nil)
	require.NoError(t, err)
	got, err = exec.Execute(ctx, query, map[string]interface{}{"mode": "explicit after write"})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(-1)}, {int64(0)}, {int64(1)}}, got.Rows)
}
