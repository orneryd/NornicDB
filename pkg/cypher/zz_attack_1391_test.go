package cypher

// Test-only self-attack (never committed): which routes stop at the
// statement's deadline / client cancel, through Execute, on the current
// parser. Reports; asserts nothing beyond setup.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestZZAttack1391(t *testing.T) {
	if os.Getenv("ZZ_ATTACK_1391") == "" {
		t.Skip("opt-in")
	}
	const people, trigger, bound = 20000, 200, 8 * pipelineCancellationInterval
	newExec := func(t *testing.T) *StorageExecutor {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "attack")
		for i := 0; i < people; i++ {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("p-%d", i)), Labels: []string{"P"},
				Properties: map[string]interface{}{"v": int64(i), "k": int64(i % 50), "name": fmt.Sprintf("n%d", i)}})
			require.NoError(t, err)
		}
		for i := 0; i+1 < people; i++ {
			require.NoError(t, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e-%d", i)),
				StartNode: storage.NodeID(fmt.Sprintf("p-%d", i)), EndNode: storage.NodeID(fmt.Sprintf("p-%d", i+1)), Type: "NEXT",
				Properties: map[string]interface{}{"w": int64(i % 7)}}))
		}
		return NewStorageExecutor(store)
	}
	var clock *boundedWorkClock
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		if name == "bound.tick" {
			return func(x interface{}) interface{} { return clock.tick(x) }, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { PluginFunctionLookup = previous })

	exec := newExec(t)
	ticked := []struct{ name, cypher string }{
		{"distinct", "MATCH (n:P) RETURN DISTINCT bound.tick(n.v) AS v"},
		{"sort", "MATCH (n:P) RETURN n.v AS v ORDER BY bound.tick(n.v) DESC"},
		{"aggregation", "MATCH (n:P) RETURN n.k AS k, collect(bound.tick(n.v)) AS vs"},
		{"projection", "MATCH (n:P) RETURN bound.tick(n.v) AS v"},
		{"unwind", "UNWIND range(1, 20000) AS x WITH bound.tick(x) AS y RETURN count(y) AS c"},
		{"traversal_onehop", "MATCH (a:P)-[:NEXT]->(b:P) WHERE bound.tick(b.v) IS NOT NULL RETURN count(b) AS c"},
		{"filter", "MATCH (n:P) WHERE bound.tick(n.v) IS NOT NULL RETURN n.v AS v"},
		{"where_count", "MATCH (n:P) WHERE bound.tick(n.v) >= 0 RETURN count(n) AS c"},
		{"with_where", "MATCH (n:P) WITH n WHERE bound.tick(n.v) >= 0 RETURN count(*) AS c"},
		{"optional_match", "MATCH (a:P) OPTIONAL MATCH (a)-[:NEXT]->(b:P) WHERE bound.tick(b.v) >= 0 RETURN count(b) AS c"},
		{"varlength", "MATCH (a:P {v: 0})-[r:NEXT*1..20000]->(b:P) WHERE bound.tick(b.v) >= 0 RETURN count(b) AS c"},
		{"call_subquery", "MATCH (a:P) CALL (a) { RETURN bound.tick(a.v) AS t } RETURN count(t) AS c"},
		{"exists_subquery", "MATCH (a:P) WHERE EXISTS { MATCH (a)-[:NEXT]->(b) WHERE bound.tick(b.v) >= 0 } RETURN count(a) AS c"},
		{"count_subquery", "MATCH (a:P) RETURN sum(COUNT { MATCH (a)-[:NEXT]->(b) WHERE bound.tick(b.v) >= 0 }) AS c"},
		{"union", "MATCH (n:P) RETURN bound.tick(n.v) AS v UNION ALL MATCH (n:P) RETURN n.v AS v"},
		{"comprehension", "RETURN size([x IN range(1, 200000) WHERE bound.tick(x) > 0]) AS c"},
		{"reduce", "RETURN reduce(s = 0, x IN range(1, 200000) | s + bound.tick(x)) AS c"},
		{"quantifier", "RETURN all(x IN range(1, 200000) WHERE bound.tick(x) > 0) AS c"},
		{"pattern_comprehension", "MATCH (a:P) RETURN sum(size([(a)-[:NEXT]->(b) | bound.tick(b.v)])) AS c"},
		{"cartesian", "MATCH (a:P), (b:P) WHERE a.v < 3 AND bound.tick(b.v) >= 0 RETURN count(*) AS c"},
		{"create_per_row", "UNWIND range(1, 20000) AS x CREATE (:Q {v: bound.tick(x)})"},
		{"merge_per_row", "UNWIND range(1, 20000) AS x MERGE (:R {v: bound.tick(x)})"},
		{"set_per_row", "MATCH (n:P) SET n.t = bound.tick(n.v)"},
		{"foreach", "FOREACH (x IN range(1, 20000) | CREATE (:S {v: bound.tick(x)}))"},
		{"delete_where", "MATCH (n:Q) WHERE bound.tick(n.v) >= 0 DETACH DELETE n"},
		{"shortest_where", "MATCH p = shortestPath((a:P {v: 0})-[:NEXT*]->(b:P {v: 19999})) WHERE all(n IN nodes(p) WHERE bound.tick(n.v) >= 0) RETURN length(p) AS l"},
	}
	modes := []struct {
		name  string
		start func() (context.Context, context.CancelFunc)
		end   func(ctx context.Context, cancel context.CancelFunc)
	}{
		{"deadline", func() (context.Context, context.CancelFunc) { return context.WithTimeout(context.Background(), 3*time.Second) },
			func(ctx context.Context, _ context.CancelFunc) { <-ctx.Done() }},
		{"cancel", func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
			func(_ context.Context, cancel context.CancelFunc) { cancel() }},
	}
	var report []string
	nonce := 0
	for _, q := range ticked {
		for _, mode := range modes {
			for _, pass := range []string{"first", "compiled"} {
				ctx, cancel := mode.start()
				clock = &boundedWorkClock{ctx: ctx, trigger: trigger, end: func() { mode.end(ctx, cancel) }}
				nonce++
				started := time.Now()
				_, err := exec.Execute(ctx, q.cypher, map[string]interface{}{"nonce": nonce})
				elapsed := time.Since(started)
				cancel()
				rows, after := clock.counts()
				verdict := "OK"
				switch {
				case rows < trigger:
					verdict = "NOTRIGGER"
				case err == nil:
					verdict = "RESULT-AFTER-END"
				case after > bound:
					verdict = "UNBOUNDED"
				}
				errText := "<nil>"
				if err != nil {
					errText = strings.SplitN(err.Error(), "\n", 2)[0]
					if len(errText) > 70 {
						errText = errText[:70]
					}
				}
				report = append(report, fmt.Sprintf("TICK\t%s\t%s\t%s\t%s\trows=%d\tafter=%d\t%s\t%s", verdict, q.name, mode.name, pass, rows, after, elapsed.Round(time.Millisecond), errText))
			}
		}
	}

	// Bulk value work with no per-element hook: overdue past a 100 ms deadline.
	bulk := []struct{ name, cypher string }{
		{"collect_sort_list", "WITH range(1, 3000000) AS l RETURN size(apoc.coll.sort(l)) AS c"},
		{"list_reverse", "RETURN size(reverse(range(1, 5000000))) AS c"},
		{"list_concat", "WITH range(1, 3000000) AS l RETURN size(l + l) AS c"},
		{"distinct_values", "UNWIND range(1, 2000000) AS x RETURN count(DISTINCT x % 1000003) AS c"},
		{"order_values", "UNWIND range(1, 2000000) AS x WITH x ORDER BY -x RETURN count(x) AS c"},
		{"collect_values", "UNWIND range(1, 3000000) AS x RETURN size(collect(x)) AS c"},
		{"string_join", "RETURN size(apoc.text.join([x IN range(1, 2000000) | toString(x)], ',')) AS c"},
		{"in_list", "WITH range(1, 3000000) AS l RETURN 2999999 IN l AS c"},
		{"keys_big_map", "WITH apoc.map.fromLists([x IN range(1, 500000) | toString(x)], range(1, 500000)) AS m RETURN size(keys(m)) AS c"},
		{"varlength_count", "MATCH (a:P {v: 0})-[:NEXT*1..20000]->(b) RETURN count(b) AS c"},
		{"shortest_all", "MATCH p = allShortestPaths((a:P {v: 0})-[:NEXT*]->(b:P {v: 19999})) RETURN length(p) AS l"},
		{"dijkstra", "MATCH (a:P {v: 0}), (b:P {v: 19999}) CALL apoc.algo.dijkstra(a, b, 'NEXT', 'w') YIELD weight RETURN weight"},
		{"labels_scan", "CALL db.labels() YIELD label RETURN count(label) AS c"},
		{"export_csv", "CALL apoc.export.csv.query('MATCH (n:P) RETURN n.v AS v, n.name AS name', null, {stream: true}) YIELD data RETURN size(data) AS c"},
		{"create_bulk", "UNWIND range(1, 300000) AS x CREATE (:T {v: x})"},
		{"delete_bulk", "MATCH (n:T) DETACH DELETE n"},
	}
	for _, q := range bulk {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		deadline, _ := ctx.Deadline()
		started := time.Now()
		_, err := exec.Execute(ctx, q.cypher, map[string]interface{}{"nonce": nonce})
		returned := time.Now()
		cancel()
		nonce++
		overdue := returned.Sub(deadline)
		verdict := "OK"
		switch {
		case err == nil && returned.After(deadline):
			verdict = "RESULT-AFTER-END"
		case err == nil:
			verdict = "FINISHED-IN-TIME"
		case overdue > 50*time.Millisecond:
			verdict = "LATE"
		}
		errText := "<nil>"
		if err != nil {
			errText = strings.SplitN(err.Error(), "\n", 2)[0]
			if len(errText) > 70 {
				errText = errText[:70]
			}
		}
		report = append(report, fmt.Sprintf("BULK\t%s\t%s\telapsed=%s\toverdue=%s\t%s", verdict, q.name, returned.Sub(started).Round(time.Millisecond), overdue.Round(time.Millisecond), errText))
	}
	t.Logf("\n%s", strings.Join(report, "\n"))
}
