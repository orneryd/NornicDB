package cypher

// Batch-3 earlier-issues probes (research only, untracked). The same file runs
// on the final build and on the previous build; build differences live in
// zz_ei3_build_test.go.

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func ei3Exec(ctx context.Context, e *StorageExecutor, q string, p map[string]interface{}) (*ExecuteResult, error) {
	return e.Execute(ctx, q, p)
}

func ei3Rows(r *ExecuteResult, err error) string {
	if err != nil {
		code, _ := nornicerrors.Neo4jStatus(err)
		return fmt.Sprintf("ERROR status=%s err=%v", code, err)
	}
	if r == nil {
		return "nil result"
	}
	return fmt.Sprintf("rows=%v", r.Rows)
}

// ---------------------------------------------------------------- L528
func TestEI3L528ForClause(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		config.SetParserType(parser)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "ei3for"))
		for _, q := range []string{
			"FOR x IN [1, 2] RETURN x ORDER BY x",
			"CYPHER 25 FOR x IN [1, 2] RETURN x ORDER BY x",
			"FOR /* gap */ x IN [1, 2] RETURN x ORDER BY x",
			"FOR x IN RETURN x",
		} {
			t.Logf("EI3 L528 parser=%s query=%q -> %s", parser, q, ei3Rows(ei3Exec(t.Context(), exec, q, nil)))
		}
	}
}

// ---------------------------------------------------------------- L529
func ei3VectorComponent(value interface{}) interface{} {
	switch typed := value.(type) {
	case *storage.Node:
		if vector := typed.GetDefaultEmbedding(); len(vector) > 3 {
			return float64(vector[3])
		}
		return fmt.Sprintf("node without vector (chunks=%d)", len(typed.ChunkEmbeddings))
	case []interface{}:
		if len(typed) == 1 {
			return ei3VectorComponent(typed[0])
		}
	case map[string]interface{}:
		if n, ok := typed["node"]; ok {
			return ei3VectorComponent(n)
		}
		return fmt.Sprintf("map %v", typed)
	}
	return fmt.Sprintf("type %T", value)
}

func TestEI3L529PluginVectors(t *testing.T) {
	ei3InstallLookup(t, map[string]interface{}{
		"ei3.vectorComponent": func(value interface{}) interface{} { return ei3VectorComponent(value) },
	}, false)
	g := newColdLargeEmbeddingGraph(t, 4)
	for _, id := range []storage.NodeID{"doc-0", "doc-1", "hub"} {
		ei3GetNode(t.Context(), g.store, id)
	}
	inner := "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) RETURN ei3.vectorComponent(n) AS score"
	cases := []struct {
		name, query string
		params      map[string]interface{}
	}{
		{"direct-hub", "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) RETURN n.title, ei3.vectorComponent(n)", nil},
		{"direct-label", "MATCH (n:Doc {k: 3}) RETURN ei3.vectorComponent(n)", nil},
		{"direct-list", "MATCH (n:Doc {k: 3}) RETURN ei3.vectorComponent([n])", nil},
		{"direct-map", "MATCH (n:Doc {k: 3}) RETURN ei3.vectorComponent({node: n})", nil},
		{"with-then-return", "MATCH (n:Doc {k: 3}) WITH n RETURN ei3.vectorComponent(n)", nil},
		{"apoc.cypher.run-inner", "CALL apoc.cypher.run($q, {}) YIELD value RETURN value.score", map[string]interface{}{"q": inner}},
		{"apoc.cypher.run-outer-node", "MATCH (n:Doc {k: 3}) CALL apoc.cypher.run('RETURN ei3.vectorComponent($n) AS score', {n: n}) YIELD value RETURN value.score", nil},
	}
	for _, explicit := range []bool{false, true} {
		for _, c := range cases {
			ctx := context.Background()
			if explicit {
				if _, err := g.exec.Execute(ctx, "BEGIN", nil); err != nil {
					t.Fatalf("BEGIN: %v", err)
				}
			}
			r, err := g.exec.Execute(ctx, c.query, c.params)
			if explicit {
				_, _ = g.exec.Execute(ctx, "ROLLBACK", nil)
			}
			got := ei3Rows(r, err)
			ok := err == nil && len(r.Rows) == 1 && r.Rows[0][len(r.Rows[0])-1] == float64(1)
			t.Logf("EI3 L529 explicit=%v case=%s -> %s correct=%v", explicit, c.name, got, ok)
			if !ok {
				t.Errorf("EI3 L529 explicit=%v case=%s: plugin function did not receive the stored vector: %s", explicit, c.name, got)
			}
		}
	}
}

// ---------------------------------------------------------------- L532
func TestEI3L532ApocNodeEquality(t *testing.T) {
	queries := []string{
		"MATCH (n:Doc {k: 3}) WITH n, COLLECT { MATCH (m:Doc {k: 3}) RETURN m } AS ms RETURN size(apoc.coll.toSet([n] + ms)) AS s, apoc.coll.contains(ms, n) AS c",
		"MATCH (n:Doc {k: 3}) CALL { MATCH (m:Doc {k: 3}) RETURN m } RETURN size(apoc.coll.toSet([n, m])) AS s, apoc.coll.contains([m], n) AS c",
		"MATCH (n:Doc {k: 3}) MATCH (m:Doc {k: 3}) RETURN size(apoc.coll.toSet([n, m])) AS s, apoc.coll.contains([m], n) AS c",
		"MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) MATCH (m:Doc) WHERE m.k = 3 RETURN size(apoc.coll.toSet([n, m])) AS s, apoc.coll.contains([m], n) AS c",
		"MATCH (n:Doc {k: 3}) WITH n, ei3.vectorComponent(n) AS v MATCH (m:Doc {k: 3}) RETURN size(apoc.coll.toSet([n, m])) AS s, apoc.coll.contains([m], n) AS c",
		"MATCH (n:Doc {k: 3}) WITH collect(n) AS a MATCH (m:Doc {k: 3}) WITH a, collect(m) AS b RETURN size(apoc.coll.toSet(a + b)) AS s, apoc.coll.contains(b, a[0]) AS c",
	}
	for _, plugin := range []bool{false, true} {
		t.Run(fmt.Sprintf("bundledPlugin=%v", plugin), func(t *testing.T) {
			ei3InstallLookup(t, map[string]interface{}{
				"ei3.vectorComponent": func(value interface{}) interface{} { return ei3VectorComponent(value) },
			}, plugin)
			g := newColdLargeEmbeddingGraph(t, 4)
			for _, q := range queries {
				r, err := g.exec.Execute(context.Background(), q, nil)
				got := ei3Rows(r, err)
				ok := err == nil && len(r.Rows) == 1 && fmt.Sprint(r.Rows[0][0]) == "1" && r.Rows[0][1] == true
				t.Logf("EI3 L532 plugin=%v q=%q -> %s correct=%v", plugin, q, got, ok)
				if !ok {
					t.Errorf("EI3 L532 plugin=%v: the same node compared unequal: %q -> %s", plugin, q, got)
				}
			}
		})
	}
}

// ---------------------------------------------------------------- L536
func TestEI3L536IdleHolder(t *testing.T) {
	for _, reason := range []string{"terminate", "idle-timeout"} {
		t.Run(reason, func(t *testing.T) {
			if reason == "idle-timeout" && !ei3HasServerTxTimeout {
				t.Logf("EI3 L536 %s: this build has no server transaction timeout setting; the idle holder is checked after 800 ms of idleness anyway", reason)
			}
			store := ei3TxStore(t, 300*time.Millisecond)
			holder := NewStorageExecutor(store)
			_, err := holder.Execute(t.Context(), "BEGIN", nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
			if _, err := holder.Execute(t.Context(), "MERGE (:U {k: 1})", nil); err != nil {
				t.Fatal(err)
			}
			if reason == "terminate" {
				id := holder.txContext.running.id()
				r, err := NewStorageExecutor(store).Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", id), nil)
				t.Logf("EI3 L536 terminate %s -> %s", id, ei3Rows(r, err))
			} else {
				time.Sleep(800 * time.Millisecond)
			}
			// a waiter on the same key
			waitCtx, cancelWait := context.WithTimeout(context.Background(), 4*time.Second)
			started := time.Now()
			_, werr := NewStorageExecutor(store).Execute(waitCtx, "MERGE (:U {k: 1})", nil)
			cancelWait()
			waitElapsed := time.Since(started)
			// an unrelated schema change under a 750 ms cap
			ddlCtx, cancelDDL := context.WithTimeout(context.Background(), 750*time.Millisecond)
			started = time.Now()
			_, derr := NewStorageExecutor(store).Execute(ddlCtx, "CREATE CONSTRAINT ei3_other FOR (n:Other) REQUIRE n.k IS UNIQUE", nil)
			cancelDDL()
			ddlElapsed := time.Since(started)
			_, cerr := holder.Execute(t.Context(), "COMMIT", nil)
			code, _ := nornicerrors.Neo4jStatus(cerr)
			r, err := NewStorageExecutor(store).Execute(t.Context(), "MATCH (u:U {k: 1}) RETURN count(u)", nil)
			t.Logf("EI3 L536 reason=%s waiterMERGE err=%v after %v; CREATE CONSTRAINT err=%v after %v; holder COMMIT status=%q err=%v; count(U{k:1}) %s",
				reason, werr, waitElapsed.Round(time.Millisecond), derr, ddlElapsed.Round(time.Millisecond), code, cerr, ei3Rows(r, err))
			if werr != nil || waitElapsed > time.Second {
				t.Errorf("EI3 L536 %s: waiter blocked by the ended idle holder (%v after %v)", reason, werr, waitElapsed)
			}
			if derr != nil {
				t.Errorf("EI3 L536 %s: schema change blocked by the ended idle holder: %v", reason, derr)
			}
			if cerr == nil {
				t.Errorf("EI3 L536 %s: the ended holder still committed", reason)
			}
		})
	}
}

// ---------------------------------------------------------------- L540
func TestEI3L540EndedStatus(t *testing.T) {
	reasons := []string{"terminate"}
	if ei3HasServerTxTimeout {
		reasons = append(reasons, "timeout")
	}
	for _, reason := range reasons {
		for _, first := range []string{"RETURN 1", "SHOW DATABASES"} {
			t.Run(reason+"/"+first, func(t *testing.T) {
				mgr, store := ei3TxManager(t, 300*time.Millisecond)
				holder := NewStorageExecutor(store)
				holder.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
				if _, err := holder.Execute(t.Context(), "BEGIN", nil); err != nil {
					t.Fatal(err)
				}
				defer holder.Execute(context.Background(), "ROLLBACK", nil)
				if _, err := holder.Execute(t.Context(), first, nil); err != nil {
					t.Fatal(err)
				}
				want := "Neo.ClientError.Transaction.Terminated"
				if reason == "timeout" {
					want = nornicerrors.TransactionTimedOut
					time.Sleep(700 * time.Millisecond)
				} else {
					_, err := NewStorageExecutor(store).Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", holder.txContext.running.id()), nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				next := "SHOW DATABASES"
				if first == next {
					next = "RETURN 1"
				}
				_, err := holder.Execute(t.Context(), next, nil)
				got, _ := nornicerrors.Neo4jStatus(err)
				_, cerr := holder.Execute(t.Context(), "COMMIT", nil)
				cgot, _ := nornicerrors.Neo4jStatus(cerr)
				t.Logf("EI3 L540 reason=%s first=%q next=%q -> status=%s err=%v; COMMIT status=%s", reason, first, next, got, err, cgot)
				if got != want {
					t.Errorf("EI3 L540 %s first=%q next=%q: status %s, want %s", reason, first, next, got, want)
				}
			})
		}
	}
}

// ---------------------------------------------------------------- L543
func ei3AllocOf(t *testing.T, f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func ei3ExplicitRun(t *testing.T, q string) uint64 {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_explicit"))
	if _, err := executor.Execute(t.Context(), "BEGIN", nil); err != nil {
		t.Fatal(err)
	}
	defer executor.Execute(context.Background(), "ROLLBACK", nil)
	var r *ExecuteResult
	var err error
	bytes := ei3AllocOf(t, func() { r, err = executor.executeInTransaction(context.Background(), q, q) })
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	t.Logf("EI3 L543 %q rows=%v allocated=%d bytes", q, r.Rows, bytes)
	return bytes
}

func ei3RangeQuery(n int) string {
	return fmt.Sprintf("UNWIND range(1, %d) AS x WITH x WHERE x < 0 RETURN count(*) AS c", n)
}

// Cold: the test's own measurement first in a fresh process.
func TestEI3L543ColdFirst(t *testing.T) {
	cold := ei3ExplicitRun(t, ei3RangeQuery(30000))
	warm := ei3ExplicitRun(t, ei3RangeQuery(30000))
	big := ei3ExplicitRun(t, ei3RangeQuery(300000))
	tiny := ei3ExplicitRun(t, ei3RangeQuery(3))
	t.Logf("EI3 L543 summary cold30000=%d warm30000=%d warm300000=%d warm3=%d", cold, warm, big, tiny)
}

// Warmed by an unrelated explicit statement first, then the same measurement.
func TestEI3L543WarmedByOtherStatement(t *testing.T) {
	first := ei3ExplicitRun(t, "RETURN 1 AS one")
	measured := ei3ExplicitRun(t, ei3RangeQuery(30000))
	tiny := ei3ExplicitRun(t, ei3RangeQuery(3))
	t.Logf("EI3 L543 summary firstOther=%d then30000=%d then3=%d", first, measured, tiny)
}

// Cold tiny range first: if this allocates as much as the 30000 range, the
// cost is first-statement initialization, not row materialization.
func TestEI3L543ColdTiny(t *testing.T) {
	tiny := ei3ExplicitRun(t, ei3RangeQuery(3))
	measured := ei3ExplicitRun(t, ei3RangeQuery(30000))
	t.Logf("EI3 L543 summary coldTiny3=%d then30000=%d", tiny, measured)
}

// ---------------------------------------------------------------- L549
func TestEI3L549SchemaStatementCache(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "ei3cache")
	exec := ei3NoCacheExecutor(store)
	ids := make([]interface{}, 50)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
		if err := ei3CreateNode(t.Context(), store, &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"CacheNode"}, Properties: map[string]interface{}{"id": fmt.Sprint(i), "payload": strings.Repeat("stored property value ", 8)}}); err != nil {
			t.Fatal(err)
		}
	}
	reads := []struct {
		name, q string
		p       map[string]interface{}
	}{
		{"label-scan-node", "MATCH (n:CacheNode) RETURN n", nil},
		{"label-scan-projection", "MATCH (n:CacheNode) RETURN n.payload", nil},
		{"id-list-batch", "MATCH (n:CacheNode) WHERE n.id IN $ids RETURN n.id, n.payload", map[string]interface{}{"ids": ids}},
		{"unwind-point-reads", "UNWIND $ids AS id MATCH (n:CacheNode {id: id}) RETURN n", map[string]interface{}{"ids": ids}},
	}
	measure := func(phase string) {
		for _, read := range reads {
			runtime.GC()
			var a, b runtime.MemStats
			runtime.ReadMemStats(&a)
			r, err := exec.Execute(context.Background(), read.q, read.p)
			runtime.ReadMemStats(&b)
			if err != nil || len(r.Rows) != 50 {
				t.Fatalf("%s: %v %v", read.name, err, r)
			}
			t.Logf("EI3 L549 phase=%s read=%s bytes=%d allocs=%d", phase, read.name, b.TotalAlloc-a.TotalAlloc, b.Mallocs-a.Mallocs)
		}
	}
	measure("initial")
	measure("warm1")
	measure("warm2")
	if _, err := exec.Execute(context.Background(), "CREATE INDEX ei3_unrelated FOR (n:UnusedLabel) ON (n.k)", nil); err != nil {
		t.Fatal(err)
	}
	measure("after-ddl-1")
	measure("after-ddl-2")
	if _, err := exec.Execute(context.Background(), "CREATE CONSTRAINT ei3_unrelated_c FOR (n:OtherUnused) REQUIRE n.k IS UNIQUE", nil); err != nil {
		t.Fatal(err)
	}
	measure("after-constraint-1")
	measure("after-constraint-2")
}
