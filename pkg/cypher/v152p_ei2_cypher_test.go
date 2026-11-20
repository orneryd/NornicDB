package cypher

// Batch 2 earlier-issue probes on the final build (research only, untracked).

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// L506: a label scan inside an explicit transaction after a peer suppression.
func TestZZEI2SuppressedLabelScan(t *testing.T) {
	for _, pending := range []bool{false, true} {
		for _, scannedFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("pending=%t/scannedFirst=%t", pending, scannedFirst), func(t *testing.T) {
				ctx := t.Context()
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2held")
				exec := NewStorageExecutor(store)
				_, err := exec.Execute(ctx, "CREATE (:Held {id: 1})", nil)
				require.NoError(t, err)
				result, err := exec.Execute(ctx, "MATCH (n:Held) RETURN n", nil)
				require.NoError(t, err)
				id := result.Rows[0][0].(*storage.Node).ID
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
				if pending {
					_, err = exec.Execute(ctx, "MATCH (n:Held) SET n.touched = true", nil)
					require.NoError(t, err)
				}
				if scannedFirst {
					result, err = exec.Execute(ctx, "MATCH (n:Held) RETURN n.id AS id", nil)
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
				}
				require.NoError(t, store.SetManualSuppression(ctx, storage.StoredEntityNodeKind, string(id), true))
				point, err := exec.Execute(ctx, "MATCH (n) WHERE id(n) = $id RETURN n.id AS id", map[string]any{"id": string(id)})
				require.NoError(t, err)
				result, err = exec.Execute(ctx, "MATCH (n:Held) RETURN n.id AS id", nil)
				require.NoError(t, err)
				t.Logf("ZZEI2 L506 pending=%t scannedFirst=%t: point read rows=%v, label scan rows=%v", pending, scannedFirst, point.Rows, result.Rows)
				require.Empty(t, result.Rows, "a transaction label scan must apply live suppression as a fresh read does")
			})
		}
	}
}

// L508: a transaction-first ranked full-text request on an executor with no
// search resolver.
func TestZZEI2TransactionFirstFulltextReuse(t *testing.T) {
	for _, txFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("txFirst=%t", txFirst), func(t *testing.T) {
			exec, store := fulltextMaintainedFixture(t, 80)
			require.Nil(t, exec.search(), "no search service before the first full-text request")
			const query = "CALL db.index.fulltext.queryNodes('source_text','hotel') YIELD node,score RETURN node.id,score ORDER BY node.id"
			if txFirst {
				_, err := exec.Execute(t.Context(), "BEGIN", nil)
				require.NoError(t, err)
				inside, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.NotEmpty(t, inside.Rows)
				_, err = exec.Execute(t.Context(), "ROLLBACK", nil)
				require.NoError(t, err)
			}
			first, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			before := store.scans.Load()
			second, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			third, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
			require.Equal(t, first.Rows, second.Rows)
			require.Equal(t, first.Rows, third.Rows)
			t.Logf("ZZEI2 L508 txFirst=%t: whole-store scans before second request=%d, after third=%d", txFirst, before, store.scans.Load())
			require.Equal(t, before, store.scans.Load(), "a committed full-text request must reuse the corpus")
		})
	}
}

// L510: FOREACH in a correlated CALL with imported variables.
func TestZZEI2ForeachImports(t *testing.T) {
	for _, form := range []string{"CALL (n)", "CALL (*)", "CALL"} {
		for _, horizon := range []string{"", "WITH 42 AS marker "} {
			t.Run(form+"/"+horizon, func(t *testing.T) {
				ctx := context.Background()
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2foreach"))
				_, err := e.Execute(ctx, "CREATE (:ZZForeach {v:0})", nil)
				require.NoError(t, err)
				body := horizon + "FOREACH (i IN [1,2] | SET n.v = i)"
				if form == "CALL" {
					body = "WITH n " + body
				}
				q := "MATCH (n:ZZForeach) " + form + " { " + body + " } RETURN n.v AS v"
				result, err := e.Execute(ctx, q, nil)
				t.Logf("ZZEI2 L510 %q -> rows=%v err=%v", q, zzei2Rows(result), err)
				require.NoError(t, err, q)
				require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
			})
		}
	}
	t.Run("create-with-listed", func(t *testing.T) {
		e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2foreach2"))
		q := "CREATE (n:ZZForeach {v:0}) WITH n CALL (n) { FOREACH (i IN [1,2] | SET n.v = i) } RETURN n.v AS v"
		result, err := e.Execute(context.Background(), q, nil)
		t.Logf("ZZEI2 L510 %q -> rows=%v err=%v", q, zzei2Rows(result), err)
		require.NoError(t, err, q)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
	})
}

func zzei2Rows(result *ExecuteResult) any {
	if result == nil {
		return nil
	}
	return result.Rows
}

// L511: WITH * in CALL ... IN TRANSACTIONS.
func TestZZEI2BatchStarImport(t *testing.T) {
	for _, imports := range []string{"WITH *", "WITH x"} {
		for _, batch := range []string{"", " IN TRANSACTIONS OF 1 ROW"} {
			t.Run(imports+batch, func(t *testing.T) {
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2batch"))
				q := "UNWIND [1,2] AS x CALL { " + imports + " CREATE (:ZZBatch {v:x}) RETURN x AS value }" + batch + " RETURN value ORDER BY value"
				result, err := e.Execute(context.Background(), q, nil)
				t.Logf("ZZEI2 L511 %q -> rows=%v err=%v", q, zzei2Rows(result), err)
				require.NoError(t, err, q)
				require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
				saved, err := e.Execute(context.Background(), "MATCH (n:ZZBatch) RETURN n.v AS v ORDER BY v", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, saved.Rows)
			})
		}
	}
}

// L517: a nested CALL local name must not constrain a UNION branch.
func TestZZEI2NestedLocalUnion(t *testing.T) {
	ctx := context.Background()
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2nested"))
	_, err := e.Execute(ctx, "CREATE (:ZZNested {v:1}), (:ZZNested {v:2})", nil)
	require.NoError(t, err)
	for _, q := range []string{
		// The reported form.
		"MATCH (n:ZZNested {v:1}) CALL { CALL () { MATCH (n:ZZNested) RETURN count(n) AS k } MATCH (n:ZZNested) RETURN n.v AS v UNION WITH n RETURN n.v AS v } RETURN v ORDER BY v",
		// Control without the nested local name.
		"MATCH (n:ZZNested {v:1}) CALL { MATCH (n:ZZNested) RETURN n.v AS v UNION WITH n RETURN n.v AS v } RETURN v ORDER BY v",
	} {
		result, err := e.Execute(ctx, q, nil)
		t.Logf("ZZEI2 L517 %q -> rows=%v err=%v", q, zzei2Rows(result), err)
		require.NoError(t, err, q)
		require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows, q)
	}
}

// L514: remote CALL arity is checked against the coordinator's registry.
func TestZZEI2RemoteProcedureArity(t *testing.T) {
	replaceProcedureRegistryForTest(t, NewProcedureRegistry())
	require.NoError(t, globalProcedureRegistry.RegisterUser(ProcedureSpec{
		Name: "custom.enrich", Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1,
		Params:  []ProcedureParam{{Name: "name", Type: "STRING"}},
		Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}},
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		t.Error("the coordinator must not execute a remote procedure")
		return &ExecuteResult{Columns: []string{"value"}}, nil
	}))
	var calls atomic.Int64
	cfg := multidb.DefaultConfig()
	cfg.RemoteEngineFactory = func(ref multidb.ConstituentRef, _ string) (storage.Engine, error) {
		return storage.NewStatementRemoteEngine(nil, func(_ context.Context, query string, _ map[string]interface{}, _ storage.RemoteAccessMode) ([]string, [][]interface{}, error) {
			if strings.Contains(query, "custom.enrich") {
				calls.Add(1)
				return []string{"value"}, [][]interface{}{{int64(42)}}, nil
			}
			t.Errorf("unexpected remote statement: %s", query)
			return nil, nil, nil
		}), nil
	}
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	mgr, err := multidb.NewDatabaseManager(t.Context(), base, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{
		{Alias: "remote", DatabaseName: "remote_one", Type: "remote", AccessMode: "read", URI: "http://remote.example"},
	}))
	adapter := &testDatabaseManagerAdapter{manager: mgr}
	exec := newCompositeTestExecutor(t, mgr, mgr.DefaultDatabaseName(), adapter)
	for _, args := range []string{"'alice'", "'alice', {}"} {
		t.Run(args, func(t *testing.T) {
			q := "USE comp.remote CALL custom.enrich(" + args + ") YIELD value RETURN value"
			result, err := exec.Execute(t.Context(), q, nil)
			t.Logf("ZZEI2 L514 %q -> rows=%v err=%v remote calls so far=%d", q, zzei2Rows(result), err, calls.Load())
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(42)}}, result.Rows)
		})
	}
	require.Equal(t, int64(2), calls.Load(), "both signatures belong to the remote participant")
}

// L515: end-seeded ORDER BY ... LIMIT with a repeated variable.
func TestZZEI2EndSeedRepeated(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("indexed=%v/tx=%v", indexed, explicit), func(t *testing.T) {
				e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2endseed"))
				ctx := t.Context()
				_, err := e.Execute(ctx, `CREATE (a:P {id:1})-[:SELF]->(b:P {id:2}), (c:P {id:3})-[:SELF]->(c)`, nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, `UNWIND range(4, 250) AS id CREATE (:P {id:id})`, nil)
				require.NoError(t, err)
				if indexed {
					_, err = e.Execute(ctx, `CREATE INDEX p_id FOR (p:P) ON (p.id)`, nil)
					require.NoError(t, err)
				}
				if explicit {
					_, err = e.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer e.Execute(ctx, "ROLLBACK", nil)
				}
				for _, q := range []string{
					`MATCH (p)-[:SELF]->(p:P) RETURN p.id ORDER BY p.id LIMIT 1`,
					`MATCH (p:P)-[:SELF]->(p) RETURN p.id ORDER BY p.id LIMIT 1`,
					`MATCH (p)-[:SELF]->(p:P) RETURN p.id ORDER BY p.id`,
				} {
					result, err := e.Execute(ctx, q, nil)
					t.Logf("ZZEI2 L515 indexed=%v tx=%v %q -> rows=%v err=%v", indexed, explicit, q, zzei2Rows(result), err)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows, q)
				}
			})
		}
	}
}

type zzei2CostEngine struct {
	*storage.NamespacedEngine
	reads atomic.Int64
}

func (s *zzei2CostEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	s.reads.Add(1)
	return s.NamespacedEngine.ReadAdjacentEdges(ctx, request)
}

// L516: LIMIT 1 on a repeated endpoint stops early.
func TestZZEI2RepeatedEndpointCost(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2cost")
	ctx := t.Context()
	for i := 0; i < 1000; i++ {
		id := storage.NodeID(fmt.Sprintf("node-%04d", i))
		_, err := store.CreateNode(ctx, &storage.Node{ID: id, Labels: []string{"P"}, Properties: map[string]any{"id": int64(i)}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("edge-%04d", i)), StartNode: id, EndNode: id, Type: "SELF"}))
	}
	plain := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	measure := func(q string) (int64, uint64) {
		probe := &zzei2CostEngine{NamespacedEngine: store}
		result, err := NewStorageExecutor(probe, QueryCachePolicy{MaxEntries: 0}).Execute(ctx, q, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		_, err = plain.Execute(ctx, q, nil)
		require.NoError(t, err)
		runtime.GC()
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		for i := 0; i < 5; i++ {
			_, err = plain.Execute(ctx, q, nil)
			require.NoError(t, err)
		}
		runtime.ReadMemStats(&b)
		return probe.reads.Load(), (b.TotalAlloc - a.TotalAlloc) / 5
	}
	repeatedReads, repeatedBytes := measure(`MATCH (p:P)-[:SELF]->(p) RETURN p.id LIMIT 1`)
	controlReads, controlBytes := measure(`MATCH (p:P)-[:SELF]->(q) RETURN p.id LIMIT 1`)
	t.Logf("ZZEI2 L516 1000 self-loops LIMIT 1: repeated endpoint adjacency reads=%d bytes/run=%d; distinct endpoint control reads=%d bytes/run=%d", repeatedReads, repeatedBytes, controlReads, controlBytes)
	require.LessOrEqual(t, repeatedReads, int64(2), "LIMIT 1 must stop once one identity-consistent path is accepted")
}

// L520: EXPLAIN / PROFILE of schema statements.
func TestZZEI2ExplainSchema(t *testing.T) {
	cases := []struct {
		name, query string
		wantError   bool
	}{
		{"node_key", "EXPLAIN CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY", false},
		{"profile_node_key", "PROFILE CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY", false},
		{"relationship_key", "EXPLAIN CREATE CONSTRAINT review_key FOR ()-[r:Review]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY", false},
		{"unique", "EXPLAIN CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, n.b) IS UNIQUE", false},
		{"missing_options_parameter", "EXPLAIN CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY OPTIONS $options", false},
		{"unevaluated_options", "EXPLAIN CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY OPTIONS {indexProvider: toString(1/0)}", false},
		{"wrong_variable", "EXPLAIN CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (m.a, m.b) IS NODE KEY", true},
		{"nonproperty", "EXPLAIN CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, 7) IS NODE KEY", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2explain"))
			result, err := e.Execute(t.Context(), c.query, nil)
			var cols any
			if result != nil {
				cols = result.Columns
			}
			t.Logf("ZZEI2 L520 %s: query=%s columns=%v error=%v", c.name, c.query, cols, err)
			if (err != nil) != c.wantError {
				t.Errorf("wantError=%v, got %v", c.wantError, err)
			}
			shown, err := e.Execute(t.Context(), "SHOW CONSTRAINTS YIELD name RETURN name", nil)
			require.NoError(t, err)
			require.Empty(t, shown.Rows, "EXPLAIN/PROFILE must not create the constraint")
		})
	}
}

// L523: db.labels() reads node images with their chunk vectors.
func TestZZEI2DbLabelsVectors(t *testing.T) {
	g := newNOR413Graph(t, 16, 256)
	exec := NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	run := func(q string) {
		result, err := exec.Execute(context.Background(), q, nil)
		require.NoError(t, err)
		require.NotEmpty(t, result.Rows)
	}
	nothing := func() {}
	begin := func() { zzei2Run(t, exec, "BEGIN") }
	rollback := func() { zzei2Run(t, exec, "ROLLBACK") }
	for _, q := range []string{
		"CALL db.labels() YIELD label RETURN label",
		"CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey",
		"MATCH (n) RETURN count(n) AS c",
		"MATCH (n) RETURN DISTINCT labels(n) AS l",
	} {
		auto := nor413Allocated(t, nothing, func() { run(q) }, nothing)
		tx := nor413Allocated(t, begin, func() { run(q) }, rollback)
		t.Logf("ZZEI2 L523 %q: auto-commit %.2f MiB, explicit tx %.2f MiB; stored chunk vectors %.2f MiB",
			q, float64(auto)/(1<<20), float64(tx)/(1<<20), float64(g.vectorBytes)/(1<<20))
	}
	auto := nor413Allocated(t, nothing, func() { run("CALL db.labels() YIELD label RETURN label") }, nothing)
	require.Less(t, auto, g.vectorBytes/4, "db.labels() must not load the stored chunk vectors to list label names")
}

func zzei2Run(t *testing.T, exec *StorageExecutor, q string) {
	t.Helper()
	_, err := exec.Execute(context.Background(), q, nil)
	require.NoError(t, err)
}

// L524: the node cache keeps full vectors and copies them on every hit.
func TestZZEI2NodeCacheVectorCopies(t *testing.T) {
	g := newNOR413Graph(t, 4, 256)
	id := storage.NodeID("test:src-0")
	perNode := uint64(256 * nor413Dims * 4)
	for i := 0; i < 3; i++ {
		node, err := g.engine.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.Len(t, node.ChunkEmbeddings, 256)
	}
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	const hits = 10
	for i := 0; i < hits; i++ {
		node, err := g.engine.GetNode(t.Context(), id)
		require.NoError(t, err)
		require.Len(t, node.ChunkEmbeddings, 256)
	}
	runtime.ReadMemStats(&b)
	perHit := (b.TotalAlloc - a.TotalAlloc) / hits
	// Metadata-only reads of the same node for comparison.
	runtime.GC()
	runtime.ReadMemStats(&a)
	for i := 0; i < hits; i++ {
		_, err := g.engine.GetQueryNode(t.Context(), id)
		require.NoError(t, err)
	}
	runtime.ReadMemStats(&b)
	perQuery := (b.TotalAlloc - a.TotalAlloc) / hits
	t.Logf("ZZEI2 L524 warm GetNode of a node with %d chunk vectors (%.2f MiB): %.2f MiB allocated per hit; GetQueryNode %.3f MiB per hit",
		256, float64(perNode)/(1<<20), float64(perHit)/(1<<20), float64(perQuery)/(1<<20))
	require.Less(t, perHit, perNode/4, "a warm full read copies every stored vector on each hit")
}

// L525: the vector-free read setting across per-statement executor copies.
func TestZZEI2VectorFreeExecutorCopies(t *testing.T) {
	g := newNOR413Graph(t, 16, 64)
	exec := NewStorageExecutor(g.store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	params := map[string]interface{}{"ids": g.ids}
	nothing := func() {}
	for _, tc := range []struct{ name, query string }{
		{"control-without-subquery", nor413WithoutCall},
		{"count-subquery", "UNWIND $ids AS id MATCH (d:Document {id: id}) RETURN d.id AS x, COUNT { MATCH (s:Source {id: replace(id, 'doc-', 'src-')}) WHERE s.title IS NOT NULL } AS c"},
		{"exists-subquery", "UNWIND $ids AS id MATCH (d:Document {id: id}) WHERE EXISTS { MATCH (s:Source {id: replace(id, 'doc-', 'src-')}) WHERE s.title IS NOT NULL } RETURN d.id AS x"},
		{"collect-subquery", "UNWIND $ids AS id MATCH (d:Document {id: id}) RETURN d.id AS x, COLLECT { MATCH (s:Source {id: replace(id, 'doc-', 'src-')}) RETURN s.title } AS c"},
		{"call-subquery-in-line", "UNWIND $ids AS id CALL (id) { MATCH (s:Source {id: replace(id, 'doc-', 'src-')}) RETURN s.title AS t } RETURN t"},
		{"call-in-transactions", "UNWIND $ids AS id CALL (id) { MATCH (s:Source {id: replace(id, 'doc-', 'src-')}) RETURN s.title AS t } IN TRANSACTIONS OF 4 ROWS RETURN t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), tc.query, params)
			require.NoError(t, err, tc.query)
			require.Len(t, result.Rows, len(g.ids))
			bytes := nor413Allocated(t, nothing, func() {
				_, err := exec.Execute(context.Background(), tc.query, params)
				require.NoError(t, err)
			}, nothing)
			t.Logf("ZZEI2 L525 %s: auto-commit %.2f MiB; matched chunk vectors %.2f MiB", tc.name, float64(bytes)/(1<<20), float64(g.vectorBytes)/(1<<20))
			require.Less(t, bytes, g.vectorBytes/4, "a read-only statement does not load the chunk vectors it never reads")
		})
	}
}

// L526: apoc.periodic.iterate with params on both parsers.
func TestZZEI2PeriodicIterateParams(t *testing.T) {
	for _, tc := range []struct{ name, config string }{
		{"outer-parameter", "{batchSize: 1, params: {v: $v}}"},
		{"literal", "{batchSize: 1, params: {v: 7}}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2iterate"))
			q := "CALL apoc.periodic.iterate('UNWIND [1,2,3] AS i RETURN i', 'CREATE (:ZZIter {i: i, v: $v})', " + tc.config + ") YIELD batches, total RETURN batches, total"
			result, err := e.Execute(t.Context(), q, map[string]any{"v": int64(7)})
			t.Logf("ZZEI2 L526 %s: %q -> rows=%v err=%v", tc.name, q, zzei2Rows(result), err)
			require.NoError(t, err)
			saved, err := e.Execute(t.Context(), "MATCH (n:ZZIter) RETURN n.i AS i, n.v AS v ORDER BY i", nil)
			require.NoError(t, err)
			t.Logf("ZZEI2 L526 %s: stored rows=%v", tc.name, saved.Rows)
			require.Equal(t, [][]interface{}{{int64(1), int64(7)}, {int64(2), int64(7)}, {int64(3), int64(7)}}, saved.Rows)
		})
	}
}
