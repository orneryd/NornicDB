package cypher

// Native consumers retain complete node images across automatic, explicit and
// Fabric execution. Metadata-only consumers share a consistent body-only image,
// including vector-search output after scoring, without a newer-snapshot read.

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"testing"

	apoccoll "github.com/orneryd/nornicdb/apoc/coll"
	apocconvert "github.com/orneryd/nornicdb/apoc/convert"
	apocmap "github.com/orneryd/nornicdb/apoc/map"
	"github.com/orneryd/nornicdb/apoc/registry"
	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor413VectorComponent is the fourth component of value's default
// embedding when value is a node that carries one, else nil.
func nor413VectorComponent(value interface{}) interface{} {
	switch typed := value.(type) {
	case *storage.Node:
		if vector := typed.GetDefaultEmbedding(); len(vector) > 3 {
			return float64(vector[3])
		}
	case []interface{}:
		if len(typed) == 1 {
			return nor413VectorComponent(typed[0])
		}
	case map[string]interface{}:
		return nor413VectorComponent(typed["node"])
	}
	return nil
}

// newNOR413ColdDocGraph is largeEmbeddingGraph on an engine whose node cache
// holds one entry, with doc-3 (vector component 3 set) out of it: a graph
// larger than its node cache.
func newNOR413ColdDocGraph(t *testing.T) *largeEmbeddingGraph {
	t.Helper()
	g := newColdLargeEmbeddingGraph(t, 4)
	stored := g.storedChunks(t, "doc-3")
	require.Len(t, stored, 1)
	require.Equal(t, float32(1), stored[0][3])
	for _, id := range []storage.NodeID{"doc-0", "doc-1", "hub"} {
		_, err := g.store.GetNode(t.Context(), id)
		require.NoError(t, err)
	}
	return g
}

// TestNOR413NativeReadProcedureReceivesStoredVectors is the first review's
// reproduction: in auto-commit, a registered READ procedure that reads its
// node argument's default embedding gets the stored vector, for a node passed
// directly, in a list and in a map.
func TestNOR413NativeReadProcedureReceivesStoredVectors(t *testing.T) {
	const name = "nor413.vectorScore"
	cleanupAdmissionProcedure(t, name)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: name, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1,
		Returns: []ProcedureColumn{{Name: "score", Type: "FLOAT"}},
	}, func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"score"}, Rows: [][]interface{}{{nor413VectorComponent(args[0])}}}, nil
	}))
	g := newNOR413ColdDocGraph(t)
	for _, argument := range []string{"n", "[n]", "{node: n}"} {
		t.Run(argument, func(t *testing.T) {
			result := g.run(t, "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL nor413.vectorScore("+argument+") YIELD score RETURN n.title, score", nil)
			require.Equal(t, [][]interface{}{{"doc 3", float64(1)}}, result.Rows)
		})
	}
}

// TestNOR413NativePluginFunctionReceivesStoredVectors: in an auto-commit
// statement with CALL (a procedure or a CALL subquery), a plugin function that
// reads its node argument's default embedding gets the stored vector, as on
// the release head.
func TestNOR413NativePluginFunctionReceivesStoredVectors(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
		if name == "nor413.vectorComponent" {
			return func(value interface{}) interface{} { return nor413VectorComponent(value) }, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	})
	g := newNOR413ColdDocGraph(t)
	for _, query := range []string{
		"MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL db.ping() YIELD success RETURN n.title, nor413.vectorComponent([n])",
		"MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL { WITH n RETURN n.title AS title } RETURN title, nor413.vectorComponent(n)",
		"MATCH (h:Hub) CALL { WITH h MATCH (h)-[:HAS]->(n:Doc {k: 3}) RETURN n } RETURN n.title, nor413.vectorComponent(n)",
	} {
		t.Run(query, func(t *testing.T) {
			require.Equal(t, [][]interface{}{{"doc 3", float64(1)}}, g.run(t, query, nil).Rows)
		})
	}
}

// nor413CallLookup serves the plugin functions of the CALL statement tests:
// the bundled APOC library's apoc.map.setKey and apoc.coll.contains, and a
// third-party nor413.title.
func nor413CallLookup(t *testing.T) {
	t.Helper()
	setKey, err := registry.Compile(apocmap.SetKey, registry.CanonicalReads)
	require.NoError(t, err)
	contains, err := registry.Compile(apoccoll.Contains, registry.CanonicalReads)
	require.NoError(t, err)
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	external := checkedTestLookup(func(name string) (interface{}, bool) {
		if name == "nor413.title" {
			return func(value interface{}) interface{} { return value.(*storage.Node).Properties["title"] }, true
		}
		return nil, false
	})
	PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
		switch name {
		case "apoc.map.setKey":
			return setKey, true
		case "apoc.coll.contains":
			return contains, true
		}
		return external(name)
	}
}

// nor413RegisterNodeTitle registers nor413.nodeTitle, a user READ procedure
// that returns its node argument's title.
func nor413RegisterNodeTitle(t *testing.T) {
	t.Helper()
	const name = "nor413.nodeTitle"
	cleanupAdmissionProcedure(t, name)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: name, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1,
		Returns: []ProcedureColumn{{Name: "title", Type: "STRING"}},
	}, func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"title"}, Rows: [][]interface{}{{args[0].(*storage.Node).Properties["title"]}}}, nil
	}))
}

// TestNOR413CallStatementEligibility checks the admitted image declaration:
// metadata-only procedures and ordinary subqueries avoid stored vectors;
// native procedures and entity-valued native function inputs retain them.
func TestNOR413CallStatementEligibility(t *testing.T) {
	nor413CallLookup(t)
	nor413RegisterNodeTitle(t)
	g := newLargeEmbeddingGraph(t, 4)
	const match = "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) "
	for _, tc := range []struct {
		query      string
		vectorFree bool
	}{
		{match + "RETURN n", true},
		{"MATCH (h:Hub) CALL { WITH h MATCH (h)-[:HAS]->(n:Doc {k: 3}) RETURN n } RETURN n", true},
		{match + "CALL (n) { RETURN n.title AS title } RETURN title", true},
		{match + "CALL { WITH n CALL { WITH n RETURN n AS m } RETURN m } RETURN m", true},
		{match + "CALL { WITH n RETURN apoc.map.setKey({}, 'node', n) AS m } RETURN m", true},
		{match + "CALL { WITH n RETURN n.use AS u, n.transactions AS tx } RETURN u, tx", true},
		{match + "CALL db.ping() YIELD success RETURN n", true},
		{match + "CALL { WITH n CALL db.ping() YIELD success RETURN success } RETURN n", true},
		{match + "CALL { WITH n RETURN n AS m } WITH m WHERE EXISTS { CALL db.labels() YIELD label RETURN label } RETURN m", true},
		{match + "CALL apoc.path.subgraphNodes(n, {maxLevel: 0}) YIELD node RETURN node", true},
		{match + "CALL db.index.vector.queryNodes('doc_chunks', 1, $q) YIELD node AS m RETURN apoc.coll.contains([n], m)", true},
		{match + "CALL nor413.nodeTitle(n) YIELD title RETURN title", false},
		{match + "CALL { WITH n RETURN n.title AS title } RETURN title, nor413.title(n)", false},
		{match + "CALL { WITH n RETURN n AS m } IN TRANSACTIONS RETURN m", true},
		{match + "CALL { USE neo4j MATCH (m:Doc {k: 3}) RETURN m } RETURN n, m", true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			info, err := g.exec.analyzer.Analyze(t.Context(), tc.query)
			require.NoError(t, err)
			require.True(t, info.ReadOnly(), "the effect owner reads the statement as read-only")
			statement := &statementExecutionContext{Context: t.Context()}
			require.NoError(t, g.exec.resolveStatementNodeImages(statement, info, tc.query))
			require.Equal(t, tc.vectorFree, statement.nodeImages.images == MetadataNodeImages)
		})
	}
}

// Graph selection and batch ownership do not themselves imply a native node
// consumer. The enclosing policy follows the actual registered consumers.
func TestStatementNodeImagesAcrossSubqueryOwners(t *testing.T) {
	exec := newLargeEmbeddingGraph(t, 2).exec
	for _, query := range []string{
		"USE g MATCH (n) RETURN n",
		"MATCH (n) RETURN n.use AS u, n.transactions AS tx",
		"USE g MATCH (n) CALL { WITH n RETURN n AS m } RETURN m",
		"MATCH (n) CALL { USE g MATCH (m) RETURN m } RETURN n, m",
		"USE g CALL { USE g.one MATCH (m) RETURN m } RETURN m",
		"MATCH (n) CALL { WITH n RETURN n AS m } IN TRANSACTIONS RETURN m",
		"MATCH (n) CALL { WITH n CALL { USE g MATCH (m) RETURN m } RETURN m } RETURN m",
	} {
		info, err := exec.analyzer.Analyze(t.Context(), query)
		require.NoError(t, err)
		statement := &statementExecutionContext{Context: t.Context()}
		require.NoError(t, exec.resolveStatementNodeImages(statement, info, query))
		require.Equal(t, MetadataNodeImages, statement.nodeImages.images, query)
	}
}

// TestNOR413CallStatementReadModes verifies that each admitted statement uses
// one consistent node representation, according to its actual consumers.
func TestNOR413CallStatementReadModes(t *testing.T) {
	nor413CallLookup(t)
	nor413RegisterNodeTitle(t)
	g := newLargeEmbeddingGraph(t, 4)
	spy := storagefault.NewQueryNodeReadSpy(g.store)
	exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	const match = "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) "
	for _, tc := range []struct {
		query      string
		vectorFree bool
	}{
		{"MATCH (h:Hub) CALL { WITH h MATCH (h)-[:HAS]->(n:Doc {k: 3}) RETURN n } RETURN n.title", true},
		{match + "CALL (n) { RETURN n.title AS title } RETURN title", true},
		{match + "CALL { WITH n CALL { WITH n RETURN n AS m } RETURN m } RETURN m.title", true},
		{match + "CALL { WITH n RETURN apoc.map.setKey({}, 'node', n) AS m } RETURN m.node.title", true},
		{match + "CALL db.ping() YIELD success RETURN n.title", true},
		{match + "CALL { WITH n CALL db.ping() YIELD success RETURN success } RETURN n.title", true},
		{match + "CALL apoc.path.subgraphNodes(n, {maxLevel: 0}) YIELD node RETURN node.title", true},
		{match + "CALL nor413.nodeTitle(n) YIELD title RETURN title", false},
		{match + "CALL { WITH n RETURN n.title AS title } RETURN nor413.title(n)", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			spy.Full.Store(0)
			spy.Query.Store(0)
			result, err := exec.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"doc 3"}}, result.Rows)
			nor413RequireReadMode(t, spy, tc.vectorFree)
		})
	}
	bundled, err := registry.Compile(apocconvert.ToString, registry.CanonicalReads)
	require.NoError(t, err)
	require.False(t, nativePluginFunction(bundled), bundled.Symbol())
	thirdParty, err := registry.Compile(func(value interface{}) interface{} { return value }, registry.OpaqueReads)
	require.NoError(t, err)
	require.True(t, nativePluginFunction(thirdParty), thirdParty.Symbol())
}

// TestNOR413VectorSearchMembershipKeepsOneReadMode preserves same-node
// membership across MATCH and both service/exact vector scoring. The statement
// returns consistent metadata images after scoring; native vector consumers
// are verified independently with full images.
func TestNOR413VectorSearchMembershipKeepsOneReadMode(t *testing.T) {
	nor413CallLookup(t)
	q := make([]interface{}, 2048)
	for i := range q {
		q[i] = 0.0
	}
	q[3] = 1.0
	params := map[string]interface{}{"q": q}
	const vectorSearch = "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL db.index.vector.queryNodes('doc_chunks', 1, $q) YIELD node AS m, score RETURN id(n) = id(m), apoc.coll.contains([n], m)"
	const subquery = "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) CALL { WITH n MATCH (m:Doc {k: 3}) RETURN m } RETURN id(n) = id(m), apoc.coll.contains([n], m)"
	for _, wired := range []bool{false, true} {
		t.Run(fmt.Sprintf("search-service=%v", wired), func(t *testing.T) {
			g := newNOR413ColdDocGraph(t)
			if wired {
				service := search.NewService(g.store, 2048, nil)
				t.Cleanup(func() { _ = service.Close() })
				require.NoError(t, service.BuildIndexes(t.Context()))
				g.exec.SetSearchService(service)
				_, err := g.store.GetNode(t.Context(), "hub")
				require.NoError(t, err)
			}
			for _, query := range []string{vectorSearch, subquery} {
				require.Equal(t, [][]interface{}{{true, true}}, g.run(t, query, params).Rows, query)
			}
			// Exact scoring projects its caller-owned image after scoring. That
			// projection must never alter the retained stored vectors.
			chunks := g.storedChunks(t, "doc-3")
			require.Len(t, chunks, 1)
			require.Equal(t, float32(1), chunks[0][3])
			require.Equal(t, [][]interface{}{{true, true}}, g.run(t, vectorSearch, params).Rows)
		})
	}
}

// TestNOR413NativeConsumersAcrossFabric distinguishes equal local IDs in two
// constituents. Native consumers at either the fragment or coordinator receive
// the originating node's complete image in automatic and explicit transactions.
func TestNOR413NativeConsumersAcrossFabric(t *testing.T) {
	const name = "nor413.fabricScore"
	cleanupAdmissionProcedure(t, name)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: name, Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1,
		Returns: []ProcedureColumn{{Name: "score", Type: "FLOAT"}},
	}, func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"score"}, Rows: [][]interface{}{{nor413VectorComponent(args[0])}}}, nil
	}))
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
		if name == "nor413.fabricComponent" {
			return func(value interface{}) interface{} { return nor413VectorComponent(value) }, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	})
	base, err := fixture.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, NodeCacheMaxEntries: 1}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	manager, err := multidb.NewDatabaseManager(t.Context(), base, multidb.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	for i, database := range []string{"one", "two"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: database}))
		store, err := manager.GetStorage(database)
		require.NoError(t, err)
		_, err = store.CreateNode(t.Context(), &storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": database}})
		require.NoError(t, err)
		stored, err := base.GetNode(t.Context(), storage.NodeID(database+":doc"))
		require.NoError(t, err)
		vector := make([]float32, 2048)
		vector[3] = float32(i + 1)
		stored.ChunkEmbeddings = [][]float32{vector}
		stored.EmbedMeta = map[string]interface{}{"has_embedding": true, "chunk_count": 1}
		require.NoError(t, base.UpdateNodeEmbeddingSidecar(t.Context(), stored))
		_, err = store.CreateNode(t.Context(), &storage.Node{ID: "hub", Labels: []string{"Hub"}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "link", StartNode: "hub", EndNode: "doc", Type: "HAS"}))
	}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "one", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "two", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage("comp")
	require.NoError(t, err)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	for _, explicit := range []bool{false, true} {
		for _, query := range []string{
			"USE comp.one MATCH (:Hub)-[:HAS]->(n:Doc) CALL nor413.fabricScore(n) YIELD score RETURN score",
			"USE comp CALL { USE comp.one MATCH (:Hub)-[:HAS]->(n:Doc) CALL nor413.fabricScore(n) YIELD score RETURN score } RETURN score",
			"USE comp CALL { USE comp.one MATCH (:Hub)-[:HAS]->(n:Doc) CALL db.ping() YIELD success RETURN n } RETURN nor413.fabricComponent(n)",
			"USE comp CALL { USE comp.two MATCH (:Hub)-[:HAS]->(n:Doc) CALL db.ping() YIELD success RETURN n } RETURN nor413.fabricComponent(n) / 2.0",
		} {
			t.Run(fmt.Sprintf("explicit=%v/%s", explicit, query), func(t *testing.T) {
				// Keep the source outside the one-entry full-node cache.
				_, err := base.GetNode(t.Context(), "one:hub")
				require.NoError(t, err)
				if explicit {
					_, err = exec.Execute(t.Context(), "BEGIN", nil)
					require.NoError(t, err)
					defer exec.Execute(t.Context(), "ROLLBACK", nil)
				}
				result, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Equal(t, float64(1), result.Rows[0][0], "the native consumer receives the exact vectors of its own constituent")
			})
		}
	}
}

// TestNOR413ScalarPluginCallKeepsReadCost is the third review's
// reproduction: a plugin function given only a property value leaves the row
// node's 4 MiB of stored chunk vectors unread, auto-commit and in a
// transaction, within the release head's allocation (about 4 MiB auto-commit,
// 34 KiB in a transaction).
func TestNOR413ScalarPluginCallKeepsReadCost(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
		if name == "nor413.same" {
			return func(value interface{}) interface{} { return value }, true
		}
		return nil, false
	})
	g := newColdLargeEmbeddingGraph(t, 4)
	node, err := g.engine.GetNode(t.Context(), "test:doc-3")
	require.NoError(t, err)
	node.ChunkEmbeddings = make([][]float32, 512)
	for i := range node.ChunkEmbeddings {
		node.ChunkEmbeddings[i] = make([]float32, 2048)
	}
	node.EmbedMeta["chunk_count"] = 512
	require.NoError(t, g.engine.UpdateNodeEmbeddingSidecar(t.Context(), node))
	require.Len(t, g.storedChunks(t, "doc-3"), 512)
	const query = "MATCH (:Hub)-[:HAS]->(n:Doc {k: 3}) RETURN nor413.same(n.title)"
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			if explicit {
				g.run(t, "BEGIN", nil)
				defer g.run(t, "ROLLBACK", nil)
			}
			var rows [][]interface{}
			allocated := nor413Allocated(t, func() {}, func() { rows = g.run(t, query, nil).Rows }, func() {})
			require.Equal(t, [][]interface{}{{"doc 3"}}, rows)
			require.Less(t, allocated, uint64(6<<20), "a scalar plugin call reads no vectors")
		})
	}
}
