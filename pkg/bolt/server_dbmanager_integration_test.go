package bolt

import (
	"context"
	"fmt"
	"net"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSessionGetExecutorForDatabase_WiresDatabaseManagerCommands(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("tenant_a"))

	s := &Session{server: &Server{dbManager: mgr}}
	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)

	res, err := exec.Execute(context.Background(), "SHOW DATABASES", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)
}

func TestSessionGetExecutorForDatabase_RollbackRemovesDatabaseScopedWrites(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	exec := transactionalDatabaseScopedExecutor(t, store)
	txExec, ok := exec.(TransactionalExecutor)
	require.True(t, ok, "database-scoped Bolt executor must support real explicit transactions")

	ctx := context.Background()
	require.NoError(t, txExec.BeginTransaction(ctx, nil))
	_, err := txExec.Execute(ctx, "CREATE (n:TxProbe {id: 'clean-rollback'})", nil)
	require.NoError(t, err)
	require.NoError(t, txExec.RollbackTransaction(ctx))

	count := databaseScopedNodeCount(t, exec, "clean-rollback")
	require.Equal(t, int64(0), count)
}

func TestSessionGetExecutorForDatabase_RollbackAfterStatementFailureRemovesPriorWrites(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	exec := transactionalDatabaseScopedExecutor(t, store)
	txExec, ok := exec.(TransactionalExecutor)
	require.True(t, ok, "database-scoped Bolt executor must support real explicit transactions")

	ctx := context.Background()
	require.NoError(t, txExec.BeginTransaction(ctx, nil))
	_, err := txExec.Execute(ctx, "CREATE (n:TxProbe {id: 'failed-statement-rollback'})", nil)
	require.NoError(t, err)
	_, err = txExec.Execute(ctx, "THIS IS NOT CYPHER", nil)
	require.Error(t, err)
	require.NoError(t, txExec.RollbackTransaction(ctx))

	count := databaseScopedNodeCount(t, exec, "failed-statement-rollback")
	require.Equal(t, int64(0), count)
}

func TestDatabaseManagerBoltNeo4jDriverExecuteWriteRollsBackPriorWrites(t *testing.T) {
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": store,
		},
		defaultDB: "nornic",
	}
	server := NewWithDatabaseManager(&Config{
		Port:            0,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	t.Cleanup(func() {
		_ = server.Close()
	})
	port := startBoltTestServer(t, server)

	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = driver.Close(context.Background())
	})
	require.NoError(t, driver.VerifyConnectivity(ctx))

	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	defer func() {
		_ = session.Close(ctx)
	}()

	_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, runErr := tx.Run(ctx, "CREATE (n:TxProbe {id: $id})", map[string]any{"id": "driver-rollback"})
		if runErr != nil {
			return nil, runErr
		}
		if _, consumeErr := result.Consume(ctx); consumeErr != nil {
			return nil, consumeErr
		}
		result, runErr = tx.Run(ctx, "THIS IS NOT CYPHER", nil)
		if runErr != nil {
			return nil, runErr
		}
		_, consumeErr := result.Consume(ctx)
		return nil, consumeErr
	})
	require.Error(t, err)

	count := databaseScopedNodeCount(t, databaseScopedExecutor(t, store), "driver-rollback")
	require.Equal(t, int64(0), count)
}

func TestDatabaseManagerBoltNeo4jDriverExecuteWritePrefixesGeneratedNodeIDs(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.Close()
	})

	server := NewWithDatabaseManager(&Config{
		Port:            0,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	t.Cleanup(func() {
		_ = server.Close()
	})
	port := startBoltTestServer(t, server)

	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = driver.Close(context.Background())
	})
	require.NoError(t, driver.VerifyConnectivity(ctx))

	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	defer func() {
		_ = session.Close(ctx)
	}()

	_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, runErr := tx.Run(ctx, "MERGE (d:Directory {path: $path}) SET d.name = $name", map[string]any{
			"path": "/repo/internal/runtime",
			"name": "runtime",
		})
		if runErr != nil {
			return nil, runErr
		}
		_, consumeErr := result.Consume(ctx)
		return nil, consumeErr
	})
	require.NoError(t, err)

	store, err := mgr.GetStorage("nornic")
	require.NoError(t, err)
	exec := cypher.NewStorageExecutor(store)
	res, err := exec.Execute(ctx,
		"MATCH (d:Directory {path: $path}) RETURN count(d) AS c",
		map[string]any{"path": "/repo/internal/runtime"},
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])
}

func TestDatabaseManagerBoltNeo4jDriverExecuteWritePrefixesCanonicalUnwindIDs(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.Close()
	})

	server := NewWithDatabaseManager(&Config{
		Port:            0,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	t.Cleanup(func() {
		_ = server.Close()
	})
	port := startBoltTestServer(t, server)

	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = driver.Close(context.Background())
	})
	require.NoError(t, driver.VerifyConnectivity(ctx))

	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	defer func() {
		_ = session.Close(ctx)
	}()

	_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
		statements := []struct {
			query  string
			params map[string]any
		}{
			{
				query: "MERGE (r:Repository {id: $repo_id}) SET r.name = $name, r.path = $path",
				params: map[string]any{
					"repo_id": "pcg-nornicdb-canonical",
					"name":    "pcg-nornicdb-canonical",
					"path":    "/tmp/pcg-nornicdb-canonical",
				},
			},
			{
				query: `UNWIND $rows AS row
MATCH (r:Repository {id: row.repo_id})
MERGE (d:Directory {path: row.path})
SET d.name = row.name, d.repo_id = row.repo_id,
    d.scope_id = row.scope_id, d.generation_id = row.generation_id
MERGE (r)-[:CONTAINS]->(d)`,
				params: map[string]any{
					"rows": []map[string]any{{
						"repo_id":       "pcg-nornicdb-canonical",
						"path":          "/tmp/pcg-nornicdb-canonical/src",
						"name":          "src",
						"scope_id":      "scope:pcg-nornicdb-canonical",
						"generation_id": "generation:pcg-nornicdb-canonical",
					}},
				},
			},
		}
		for _, stmt := range statements {
			result, runErr := tx.Run(ctx, stmt.query, stmt.params)
			if runErr != nil {
				return nil, runErr
			}
			if _, consumeErr := result.Consume(ctx); consumeErr != nil {
				return nil, consumeErr
			}
		}
		return nil, nil
	})
	require.NoError(t, err)

	store, err := mgr.GetStorage("nornic")
	require.NoError(t, err)
	exec := cypher.NewStorageExecutor(store)
	res, err := exec.Execute(ctx,
		"MATCH (:Repository {id: $repo_id})-[:CONTAINS]->(:Directory {path: $path}) RETURN count(*) AS c",
		map[string]any{
			"repo_id": "pcg-nornicdb-canonical",
			"path":    "/tmp/pcg-nornicdb-canonical/src",
		},
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 1, res.Rows[0][0])
}

func databaseScopedExecutor(t *testing.T, store storage.Engine) QueryExecutor {
	t.Helper()

	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": store,
		},
		defaultDB: "nornic",
	}
	s := &Session{
		server: &Server{
			dbManager: mgr,
		},
	}
	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)
	return exec
}

// startBoltTestServer pre-binds a loopback listener for the test server and
// runs the accept loop directly, avoiding racy reads of server.listener and the
// reserve-then-rebind TOCTOU window.
func startBoltTestServer(t *testing.T, server *Server) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	require.Greater(t, addr.Port, 0)
	server.listener = listener

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.serve()
	}()

	t.Cleanup(func() {
		require.NoError(t, server.Close())
		require.NoError(t, <-errCh)
	})
	return addr.Port
}

func transactionalDatabaseScopedExecutor(t *testing.T, store storage.Engine) QueryExecutor {
	t.Helper()

	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": store,
		},
		defaultDB: "nornic",
	}
	s := &Session{
		server: &Server{
			dbManager: mgr,
		},
	}
	exec, err := s.getTransactionalExecutorForDatabase("nornic")
	require.NoError(t, err)
	return exec
}

func databaseScopedNodeCount(t *testing.T, exec QueryExecutor, id string) int64 {
	t.Helper()

	res, err := exec.Execute(context.Background(),
		"MATCH (n:TxProbe {id: $id}) RETURN count(n) AS c",
		map[string]any{"id": id},
	)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	switch count := res.Rows[0][0].(type) {
	case int:
		return int64(count)
	case int64:
		return count
	case float64:
		return int64(count)
	default:
		t.Fatalf("count type = %T, want numeric", count)
	}
	return -1
}

type fixedEmbedder struct {
	dims int
}

func (f *fixedEmbedder) Embed(_ context.Context, _ string) ([]float32, error) {
	out := make([]float32, f.dims)
	if f.dims > 0 {
		out[0] = 1
	}
	return out, nil
}

func (f *fixedEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for range texts {
		v, _ := f.Embed(ctx, "")
		out = append(out, v)
	}
	return out, nil
}

func (f *fixedEmbedder) Dimensions() int { return f.dims }
func (f *fixedEmbedder) Model() string   { return "fixed" }
func (f *fixedEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06

func (f *fixedEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return []string{text}, nil
}

type providerBackedExecutor struct {
	base *cypher.StorageExecutor
}

func (p *providerBackedExecutor) Execute(_ context.Context, _ string, _ map[string]any) (*QueryResult, error) {
	return &QueryResult{}, nil
}

func (p *providerBackedExecutor) BaseCypherExecutor() *cypher.StorageExecutor {
	return p.base
}

func (p *providerBackedExecutor) ConfigureDatabaseExecutor(exec *cypher.StorageExecutor, _ string, _ storage.Engine) {
	if p == nil || p.base == nil || exec == nil {
		return
	}
	if emb := p.base.GetEmbedder(); emb != nil {
		exec.SetEmbedder(emb)
	}
}

func TestSessionGetExecutorForDatabase_InheritsEmbedder_ForStringVectorQuery(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "nornic")
	require.NoError(t, store.GetSchema().AddVectorIndex("idx_doc", "Doc", "embedding", 3, "cosine"))
	_, err := store.CreateNode(&storage.Node{
		ID:     "d1",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"embedding": []float32{1, 0, 0},
		},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
	})
	require.NoError(t, err)

	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": store,
		},
		defaultDB: "nornic",
	}

	baseExec := cypher.NewStorageExecutor(store)
	var emb embed.Embedder = &fixedEmbedder{dims: 3}
	baseExec.SetEmbedder(emb)

	s := &Session{
		server: &Server{
			dbManager: mgr,
			executor:  &boltQueryExecutorAdapter{executor: baseExec},
		},
	}

	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)

	// Ensure the DB-scoped Bolt executor inherits the base embedder so string
	// query inputs over Bolt are accepted.
	adapter, ok := exec.(*boltQueryExecutorAdapter)
	require.True(t, ok)
	require.NotNil(t, adapter.executor.GetEmbedder())

	res, err := exec.Execute(context.Background(),
		"CALL db.index.vector.queryNodes('idx_doc', 1, $q) YIELD node, score RETURN score",
		map[string]any{"q": "hello world"},
	)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Columns)
}

func TestSessionGetExecutorForDatabase_InheritsEmbedder_FromBaseExecutorProvider(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "nornic")
	require.NoError(t, store.GetSchema().AddVectorIndex("idx_doc", "Doc", "embedding", 3, "cosine"))
	_, err := store.CreateNode(&storage.Node{
		ID:     "d1",
		Labels: []string{"Doc"},
		Properties: map[string]interface{}{
			"embedding": []float32{1, 0, 0},
		},
		ChunkEmbeddings: [][]float32{{1, 0, 0}},
	})
	require.NoError(t, err)

	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": store,
		},
		defaultDB: "nornic",
	}

	baseExec := cypher.NewStorageExecutor(store)
	var emb embed.Embedder = &fixedEmbedder{dims: 3}
	baseExec.SetEmbedder(emb)

	s := &Session{
		server: &Server{
			dbManager: mgr,
			executor:  &providerBackedExecutor{base: baseExec},
		},
	}

	exec, err := s.getExecutorForDatabase("nornic")
	require.NoError(t, err)

	adapter, ok := exec.(*boltQueryExecutorAdapter)
	require.True(t, ok)
	require.NotNil(t, adapter.executor.GetEmbedder())

	res, err := exec.Execute(context.Background(),
		"CALL db.index.vector.queryNodes('idx_doc', 1, $q) YIELD node, score RETURN score",
		map[string]any{"q": "hello world"},
	)
	require.NoError(t, err)
	require.NotNil(t, res)
}
