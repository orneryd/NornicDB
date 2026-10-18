package cypher

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type typedCountScopeFixture struct {
	base    *storage.BadgerEngine
	engine  storage.Engine
	manager *multidb.DatabaseManager
	admin   *StorageExecutor
	closed  bool
}

func openTypedCountScope(t *testing.T, dir string, buffered bool) *typedCountScopeFixture {
	t.Helper()
	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: filepath.Join(dir, "data"), LowMemory: true,
		WriteBehind: buffered, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 100000,
	})
	require.NoError(t, err)
	wal, err := storage.NewWAL(filepath.Join(dir, "wal"), &storage.WALConfig{SyncMode: "none"})
	require.NoError(t, err)
	engine := storage.NewWALEngine(base, wal)
	manager, err := multidb.NewDatabaseManager(engine, nil)
	require.NoError(t, err)
	f := &typedCountScopeFixture{base: base, engine: engine, manager: manager}
	f.admin = f.executor(t, manager.DefaultDatabaseName())
	t.Cleanup(func() { f.close(t) })
	return f
}

func (f *typedCountScopeFixture) close(t *testing.T) {
	t.Helper()
	if !f.closed {
		require.NoError(t, f.base.FlushWriteBehind())
		require.NoError(t, f.manager.Close())
		f.closed = true
	}
}

func (f *typedCountScopeFixture) executor(t *testing.T, name string) *StorageExecutor {
	t.Helper()
	engine, err := f.manager.GetStorage(name)
	require.NoError(t, err)
	exec := NewStorageExecutor(engine)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: f.manager})
	return exec
}

func typedCountRun(t *testing.T, exec *StorageExecutor, query string) *ExecuteResult {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	return result
}

func typedCountWant(t *testing.T, exec *StorageExecutor, query string, want int64) {
	t.Helper()
	for i := 0; i < 2; i++ {
		require.Equal(t, [][]interface{}{{want}}, typedCountRun(t, exec, query).Rows, query)
	}
}

func typedCountShapes(t *testing.T, exec *StorageExecutor, want int64) {
	t.Helper()
	for _, query := range []string{
		"MATCH ()-[r:SCOPE]->() RETURN count(r) AS n",
		"MATCH (:ScopeStart)-[r:SCOPE]->() RETURN count(r) AS n",
		"MATCH ()-[r:SCOPE]->(:ScopeEnd) RETURN count(r) AS n",
		"MATCH ()<-[r:SCOPE]-(:ScopeStart) RETURN count(r) AS n",
		"MATCH (:ScopeEnd)<-[r:SCOPE]-() RETURN count(r) AS n",
		"MATCH ()-[r:SCOPE|ABSENT_SCOPE]->() RETURN count(r) AS n",
		"MATCH ()-[r]->() RETURN count(r) AS n",
		"MATCH (a:ScopeProbe) WITH a LIMIT 10 MATCH (a)-[r:SCOPE]->() RETURN count(r) AS n",
	} {
		typedCountWant(t, exec, query, want)
	}
	typedCountWant(t, exec, "MATCH ()-[r:ABSENT_SCOPE]->() RETURN count(r) AS n", 0)
}

func typedCountSeed(t *testing.T, exec *StorageExecutor, edges int) {
	t.Helper()
	typedCountRun(t, exec, "CREATE (:ScopeStart:ScopeProbe {key:'a'}), (:ScopeEnd:ScopeProbe {key:'b'})")
	for i := 0; i < edges; i++ {
		typedCountRun(t, exec, "MATCH (a:ScopeStart), (b:ScopeEnd) CREATE (a)-[:SCOPE]->(b)")
	}
}

func TestTypedCountNamespaceLifecycle(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			dir := t.TempDir()
			f := openTypedCountScope(t, dir, buffered)
			for _, name := range []string{"scope_a", "scope_b", "scope_c"} {
				typedCountRun(t, f.admin, "CREATE DATABASE "+name)
			}
			a, b, c := f.executor(t, "scope_a"), f.executor(t, "scope_b"), f.executor(t, "scope_c")
			typedCountSeed(t, a, 1)
			typedCountSeed(t, b, 2)
			typedCountSeed(t, c, 5)
			typedCountShapes(t, a, 1)
			typedCountShapes(t, b, 2)
			typedCountShapes(t, c, 5)
			typedCountWant(t, a, "MATCH (n:ScopeProbe) RETURN count(n) AS n", 2)

			// A decorator between the namespace and WAL must preserve counters too.
			traced := NewStorageExecutor(storage.NewNamespacedEngine(storage.NewTracedEngine(f.engine), "scope_a"))
			typedCountShapes(t, traced, 1)

			require.NoError(t, f.manager.CreateCompositeDatabase("scope_cmp", []multidb.ConstituentRef{
				{Alias: "a", DatabaseName: "scope_a", Type: "local", AccessMode: "read_write"},
				{Alias: "b", DatabaseName: "scope_b", Type: "local", AccessMode: "read_write"},
			}))
			cmp := f.executor(t, "scope_cmp")
			for _, shape := range []string{"()-[r:SCOPE]->()", "(:ScopeStart)-[r:SCOPE]->()", "()-[r:SCOPE]->(:ScopeEnd)"} {
				typedCountWant(t, cmp, "USE scope_cmp.a MATCH "+shape+" RETURN count(r) AS n", 1)
				typedCountWant(t, cmp, "CALL { USE scope_cmp.a MATCH "+shape+" RETURN count(r) AS n UNION ALL USE scope_cmp.b MATCH "+shape+" RETURN count(r) AS n } RETURN sum(n) AS n", 3)
			}

			typedCountRun(t, a, "BEGIN")
			typedCountRun(t, a, "MATCH (x:ScopeStart), (y:ScopeEnd) CREATE (x)-[:SCOPE]->(y)")
			typedCountShapes(t, a, 2)
			typedCountShapes(t, b, 2)
			typedCountRun(t, a, "MATCH ()-[r:SCOPE]->() DELETE r")
			typedCountShapes(t, a, 0)
			typedCountRun(t, a, "ROLLBACK")
			typedCountShapes(t, a, 1)
			typedCountRun(t, a, "BEGIN")
			typedCountRun(t, a, "MATCH (x:ScopeStart), (y:ScopeEnd) CREATE (x)-[:SCOPE]->(y)")
			typedCountRun(t, a, "COMMIT")
			typedCountShapes(t, a, 2)
			typedCountRun(t, a, "MATCH ()-[r:SCOPE]->() WITH r LIMIT 1 DELETE r")
			typedCountShapes(t, a, 1)
			typedCountRun(t, a, "MATCH (n:ScopeStart) REMOVE n:ScopeStart")
			typedCountWant(t, a, "MATCH (:ScopeStart)-[r:SCOPE]->() RETURN count(r) AS n", 0)
			typedCountWant(t, a, "MATCH ()-[r:SCOPE]->(:ScopeEnd) RETURN count(r) AS n", 1)
			typedCountRun(t, a, "MATCH (n:ScopeProbe {key:'a'}) SET n:ScopeStart")
			typedCountShapes(t, a, 1)
			typedCountRun(t, a, "MATCH (n:ScopeStart) DETACH DELETE n")
			typedCountShapes(t, a, 0)
			typedCountShapes(t, b, 2)
			typedCountRun(t, a, "MATCH (n) DETACH DELETE n")
			typedCountSeed(t, a, 1)
			f.close(t)
			f = openTypedCountScope(t, dir, buffered)
			for name, want := range map[string]int64{"scope_a": 1, "scope_b": 2, "scope_c": 5} {
				typedCountShapes(t, f.executor(t, name), want)
			}
		})
	}
}

// Inspect only primary key presence to prove that acknowledged creates are
// still pending. Product correctness assertions below all use public Cypher.
func typedCountPersistedNodes(base *storage.BadgerEngine, namespace string) int {
	txn := base.DB().NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	prefix := append([]byte{0x01}, []byte(namespace+":")...)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	count := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
	}
	return count
}

func TestTypedCountNamespaceDropPending(t *testing.T) {
	dir := t.TempDir()
	f := openTypedCountScope(t, dir, true)
	typedCountRun(t, f.admin, "CREATE DATABASE scope_drop")
	a := f.executor(t, "scope_drop")
	typedCountRun(t, a, "CREATE (:ScopeProbe)-[:SCOPE]->(:ScopeProbe)")
	require.Equal(t, 0, typedCountPersistedNodes(f.base, "scope_drop"), "create must still be acknowledged-only at drop")
	typedCountRun(t, f.admin, "DROP DATABASE scope_drop")
	typedCountRun(t, f.admin, "CREATE DATABASE scope_drop")
	a = f.executor(t, "scope_drop")
	typedCountWant(t, a, "MATCH (n:ScopeProbe) RETURN count(n) AS n", 0)
	typedCountWant(t, a, "MATCH (a:ScopeProbe) WITH a LIMIT 10 MATCH (a)-[r:SCOPE]->() RETURN count(r) AS n", 0)
	f.close(t)
	f = openTypedCountScope(t, dir, true)
	a = f.executor(t, "scope_drop")
	typedCountShapes(t, a, 0)
	typedCountSeed(t, a, 1)
	typedCountShapes(t, a, 1)
}
