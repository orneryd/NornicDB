package cypher

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func quotaExecutor(t testing.TB, limits *multidb.Limits) (*StorageExecutor, storage.Engine, *multidb.DatabaseManager) {
	t.Helper()
	base := newTestMemoryEngine(t)
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("quota"))
	require.NoError(t, manager.SetDatabaseLimits("quota", limits))
	engine, err := manager.GetStorage("quota")
	require.NoError(t, err)
	return NewStorageExecutor(engine), engine, manager
}

func BenchmarkTransactionQuotaCommit(b *testing.B) {
	for _, limited := range []bool{false, true} {
		name := "unlimited"
		var limits *multidb.Limits
		if limited {
			name = "node_limit"
			limits = &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1 << 60}}
		}
		b.Run(name, func(b *testing.B) {
			exec, _, _ := quotaExecutor(b, limits)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(ctx, "CREATE (:Quota {value: $value})", map[string]interface{}{"value": i})
				require.NoError(b, err)
			}
		})
	}
}

func TestTransactionQuotas(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			limits multidb.Limits
			setup  string
			query  string
			count  int64
		}{
			{name: "nodes", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}, setup: "CREATE (:Quota)", query: "CREATE (:Quota)", count: 1},
			{name: "bytes", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: 1}}, query: "CREATE (:Quota {payload: 'too large'})"},
			{name: "edges", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxEdges: 1}}, setup: "CREATE (a:Quota)-[:R]->(b:Quota)", query: "MATCH (a:Quota), (b:Quota) CREATE (a)-[:R]->(b)", count: 2},
			{name: "batch", limits: multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}, query: "UNWIND [1, 2] AS id CREATE (:Quota {id: id})"},
			{name: "write rate", limits: multidb.Limits{Rate: multidb.RateLimits{MaxWritesPerSecond: 1}}, query: "UNWIND [1, 2] AS id CREATE (:Quota {id: id})"},
		} {
			mode := "implicit/"
			if explicit {
				mode = "explicit/"
			}
			t.Run(mode+tc.name, func(t *testing.T) {
				exec, engine, _ := quotaExecutor(t, &tc.limits)
				run := func(query string) error { _, err := exec.Execute(context.Background(), query, nil); return err }
				if tc.setup != "" {
					require.NoError(t, run(tc.setup))
				}
				if explicit {
					require.NoError(t, run("BEGIN"))
				}
				err := run(tc.query)
				if explicit && err == nil {
					err = run("COMMIT")
				}
				require.Error(t, err)
				count, err := engine.NodeCount()
				require.NoError(t, err)
				require.Equal(t, tc.count, count, "rejected transaction must persist no partial writes")
				if tc.name == "edges" {
					edges, err := engine.EdgeCount()
					require.NoError(t, err)
					require.EqualValues(t, 1, edges)
				}
			})
		}
	}
}

func TestTransactionQuotasConcurrentCommit(t *testing.T) {
	_, engine, _ := quotaExecutor(t, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}})
	executors := []*StorageExecutor{NewStorageExecutor(engine), NewStorageExecutor(engine)}
	for _, exec := range executors {
		_, err := exec.Execute(context.Background(), "BEGIN", nil)
		require.NoError(t, err)
		_, err = exec.Execute(context.Background(), "CREATE (:Quota)", nil)
		require.NoError(t, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, exec := range executors {
		wg.Add(1)
		go func(exec *StorageExecutor) {
			defer wg.Done()
			_, err := exec.Execute(context.Background(), "COMMIT", nil)
			errs <- err
		}(exec)
	}
	wg.Wait()
	close(errs)
	accepted := 0
	for err := range errs {
		if err == nil {
			accepted++
		}
	}
	require.Equal(t, 1, accepted)
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestTransactionQuotasByteAccounting(t *testing.T) {
	exec, engine, manager := quotaExecutor(t, nil)
	run := func(query string) error { _, err := exec.Execute(context.Background(), query, nil); return err }
	// Initialize the cache before any transactional write.
	initial, _, _ := manager.GetStorageSize("quota")
	require.Zero(t, initial)
	require.NoError(t, run("CREATE (a:Quota {id: 1})-[:R {payload: 'small'}]->(b:Quota {id: 2})"))
	created, _, _ := manager.GetStorageSize("quota")
	require.Positive(t, created)
	require.NoError(t, manager.SetDatabaseLimits("quota", &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: created}}))
	for _, query := range []string{
		"MATCH (n:Quota {id: 1}) SET n.payload = '" + strings.Repeat("x", 2048) + "'",
		"MATCH (:Quota)-[r:R]->(:Quota) SET r.payload = '" + strings.Repeat("x", 2048) + "'",
	} {
		require.Error(t, run(query))
		size, _, _ := manager.GetStorageSize("quota")
		require.Equal(t, created, size)
	}
	// Neither rollback nor a rejected commit consumes storage capacity.
	require.NoError(t, run("BEGIN"))
	require.NoError(t, run("MATCH (n:Quota) DETACH DELETE n"))
	require.NoError(t, run("ROLLBACK"))
	size, _, _ := manager.GetStorageSize("quota")
	require.Equal(t, created, size)
	require.NoError(t, run("MATCH (n:Quota) DETACH DELETE n"))
	size, _, _ = manager.GetStorageSize("quota")
	require.Zero(t, size, "cascade deletes must release node and edge bytes exactly once")
	require.NoError(t, run("CREATE (:Quota {id: 3})"))
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestTransactionQuotasPersistedUsage(t *testing.T) {
	dir := t.TempDir()
	open := func() (*storage.BadgerEngine, *multidb.DatabaseManager) {
		t.Helper()
		base, err := storage.NewBadgerEngine(dir)
		require.NoError(t, err)
		manager, err := multidb.NewDatabaseManager(base, nil)
		require.NoError(t, err)
		return base, manager
	}
	base, manager := open()
	require.NoError(t, manager.CreateDatabase("quota"))
	require.NoError(t, manager.SetDatabaseLimits("quota", &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}))
	engine, err := manager.GetStorage("quota")
	require.NoError(t, err)
	_, err = NewStorageExecutor(engine).Execute(context.Background(), "CREATE (:Quota)", nil)
	require.NoError(t, err)
	require.NoError(t, manager.Close())
	require.NoError(t, base.Close())
	base, manager = open()
	t.Cleanup(func() { _ = manager.Close(); _ = base.Close() })
	engine, err = manager.GetStorage("quota")
	require.NoError(t, err)
	_, err = NewStorageExecutor(engine).Execute(context.Background(), "CREATE (:Quota)", nil)
	require.ErrorIs(t, err, multidb.ErrStorageLimitExceeded)
	count, err := engine.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}
