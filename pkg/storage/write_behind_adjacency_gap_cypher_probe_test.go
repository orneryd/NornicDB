package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestWriteBehindAdjacencyGapPublicCypherProbe(t *testing.T) {
	b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir:t.TempDir(), LowMemory:true, WriteBehind:true, WriteBehindInterval:time.Hour, WriteBehindMaxOps:100000})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.FlushWriteBehind()); require.NoError(t, b.Close()) })
	exec := cypher.NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(b, "probe"), 0, 0)
	ctx := context.Background()
	run := func(query string) *cypher.ExecuteResult { result, err := exec.Execute(ctx, query, nil); require.NoError(t, err); return result }
	run("CREATE (:ScopeStart:ScopeProbe {key:'a'}), (:ScopeEnd {key:'b'})")
	require.NoError(t, b.FlushWriteBehind())
	b.GapProbeStopWorker()
	run("MATCH (a:ScopeStart), (b:ScopeEnd) CREATE (a)-[:SCOPE]->(b)")
	require.Equal(t, [][]interface{}{{int64(1)}}, run("MATCH ()-[r:SCOPE]->() RETURN count(r) AS n").Rows)
	const query = "MATCH (a:ScopeProbe) WITH a LIMIT 10 MATCH (a)-[r:SCOPE]->() RETURN count(r) AS n"
	first := b.GapProbeAcrossReplay(t, func() (int64, error) { result, err := exec.Execute(ctx, query, nil); if err != nil { return -1, err }; return result.Rows[0][0].(int64), nil })
	later := run(query).Rows
	t.Logf("first bounded MATCH=%d later bounded MATCH=%v; fresh namespace, no DROP, cache disabled", first, later)
	require.Equal(t, [][]interface{}{{int64(1)}}, later)
	require.Equal(t, int64(1), first, "a replay boundary must not hide an acknowledged relationship")
}
