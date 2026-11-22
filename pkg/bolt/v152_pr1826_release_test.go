package bolt

// v1.5.2 verification (scratch, test-only): R1 #7 in the Bolt result
// reservation: a numeric list counts 16 bytes whatever its length.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestV152P26ResultReservation(t *testing.T) {
	ctx := context.Background()
	engine, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	store := storage.NewNamespacedEngine(engine, "nornic")
	const nodes, size = 16, 8 << 20
	for i := 0; i < nodes; i++ {
		list := make([]float64, size/8)
		for j := range list {
			list[j] = float64(i+j) + 0.25
		}
		_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("f%d", i)), Labels: []string{"Floats"}, Properties: map[string]any{"v": list}})
		require.NoError(t, err)
		_, err = store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("s%d", i)), Labels: []string{"Strings"}, Properties: map[string]any{"v": strings.Repeat(fmt.Sprintf("%08d", i), size/8)}})
		require.NoError(t, err)
	}
	const limit = 64 << 20
	policy := memory.NewPolicy(limit, 0, time.Hour, func() (memory.Sample, error) {
		return memory.Sample{Used: 1 << 20, Ceiling: 4 << 30}, nil
	}, time.Now)
	require.NoError(t, policy.Refresh())
	server := New(&Config{Port: 0, MaxConnections: 4, ReadBufferSize: 8192, WriteBufferSize: 8192}, &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store, cypher.QueryCachePolicy{MaxEntries: 0})})
	server.SetMemoryPolicy(policy)
	port := serveBoltIntegrationServer(t, server)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	for _, label := range []string{"Strings", "Floats"} {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{FetchSize: 1})
		result, err := session.Run(ctx, "MATCH (n:"+label+") RETURN n.v", nil)
		reserved := policy.Reserved()
		outcome := "admitted"
		if err != nil {
			outcome = "refused: " + err.Error()
		} else if _, err = result.Consume(ctx); err != nil {
			outcome = "admitted, then " + err.Error()
		}
		_ = session.Close(ctx)
		t.Logf("P26ROW R7B %s: %d rows of %d MiB against a %d MiB limit: %s; reserved while open %d bytes", label, nodes, size>>20, limit>>20, outcome, reserved)
		if !strings.HasPrefix(outcome, "refused") && !strings.Contains(outcome, "MemoryPool") {
			t.Errorf("P26 R7B %s: a result of %d MiB against a %d MiB limit is %s, reserving %d bytes", label, nodes*size>>20, limit>>20, outcome, reserved)
		}
	}
}

// F3 on the reference: what the pinned Neo4j does with the same two sessions.
// It asserts nothing; it records the answers.
func TestV152P26ReferenceOlderTransaction(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("set NORNICDB_NEO4J_REFERENCE_URI")
	}
	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	other, older := driver.NewSession(ctx, neo4jdriver.SessionConfig{}), driver.NewSession(ctx, neo4jdriver.SessionConfig{})
	defer other.Close(ctx)
	defer older.Close(ctx)
	must := func(q string) {
		result, err := other.Run(ctx, q, nil)
		require.NoError(t, err, q)
		_, err = result.Consume(ctx)
		require.NoError(t, err, q)
	}
	result, err := other.Run(ctx, "CALL dbms.components() YIELD versions RETURN versions", nil)
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	t.Logf("P26ROW F3REF version %v", record.Values)
	must("CREATE CONSTRAINT p26_key IF NOT EXISTS FOR (n:P26Doc) REQUIRE n.key IS UNIQUE")
	must("CREATE INDEX p26_rank IF NOT EXISTS FOR (n:P26Doc) ON (n.rank)")
	must("CALL db.awaitIndexes(60)")
	must("MATCH (n:P26Doc) DETACH DELETE n")
	must("UNWIND range(0,9) AS i CREATE (:P26Doc {key:'k'+toString(i), rank:i, body:'b'+toString(i)})")
	tx, err := older.BeginTransaction(ctx)
	require.NoError(t, err)
	statements := []string{"MATCH (n:P26Doc {key:'k1'}) RETURN n.body", "MATCH (n:P26Doc) WHERE toString(n.key) = 'k1' RETURN n.body", "MATCH (n:P26Doc {rank:2}) RETURN n.body", "MATCH (n:P26Doc) WHERE n.rank + 0 = 2 RETURN n.body", "MATCH (n:P26Doc) RETURN count(n)"}
	read := func(stage string) {
		for _, q := range statements {
			result, err := tx.Run(ctx, q, nil)
			require.NoError(t, err, q)
			records, err := result.Collect(ctx)
			require.NoError(t, err, q)
			var rows []any
			for _, r := range records {
				rows = append(rows, r.Values...)
			}
			t.Logf("P26ROW F3REF %s %q -> %v", stage, q, rows)
		}
	}
	read("before")
	must("MATCH (n:P26Doc {key:'k1'}) DETACH DELETE n")
	must("MATCH (n:P26Doc {key:'k2'}) SET n.rank = 77")
	read("after")
	require.NoError(t, tx.Rollback(ctx))
	must("MATCH (n:P26Doc) DETACH DELETE n")
	must("DROP CONSTRAINT p26_key")
	must("DROP INDEX p26_rank")
}
