package bolt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func s1BoltSeed(t *testing.T, store storage.Engine, owner string) {
	t.Helper()
	ctx := context.Background()
	exec := cypher.NewStorageExecutor(store, cypher.QueryCachePolicy{MaxEntries: 0})
	for _, ddl := range []string{"CREATE CONSTRAINT s1_key FOR (n:Doc) REQUIRE n.key IS UNIQUE", "CREATE INDEX s1_rank FOR (n:Doc) ON (n.rank)"} {
		_, err := exec.Execute(ctx, ddl, nil)
		require.NoError(t, err)
	}
	for i := 0; i < 12; i++ {
		_, err := store.CreateNode(ctx, &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{
			"key": fmt.Sprintf("k%d", i), "owner": owner, "rank": int64(i), "secret": owner + "-secret-" + fmt.Sprint(i), "body": strings.Repeat(owner, 2000), "bytes": []byte{byte(i), 255}, "list": []any{"a", owner},
		}})
		require.NoError(t, err)
		if i > 0 {
			require.NoError(t, store.CreateEdge(ctx, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%d", i)), StartNode: storage.NodeID(fmt.Sprintf("n%d", i-1)), EndNode: storage.NodeID(fmt.Sprintf("n%d", i)), Type: "NEXT", Properties: map[string]any{}}))
		}
	}
	_, err := exec.Execute(ctx, "MATCH (n:Doc) SET n.d=date('2026-01-02'), n.dt=datetime('2026-11-22T10:11:12+02:00'), n.p=point({x:1.0,y:2.0}), n.dur=duration('P1DT2H')", nil)
	require.NoError(t, err)
}

// s1BoltQueries are parameterised by key; every one names only properties,
// except the last few, which return whole entities.
var s1BoltQueries = []string{
	"MATCH (n:Doc {key:$k}) RETURN n.owner, n.rank", "MATCH (n:Doc {key:$k}) RETURN n.secret", "MATCH (n:Doc {key:$k}) RETURN n.body, n.missing", "MATCH (n:Doc) WHERE n.rank >= 3 AND n.owner = $owner RETURN n.key, n.secret ORDER BY n.rank", "MATCH (n:Doc) RETURN count(n.secret), collect(DISTINCT n.owner)", "UNWIND ['k1','k2','k1'] AS k MATCH (n:Doc {key:k}) RETURN n.key, n.owner, n.list",
	"MATCH (n:Doc {key:$k}) RETURN n.bytes, n.d, n.dt, n.p, n.dur", "MATCH (n:Doc {key:$k}) RETURN n{.key,.owner}", "MATCH (n:Doc {key:$k}) RETURN n{.*}", "MATCH (n:Doc {key:$k}) RETURN properties(n)", "MATCH (n:Doc {key:$k}) RETURN keys(n)", "MATCH (n:Doc {key:$k})-[:NEXT]->(m) RETURN n.owner, m.secret", "OPTIONAL MATCH (n:Doc {key:'nope'}) RETURN n.owner", "MATCH (n:Doc {key:$k}) CALL (n) { RETURN n.secret AS s } RETURN s", "MATCH (n:Doc {key:$k}) WITH n AS q WHERE q.rank > 0 RETURN q.secret", "MATCH (n:Doc) WITH n.owner AS o, max(n.rank) AS r RETURN o, r", "MATCH (n:Doc {key:$k}) RETURN n.owner UNION MATCH (n:Doc {key:'k5'}) RETURN n.owner", "MATCH (n:Doc) WHERE n.key STARTS WITH 'k1' RETURN n.key, n.owner ORDER BY n.key",
	"MATCH (n:Doc {key:$k}) RETURN n", "MATCH (n:Doc {key:$k}) RETURN n.owner, n", "MATCH (n:Doc {key:$k})-[r:NEXT]->(m) RETURN n, r, m",
}

func s1BoltRun(ctx context.Context, run func(string, map[string]any) (neo4jdriver.ResultWithContext, error), query, key, owner string) string {
	result, err := run(query, map[string]any{"k": key, "owner": owner})
	if err != nil {
		return "ERR " + err.Error()
	}
	var rows []string
	for result.Next(ctx) {
		encoded, err := json.Marshal(result.Record().Values)
		if err != nil {
			return "ERR marshal " + err.Error()
		}
		rows = append(rows, string(encoded))
	}
	if err := result.Err(); err != nil {
		return "ERR " + err.Error()
	}
	if !strings.Contains(query, "ORDER BY") {
		sort.Strings(rows)
	}
	return strings.Join(rows, "\n")
}

// s1BoltFull hides projection from the executor (the oracle).
type s1BoltFull struct{ storage.Engine }

func (f s1BoltFull) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, fn func(*storage.Node) error) error {
	opts.Projection, opts.PropertyFilterProjection, opts.PropertyFilter, opts.PropertyStringEquals = nil, nil, nil, nil
	return f.Engine.StreamNodesWithOptions(ctx, opts, fn)
}

func s1BoltSession(t *testing.T, port int, database string) neo4jdriver.SessionWithContext {
	t.Helper()
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	session := driver.NewSession(context.Background(), neo4jdriver.SessionConfig{DatabaseName: database})
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	return session
}

// TestPR1826S1BoltDifferential: the same statements over Bolt, auto-commit and
// explicit, against a server whose store cannot project.
func TestPR1826S1BoltDifferential(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(base, "nornic")
	s1BoltSeed(t, store, "alice")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	_, oraclePort := startBoltIntegrationServer(t, s1BoltFull{store}, boltIntegrationAutocommit)
	session, oracle := s1BoltSession(t, port, ""), s1BoltSession(t, oraclePort, "")
	auto := func(s neo4jdriver.SessionWithContext) func(string, map[string]any) (neo4jdriver.ResultWithContext, error) {
		return func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return s.Run(ctx, q, p) }
	}
	digest := sha256.New()
	for round := 0; round < 3; round++ {
		for _, key := range []string{"k1", "k4"} {
			for _, q := range s1BoltQueries {
				want := s1BoltRun(ctx, auto(oracle), q, key, "alice")
				got := s1BoltRun(ctx, auto(session), q, key, "alice")
				fmt.Fprintln(digest, q, got)
				if got != want {
					t.Errorf("S1MISMATCH bolt auto %s %s\nwant %.500s\ngot  %.500s", key, q, want, got)
				}
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				got = s1BoltRun(ctx, func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return tx.Run(ctx, q, p) }, q, key, "alice")
				_ = tx.Rollback(ctx)
				if got != want {
					t.Errorf("S1MISMATCH bolt explicit %s %s\nwant %.500s\ngot  %.500s", key, q, want, got)
				}
			}
		}
	}
	t.Logf("S1ROW bolt %s", hex.EncodeToString(digest.Sum(nil)[:8]))
}

// TestPR1826S1BoltTransactions: pending writes over Bolt, another session, a
// rollback, a commit, and an older snapshot.
func TestPR1826S1BoltTransactions(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	store := storage.NewNamespacedEngine(base, "nornic")
	s1BoltSeed(t, store, "alice")
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	one, two, three := s1BoltSession(t, port, ""), s1BoltSession(t, port, ""), s1BoltSession(t, port, "")
	read := func(run func(string, map[string]any) (neo4jdriver.ResultWithContext, error), q string) string {
		return s1BoltRun(ctx, run, q, "k1", "alice")
	}
	autoTwo := func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return two.Run(ctx, q, p) }
	narrow, wide := "MATCH (n:Doc {key:$k}) RETURN n.owner, n.rank", "MATCH (n:Doc {key:$k}) RETURN n.owner, n.rank, n.secret, n.added"
	require.Equal(t, `["alice",1]`, read(autoTwo, narrow))
	snapshot, err := three.BeginTransaction(ctx)
	require.NoError(t, err)
	inSnapshot := func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) {
		return snapshot.Run(ctx, q, p)
	}
	require.Equal(t, `["alice",1]`, read(inSnapshot, narrow))
	for _, commit := range []bool{false, true} {
		tx, err := one.BeginTransaction(ctx)
		require.NoError(t, err)
		inTx := func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return tx.Run(ctx, q, p) }
		require.Equal(t, `["alice",1]`, read(inTx, narrow))
		for _, w := range []string{"MATCH (n:Doc {key:'k1'}) SET n.owner='pending', n.added='x' REMOVE n.secret", "MATCH (n:Doc {key:'k2'}) DETACH DELETE n", "MATCH (n:Doc {key:'k3'}) REMOVE n:Doc SET n:Gone"} {
			result, err := tx.Run(ctx, w, nil)
			require.NoError(t, err)
			_, err = result.Consume(ctx)
			require.NoError(t, err)
		}
		require.Equal(t, `["pending",1]`, read(inTx, narrow))
		require.Equal(t, `["pending",1,null,"x"]`, read(inTx, wide))
		require.Equal(t, "", s1BoltRun(ctx, inTx, narrow, "k2", ""))
		require.Equal(t, "", s1BoltRun(ctx, inTx, narrow, "k3", ""))
		require.Equal(t, `["alice",1]`, read(autoTwo, narrow), "another session does not see pending writes")
		require.Equal(t, `["alice",1,"alice-secret-1",null]`, read(autoTwo, wide))
		require.Equal(t, `["alice",2]`, s1BoltRun(ctx, autoTwo, narrow, "k2", ""))
		require.Equal(t, `["alice",3]`, s1BoltRun(ctx, autoTwo, narrow, "k3", ""))
		if commit {
			require.NoError(t, tx.Commit(ctx))
			require.Equal(t, `["pending",1]`, read(autoTwo, narrow))
			require.Equal(t, `["pending",1,null,"x"]`, read(autoTwo, wide))
			require.Equal(t, "", s1BoltRun(ctx, autoTwo, narrow, "k2", ""))
			require.Equal(t, "", s1BoltRun(ctx, autoTwo, narrow, "k3", ""))
		} else {
			require.NoError(t, tx.Rollback(ctx))
			require.Equal(t, `["alice",1,"alice-secret-1",null]`, read(autoTwo, wide))
		}
		require.Equal(t, `["alice",1]`, read(inSnapshot, narrow), "the older snapshot keeps its values")
		require.Equal(t, `["alice",1,"alice-secret-1",null]`, read(inSnapshot, wide))
		if got := s1BoltRun(ctx, inSnapshot, narrow, "k2", ""); got != `["alice",2]` {
			t.Errorf("S1MISMATCH bolt snapshot commit=%v: a node deleted after the snapshot began, read through its unique key: want [alice,2] got %q", commit, got)
		}
		t.Logf("S1SNAP commit=%v scan=%q byRank=%q", commit, s1BoltRun(ctx, inSnapshot, "MATCH (n:Doc) WHERE n.owner = $owner RETURN count(n)", "", "alice"), s1BoltRun(ctx, inSnapshot, "MATCH (n:Doc) WHERE n.rank = 2 RETURN n.owner", "", ""))
	}
	require.NoError(t, snapshot.Commit(ctx))
	require.Equal(t, `["pending",1,null,"x"]`, s1BoltRun(ctx, func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return three.Run(ctx, q, p) }, wide, "k1", ""))
}

// TestPR1826S1BoltManagedAndComposite: two managed databases with the same node
// ids and keys, read alternately and through a composite database, never serve
// each other's images; dropping and re-creating one serves only new values.
func TestPR1826S1BoltManagedAndComposite(t *testing.T) {
	ctx := context.Background()
	manager, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	for _, name := range []string{"dba", "dbb"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
		store, err := manager.GetStorage(name)
		require.NoError(t, err)
		s1BoltSeed(t, store, name)
	}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{{Alias: "a", DatabaseName: "dba", Type: "local", AccessMode: "read_write"}, {Alias: "b", DatabaseName: "dbb", Type: "local", AccessMode: "read_write"}}))
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 32, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, manager)
	t.Cleanup(func() { _ = server.Close() })
	port := serveBoltIntegrationServer(t, server)
	digest := sha256.New()
	compare := func(stage string) {
		for round := 0; round < 3; round++ {
			for _, name := range []string{"dba", "dbb"} {
				session := s1BoltSession(t, port, name)
				run := func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return session.Run(ctx, q, p) }
				store, err := manager.GetStorage(name)
				require.NoError(t, err)
				other := map[string]string{"dba": "dbb", "dbb": "dba"}[name]
				for _, q := range s1BoltQueries {
					got := s1BoltRun(ctx, run, q, "k4", name)
					fmt.Fprintln(digest, stage, name, q, got)
					if strings.Contains(strings.ReplaceAll(got, "4:"+other, ""), other) {
						t.Errorf("S1MISMATCH %s: %s shows %s's data: %s\n%.400s", stage, name, other, q, got)
					}
					if strings.HasPrefix(got, "ERR") {
						t.Errorf("S1MISMATCH %s %s %s: %s", stage, name, q, got)
					}
				}
				node, err := store.GetNode(ctx, "n4")
				require.NoError(t, err)
				want, _ := json.Marshal([]any{node.Properties["owner"], node.Properties["rank"]})
				require.Equal(t, string(want), s1BoltRun(ctx, run, s1BoltQueries[0], "k4", name), stage)
			}
			comp := s1BoltSession(t, port, "comp")
			run := func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return comp.Run(ctx, q, p) }
			for _, c := range []struct{ q string }{
				{"CALL { USE comp.a MATCH (n:Doc {key:$k}) RETURN n.owner AS o, n.secret AS s UNION ALL USE comp.b MATCH (n:Doc {key:$k}) RETURN n.owner AS o, n.secret AS s } RETURN o, s"},
				{"CALL { USE comp.a MATCH (n:Doc {key:$k}) RETURN n } RETURN n.owner, n.secret"}, {"CALL { USE comp.b MATCH (n:Doc) WHERE n.rank = 4 RETURN n } RETURN n.owner, n.body"}, {"USE comp.a MATCH (n:Doc {key:$k}) RETURN n.owner, n.rank"}, {"USE comp.b MATCH (n:Doc {key:$k}) RETURN n.owner, n.rank"}, {"CALL { USE comp.b MATCH (n:Doc {key:$k}) RETURN n } RETURN n{.owner,.rank}, keys(n)"},
			} {
				got := s1BoltRun(ctx, run, c.q, "k4", "")
				fmt.Fprintln(digest, stage, c.q, got)
				t.Logf("S1COMP %s %q -> %.200s", stage, c.q, got)
				if strings.Contains(c.q, "comp.a") && !strings.Contains(c.q, "comp.b") && strings.Contains(got, "dbb") || strings.Contains(c.q, "comp.b") && !strings.Contains(c.q, "comp.a") && strings.Contains(got, "dba") {
					t.Errorf("S1MISMATCH composite %s crosses databases: %s\n%s", stage, c.q, got)
				}
			}
		}
	}
	compare("first")
	require.NoError(t, manager.DropCompositeDatabase(t.Context(), "comp"))
	require.NoError(t, manager.DropDatabase(t.Context(), "dba"))
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "dba"}))
	store, err := manager.GetStorage("dba")
	require.NoError(t, err)
	session := s1BoltSession(t, port, "dba")
	require.Equal(t, "", s1BoltRun(ctx, func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return session.Run(ctx, q, p) }, s1BoltQueries[0], "k4", "dba"), "a re-created database is empty")
	s1BoltSeed(t, store, "dba")
	exec := cypher.NewStorageExecutor(store, cypher.QueryCachePolicy{MaxEntries: 0})
	_, err = exec.Execute(ctx, "MATCH (n:Doc) SET n.rank = n.rank + 100, n.secret = 'dba-second'", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:Doc) SET n.rank = n.rank - 100", nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{{Alias: "a", DatabaseName: "dba", Type: "local", AccessMode: "read_write"}, {Alias: "b", DatabaseName: "dbb", Type: "local", AccessMode: "read_write"}}))
	session = s1BoltSession(t, port, "dba")
	require.Equal(t, `["dba-second"]`, s1BoltRun(ctx, func(q string, p map[string]any) (neo4jdriver.ResultWithContext, error) { return session.Run(ctx, q, p) }, s1BoltQueries[1], "k4", "dba"))
	compare("recreated")
	t.Logf("S1ROW managed %s", hex.EncodeToString(digest.Sum(nil)[:8]))
}
