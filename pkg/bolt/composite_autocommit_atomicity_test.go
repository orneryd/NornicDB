package bolt

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// compositeAtomicityShapes write to the local constituent accmp.one, with a
// write clause or a WRITE procedure, and then fail in a later part of the
// same statement when $d is 0 (succeed when it is 1). {fail} is the
// constituent the failing part runs on; it imports no variable, so a remote
// constituent fails at run time. reference is the statement Neo4j runs when
// it differs from the statement without its USE selectors.
var compositeAtomicityShapes = []struct {
	name, query, reference string
	writes                 int64
}{
	{"call_then_error", "CALL { USE accmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z", "", 1},
	{"union_branch_error", "CALL { USE accmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS z UNION ALL USE {fail} RETURN 1 / $d AS z } RETURN z", "", 1},
	{"nested_call_error", "CALL { USE accmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { CALL { USE {fail} RETURN 1 / $d AS z } RETURN z } RETURN w, z", "", 1},
	{"second_write_error", "CALL { USE accmp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE accmp.one CREATE (:AcAtom {run: $run, v: w / $d}) RETURN 1 AS z } RETURN z", "", 2},
	{"procedure_write_then_error", "CALL { USE accmp.one CALL acatom.write($run) YIELD w RETURN w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z",
		"CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z", 1},
	{"dynamic_write_then_error", "CALL { USE accmp.one CALL apoc.cypher.doIt('CREATE (:AcAtom {run: $run}) RETURN 1 AS w', {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z",
		"CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z", 1},
	{"read_call_write_then_error", "CALL { USE accmp.one CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z",
		"CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z", 1},
	{"doitall_write_then_error", "CALL { USE accmp.one CALL apoc.cypher.doitall($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z",
		"CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z", 1},
}

// compositeAtomicityBody is the writing body the dynamic procedures run.
const compositeAtomicityBody = "CREATE (:AcAtom {run: $run}) RETURN 1 AS w"

// compositeSecondWriteOrders write to accmp.one through a procedure (a WRITE
// procedure, or a reading procedure whose dynamic body writes) and to
// accmp.two through a write clause, in both orders. A statement writes to
// one database: each is refused, and leaves nothing on either.
func compositeSecondWriteOrders() []struct{ name, query string } {
	orders := []struct{ name, query string }{
		{"procedure_first", "USE accmp CALL { USE accmp.one CALL acatom.write($run) YIELD w RETURN w } CALL { WITH w USE accmp.two CREATE (:AcAtom {run: $run}) RETURN w AS z } RETURN z"},
		{"procedure_second", "USE accmp CALL { USE accmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE accmp.one CALL acatom.write($run) YIELD w AS z RETURN z } RETURN w, z"},
	}
	for _, procedure := range []string{"apoc.cypher.run", "apoc.cypher.doitall"} {
		call := "CALL " + procedure + "($body, {run: $run}) YIELD value"
		orders = append(orders,
			struct{ name, query string }{procedure + "_first", "USE accmp CALL { USE accmp.one " + call + " RETURN value.w AS w } CALL { WITH w USE accmp.two CREATE (:AcAtom {run: $run}) RETURN w AS z } RETURN z"},
			struct{ name, query string }{procedure + "_second", "USE accmp CALL { USE accmp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE accmp.one " + call + " RETURN value.w AS z } RETURN w, z"},
		)
	}
	return orders
}

// A composite statement sent over Bolt, outside or inside an explicit
// transaction, commits its local writes only when the whole statement
// succeeds, as Neo4j 5.26.30 commits a statement (compared on neo-ref). The
// failure is a runtime error on the local constituent or on a Neo4j
// constituent reached over Bolt or HTTP, after the local write. The explicit
// transaction is checked with the local failure. A procedure's write, also
// one found only when its dynamic body runs, makes accmp.one the statement's
// one write database (compositeSecondWriteOrders).
func TestBoltCompositeAutocommitStatementWritesAreAtomic(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	httpURI := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if uri == "" || httpURI == "" {
		t.Skip("requires --ref neo-ref")
	}
	ctx := context.Background()
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: authToken})
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateDatabase("acone"))
	require.NoError(t, mgr.CreateDatabase("actwo"))
	require.NoError(t, mgr.CreateCompositeDatabase("accmp", []multidb.ConstituentRef{
		{Alias: "one", DatabaseName: "acone", Type: "local", AccessMode: "read_write"},
		{Alias: "two", DatabaseName: "actwo", Type: "local", AccessMode: "read_write"},
		{Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri},
		{Alias: "http", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: httpURI},
	}))
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16, ReadBufferSize: 8192, WriteBufferSize: 8192}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	reference, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reference.Close(context.Background()) })
	// Procedure DDL runs outside a transaction: an autocommit session run.
	autocommit := func(ctx context.Context, statement string) error {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "acone"})
		defer func() { _ = session.Close(ctx) }()
		result, err := session.Run(ctx, statement, nil)
		if err == nil {
			_, err = result.Consume(ctx)
		}
		return err
	}
	require.NoError(t, autocommit(ctx, "CREATE OR REPLACE PROCEDURE acatom.write($run) MODE WRITE AS CREATE (:AcAtom {run: $run}) RETURN 1 AS w"))
	t.Cleanup(func() { _ = autocommit(context.Background(), "DROP PROCEDURE acatom.write") })

	writes := func(constituent, run string) int64 {
		result, err := neo4jdriver.ExecuteQuery(ctx, driver, "USE "+constituent+" MATCH (n:AcAtom {run: $run}) RETURN count(n) AS c", map[string]any{"run": run}, neo4jdriver.EagerResultTransformer, neo4jdriver.ExecuteQueryWithDatabase("accmp"))
		require.NoError(t, err)
		return result.Records[0].Values[0].(int64)
	}
	referenceWrites := func(statement string, params map[string]any) (bool, int64) {
		session := reference.NewSession(ctx, neo4jdriver.SessionConfig{})
		defer func() { _ = session.Close(ctx) }()
		result, err := session.Run(ctx, statement, params)
		if err == nil {
			_, err = result.Consume(ctx)
		}
		count, countErr := neo4jdriver.ExecuteQuery(ctx, reference, "MATCH (n:AcAtom {run: $run}) WITH count(n) AS c, collect(n) AS ns FOREACH (n IN ns | DELETE n) RETURN c", map[string]any{"run": params["run"]}, neo4jdriver.EagerResultTransformer)
		require.NoError(t, countErr)
		return err != nil, count.Records[0].Values[0].(int64)
	}

	for _, shape := range compositeAtomicityShapes {
		for _, fail := range []string{"accmp.one", "accmp.remote", "accmp.http"} {
			for _, explicit := range []bool{false, true} {
				if explicit && fail != "accmp.one" {
					continue
				}
				for _, d := range []int64{0, 1} {
					t.Run(fmt.Sprintf("%s/%s/explicit=%v/d=%d", shape.name, fail, explicit, d), func(t *testing.T) {
						run := fmt.Sprintf("bolt-%s-%s-%v-%d-%d", shape.name, fail, explicit, d, time.Now().UnixNano())
						params := map[string]any{"run": run, "d": d, "body": compositeAtomicityBody}
						statement := "USE accmp " + strings.ReplaceAll(shape.query, "{fail}", fail)
						session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "accmp"})
						defer func() { _ = session.Close(ctx) }()
						var runErr error
						if explicit {
							tx, err := session.BeginTransaction(ctx)
							require.NoError(t, err)
							result, err := tx.Run(ctx, statement, params)
							if err == nil {
								_, err = result.Consume(ctx)
							}
							runErr = err
							if runErr == nil {
								require.NoError(t, tx.Commit(ctx))
							} else {
								_ = tx.Rollback(ctx)
							}
							_ = tx.Close(ctx)
						} else {
							result, err := session.Run(ctx, statement, params)
							if err == nil {
								_, err = result.Consume(ctx)
							}
							runErr = err
						}
						want := shape.writes
						if d == 0 {
							require.Error(t, runErr)
							want = 0
						} else {
							require.NoError(t, runErr)
						}
						require.Equal(t, want, writes("accmp.one", run), "AcAtom nodes left on accmp.one")

						referenceStatement := shape.reference
						if referenceStatement == "" {
							referenceStatement = strings.NewReplacer("USE accmp.one ", "", "USE {fail} ", "").Replace(shape.query)
						}
						failed, referenceCount := referenceWrites(referenceStatement, params)
						require.Equal(t, d == 0, failed, "Neo4j reference outcome")
						require.Equal(t, want, referenceCount, "Neo4j reference writes")
					})
				}
			}
		}
	}

	for _, order := range compositeSecondWriteOrders() {
		t.Run("second_write_database/"+order.name, func(t *testing.T) {
			run := fmt.Sprintf("bolt-shard-%s-%d", order.name, time.Now().UnixNano())
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "accmp"})
			defer func() { _ = session.Close(ctx) }()
			result, err := session.Run(ctx, order.query, map[string]any{"run": run, "body": compositeAtomicityBody})
			if err == nil {
				_, err = result.Consume(ctx)
			}
			require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
			require.Equal(t, int64(0), writes("accmp.one", run))
			require.Equal(t, int64(0), writes("accmp.two", run))
		})
	}
}
