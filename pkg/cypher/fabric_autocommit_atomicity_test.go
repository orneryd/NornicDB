package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// fabricAtomicityProcedure is a WRITE procedure whose body creates the
// AcAtom node of run, so its write originates in a procedure.
const fabricAtomicityProcedure = "CREATE OR REPLACE PROCEDURE acatom.write($run) MODE WRITE AS CREATE (:AcAtom {run: $run}) RETURN 1 AS w"

// fabricAtomicityShapes are composite statements that write to the local
// constituent comp.one and then fail in a later part of the same statement
// when $d is 0, or succeed when $d is 1. {fail} is the constituent the
// failing part runs on; it imports no variable, so the remote constituents
// fail at run time, not on an import. Neo4j runs a statement as one
// transaction: a failed statement leaves none of its writes, a successful one
// all of them. reference is the statement Neo4j runs, when it differs from
// the statement without its USE selectors.
var fabricAtomicityShapes = []struct {
	name, query, reference string
	writes                 int64
}{
	{"call_then_error", "CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z", "", 1},
	{"union_branch_error", "CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS z UNION ALL USE {fail} RETURN 1 / $d AS z } RETURN z", "", 1},
	{"nested_call_error", "CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { CALL { USE {fail} RETURN 1 / $d AS z } RETURN z } RETURN w, z", "", 1},
	{"second_write_error", "CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.one CREATE (:AcAtom {run: $run, v: w / $d}) RETURN 1 AS z } RETURN z", "", 2},
	{"procedure_write_then_error", "CALL { USE comp.one CALL acatom.write($run) YIELD w RETURN w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z",
		"CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z", 1},
	{"dynamic_write_then_error", "CALL { USE comp.one CALL apoc.cypher.doIt('CREATE (:AcAtom {run: $run}) RETURN 1 AS w', {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z",
		"CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z", 1},
	{"read_call_write_then_error", "CALL { USE comp.one CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z",
		"CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z", 1},
	{"doitall_write_then_error", "CALL { USE comp.one CALL apoc.cypher.doitall($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { USE {fail} RETURN 1 / $d AS z } RETURN w, z",
		"CALL { CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { RETURN 1 / $d AS z } RETURN w, z", 1},
}

// fabricAtomicityFixture is a composite comp with local constituents one and
// two and, with --ref neo-ref, the remote constituents remote (Bolt) and http
// (HTTP) on Neo4j 5.26.30, which is also the reference the statements are
// compared with.
type fabricAtomicityFixture struct {
	exec      *StorageExecutor
	mgr       *multidb.DatabaseManager
	failures  []string
	reference neo4j.DriverWithContext
}

func newFabricAtomicityFixture(t testing.TB) *fabricAtomicityFixture {
	t.Helper()
	config := multidb.DefaultConfig()
	config.RemoteEngineFactory = func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
		return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: authToken})
	}
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	constituents := []multidb.ConstituentRef{}
	for _, db := range []string{"one", "two"} {
		require.NoError(t, mgr.CreateDatabase(db))
		constituents = append(constituents, multidb.ConstituentRef{Alias: db, DatabaseName: db, Type: "local", AccessMode: "read_write"})
	}
	fixture := &fabricAtomicityFixture{mgr: mgr, failures: []string{"comp.one", "comp.two"}}
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		constituents = append(constituents, multidb.ConstituentRef{Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri})
		fixture.failures = append(fixture.failures, "comp.remote")
		fixture.reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { _ = fixture.reference.Close(context.Background()) })
	}
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI"); uri != "" {
		constituents = append(constituents, multidb.ConstituentRef{Alias: "http", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri})
		fixture.failures = append(fixture.failures, "comp.http")
	}
	require.NoError(t, mgr.CreateCompositeDatabase("comp", constituents))
	store, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	fixture.exec = NewStorageExecutor(store)
	fixture.exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	fixture.exec.cache = nil
	_, err = fixture.exec.Execute(context.Background(), fabricAtomicityProcedure, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = fixture.exec.Execute(context.Background(), "DROP PROCEDURE acatom.write", nil) })
	return fixture
}

// compositeSession is an executor whose session database is comp, for an
// explicit transaction on it.
func (f *fabricAtomicityFixture) compositeSession(t *testing.T) *StorageExecutor {
	t.Helper()
	store, err := f.mgr.GetStorage("comp")
	require.NoError(t, err)
	session := NewStorageExecutor(store)
	session.SetDatabaseManager(f.exec.dbManager)
	return session
}

func (f *fabricAtomicityFixture) writes(t *testing.T, constituent, run string) int64 {
	t.Helper()
	result, err := f.exec.Execute(context.Background(), "USE "+constituent+" MATCH (n:AcAtom {run: $run}) RETURN count(n) AS c", map[string]interface{}{"run": run})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	return result.Rows[0][0].(int64)
}

// referenceWrites runs the statement on Neo4j, a single database, in an
// autocommit transaction, and returns its error and how many AcAtom nodes of
// its run it left (which it then removes).
func (f *fabricAtomicityFixture) referenceWrites(t *testing.T, query string, params map[string]interface{}) (error, int64) {
	t.Helper()
	ctx := context.Background()
	session := f.reference.NewSession(ctx, neo4j.SessionConfig{})
	defer func() { _ = session.Close(ctx) }()
	result, err := session.Run(ctx, query, params)
	if err == nil {
		_, err = result.Consume(ctx)
	}
	count, countErr := neo4j.ExecuteQuery(ctx, f.reference, "MATCH (n:AcAtom {run: $run}) WITH count(n) AS c, collect(n) AS ns FOREACH (n IN ns | DELETE n) RETURN c", map[string]interface{}{"run": params["run"]}, neo4j.EagerResultTransformer)
	require.NoError(t, countErr)
	return err, count.Records[0].Values[0].(int64)
}

// assertReference checks that Neo4j fails the statement exactly when failed is
// set and leaves writes nodes. Every USE selector is omitted unless reference
// gives the statement.
func (f *fabricAtomicityFixture) assertReference(t *testing.T, query, reference string, params map[string]interface{}, failed bool, writes int64) {
	t.Helper()
	if f.reference == nil {
		return
	}
	if reference == "" {
		reference = query
		for _, use := range []string{"USE comp.one ", "USE {fail} ", "USE comp "} {
			reference = strings.ReplaceAll(reference, use, "")
		}
	}
	err, count := f.referenceWrites(t, reference, params)
	require.Equal(t, failed, err != nil, "Neo4j reference outcome: %v", err)
	require.Equal(t, writes, count, "Neo4j reference writes")
}

// An autocommit composite statement commits its local writes, from a write
// clause or from a WRITE procedure, only when the whole statement succeeds,
// as Neo4j commits a statement. The failure comes after the local write, on
// the same or another local constituent, or (with --ref neo-ref) on a remote
// constituent over Bolt or HTTP. An explicit transaction already behaves so;
// it is checked with local failures, whose rollback involves no remote
// participant.
func TestFabricAutocommitStatementWritesAreAtomic(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	for _, shape := range fabricAtomicityShapes {
		for _, fail := range fixture.failures {
			for _, mode := range []string{"autocommit", "explicit"} {
				if mode == "explicit" && fail != "comp.one" && fail != "comp.two" {
					continue
				}
				for _, d := range []int64{0, 1} {
					t.Run(fmt.Sprintf("%s/%s/%s/d=%d", shape.name, fail, mode, d), func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						run := fmt.Sprintf("%s-%s-%s-%d-%d", shape.name, fail, mode, d, time.Now().UnixNano())
						params := map[string]interface{}{"run": run, "d": d, "body": fabricAtomicityBody}
						runExec := fixture.exec
						if mode == "explicit" {
							runExec = fixture.compositeSession(t)
							_, err := runExec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
						}
						_, err := runExec.Execute(ctx, "USE comp "+strings.ReplaceAll(shape.query, "{fail}", fail), params)
						if mode == "explicit" {
							end := "COMMIT"
							if err != nil {
								end = "ROLLBACK"
							}
							_, endErr := runExec.Execute(ctx, end, nil)
							require.NoError(t, endErr)
						}
						want := shape.writes
						if d == 0 {
							require.Error(t, err)
							want = 0
						} else {
							require.NoError(t, err)
						}
						require.Equal(t, want, fixture.writes(t, "comp.one", run), "AcAtom nodes left on comp.one")
						fixture.assertReference(t, shape.query, shape.reference, params, d == 0, want)
					})
				}
			}
		}
	}
}

// fabricAtomicityBody is the writing body the dynamic procedures run.
const fabricAtomicityBody = "CREATE (:AcAtom {run: $run}) RETURN 1 AS w"

// A write from a procedure makes its constituent the statement's one write
// constituent, as a write clause does, whether the procedure is registered
// as writing or is a reading procedure whose dynamic body writes
// (apoc.cypher.run, apoc.cypher.doitall): a write to another constituent,
// before or after it, is refused, and the statement leaves nothing on
// either.
func TestFabricProcedureWriteIsTheWriteConstituent(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	orders := []struct{ name, query string }{
		{"procedure_first", "USE comp CALL { USE comp.one CALL acatom.write($run) YIELD w RETURN w } CALL { WITH w USE comp.two CREATE (:AcAtom {run: $run}) RETURN w AS z } RETURN z"},
		{"procedure_second", "USE comp CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE comp.one CALL acatom.write($run) YIELD w AS z RETURN z } RETURN w, z"},
	}
	for _, procedure := range []string{"apoc.cypher.run", "apoc.cypher.doitall"} {
		call := "CALL " + procedure + "($body, {run: $run}) YIELD value"
		orders = append(orders,
			struct{ name, query string }{procedure + "_first", "USE comp CALL { USE comp.one " + call + " RETURN value.w AS w } CALL { WITH w USE comp.two CREATE (:AcAtom {run: $run}) RETURN w AS z } RETURN z"},
			struct{ name, query string }{procedure + "_second", "USE comp CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE comp.one " + call + " RETURN value.w AS z } RETURN w, z"},
		)
	}
	for _, order := range orders {
		for _, mode := range []string{"autocommit", "explicit"} {
			t.Run(order.name+"/"+mode, func(t *testing.T) {
				fixture.assertSecondWriteRefused(t, order.query, mode)
			})
		}
	}
}

func (f *fabricAtomicityFixture) assertSecondWriteRefused(t *testing.T, query, mode string) {
	t.Helper()
	ctx := context.Background()
	run := fmt.Sprintf("procedure-shard-%s-%d", mode, time.Now().UnixNano())
	runExec := f.exec
	if mode == "explicit" {
		runExec = f.compositeSession(t)
		_, err := runExec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
	}
	_, err := runExec.Execute(ctx, query, map[string]interface{}{"run": run, "body": fabricAtomicityBody})
	require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
	if mode == "explicit" {
		_, endErr := runExec.Execute(ctx, "ROLLBACK", nil)
		require.NoError(t, endErr)
	}
	require.Equal(t, int64(0), f.writes(t, "comp.one", run))
	require.Equal(t, int64(0), f.writes(t, "comp.two", run))
}

// The statements of an explicit transaction on a composite write to one
// database, however each writes: a write clause, a WRITE procedure, dynamic
// Cypher, or a write that changes nothing, which takes the database as
// Neo4j takes a query's access mode. A write to another database is refused
// and the transaction keeps nothing.
func TestFabricExplicitTransactionWritesToOneDatabase(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	fixture.createRunner(t)
	ctx := context.Background()
	for _, tt := range []struct {
		name       string
		statements []string
		refused    bool // the last statement is refused
		one, two   int64
	}{
		{"write_then_write_same", []string{"USE comp.one CREATE (:AcAtom {run: $run})", "USE comp CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } RETURN w"}, false, 2, 0},
		{"read_then_write_other", []string{"USE comp.one MATCH (n:AcAtom) RETURN count(n) AS c", "USE comp.two CREATE (:AcAtom {run: $run})"}, false, 0, 1},
		{"write_then_write_other", []string{"USE comp.one CREATE (:AcAtom {run: $run})", "USE comp.two CREATE (:AcAtom {run: $run})"}, true, 0, 0},
		{"procedure_then_write_other", []string{"USE comp.one CALL acatom.write($run) YIELD w RETURN w", "USE comp.two CREATE (:AcAtom {run: $run})"}, true, 0, 0},
		{"dynamic_then_write_other", []string{"USE comp.one CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w", "USE comp.two CREATE (:AcAtom {run: $run})"}, true, 0, 0},
		{"empty_write_then_write_other", []string{"USE comp.one MATCH (n:AcMissing) SET n.v = 1", "USE comp.two CREATE (:AcAtom {run: $run})"}, true, 0, 0},
		{"runner_then_write_other", []string{"USE comp.one CALL acatom.runner($stmt, $run) YIELD w RETURN w", "USE comp.two CREATE (:AcAtom {run: $run})"}, true, 0, 0},
		{"nested_dynamic_then_write_other", []string{"USE comp.one CALL apoc.cypher.run($nested, {run: $run, inner: $inner}) YIELD value RETURN value.w AS w", "USE comp.two CREATE (:AcAtom {run: $run})"}, true, 0, 0},
		{"composite_runner_then_write_other", []string{"USE comp CALL { USE comp.one CALL acatom.runner($stmt, $run) YIELD w RETURN w } RETURN w", "USE comp CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN z"}, true, 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			session := fixture.compositeSession(t)
			run := fmt.Sprintf("explicit-%s-%d", tt.name, time.Now().UnixNano())
			params := fabricEffectParams(run)
			_, err := session.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			last := len(tt.statements) - 1
			for i, statement := range tt.statements {
				_, err = session.Execute(ctx, statement, params)
				if i < last || !tt.refused {
					require.NoError(t, err, statement)
				}
			}
			end := "COMMIT"
			if tt.refused {
				require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
				end = "ROLLBACK"
			}
			_, err = session.Execute(ctx, end, nil)
			require.NoError(t, err)
			require.Equal(t, tt.one, fixture.writes(t, "comp.one", run), "AcAtom nodes left on comp.one")
			require.Equal(t, tt.two, fixture.writes(t, "comp.two", run), "AcAtom nodes left on comp.two")
		})
	}
}

// Within the statement, later fragments on the written constituent read its
// pending writes, also from batched lookups whose chunks may be dispatched
// concurrently, and the writes are visible once the statement has succeeded.
func TestFabricAutocommitStatementReadsItsOwnWrites(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	for _, shape := range []struct {
		name, query, want string
		writes            int64
	}{
		{"call", "CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.one MATCH (n:AcAtom {run: $run}) RETURN count(n) AS c } RETURN c", "[[1]]", 1},
		{"batched_lookup", "CALL { USE comp.one UNWIND range(1, 600) AS k CREATE (:AcAtom {run: $run, k: k}) RETURN k } CALL { WITH k USE comp.one MATCH (n:AcAtom {run: $run}) WHERE n.k = k RETURN count(n) AS c } RETURN count(*) AS rows, sum(c) AS matched", "[[600,600]]", 600},
	} {
		t.Run(shape.name, func(t *testing.T) {
			run := fmt.Sprintf("own-%s-%d", shape.name, time.Now().UnixNano())
			params := map[string]interface{}{"run": run}
			result, err := fixture.exec.Execute(context.Background(), "USE comp "+shape.query, params)
			require.NoError(t, err)
			rows, err := json.Marshal(result.Rows)
			require.NoError(t, err)
			require.JSONEq(t, shape.want, string(rows))
			require.Equal(t, shape.writes, fixture.writes(t, "comp.one", run))
			fixture.assertReference(t, shape.query, "", params, false, shape.writes)
		})
	}
}

// CALL { ... } IN TRANSACTIONS keeps its own batch commits on a constituent:
// the composite statement leaves what the same statement leaves on the plain
// database.
func TestFabricAutocommitCallInTransactionsKeepsBatchCommits(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	const body = " UNWIND [1, 1, $d] AS d CALL { WITH d CREATE (:AcAtom {run: $run, v: 1 / d}) } IN TRANSACTIONS OF 1 ROW"
	for _, d := range []int64{0, 1} {
		t.Run(fmt.Sprintf("d=%d", d), func(t *testing.T) {
			outcome := map[string]int64{}
			for _, use := range []string{"one", "comp.one"} {
				run := fmt.Sprintf("batches-%s-%d-%d", use, d, time.Now().UnixNano())
				_, err := fixture.exec.Execute(context.Background(), "USE "+use+body, map[string]interface{}{"run": run, "d": d})
				require.Equal(t, d == 0, err != nil, "%s: %v", use, err)
				outcome[use] = fixture.writes(t, "comp.one", run)
			}
			if d == 1 {
				require.Equal(t, int64(3), outcome["comp.one"])
			}
			require.Equal(t, outcome["one"], outcome["comp.one"], "composite and plain database batch commits")
			if fixture.reference != nil {
				_, writes := fixture.referenceWrites(t, strings.TrimSpace(body), map[string]interface{}{"run": fmt.Sprintf("batches-ref-%d-%d", d, time.Now().UnixNano()), "d": d})
				t.Logf("IN TRANSACTIONS d=%d: plain %d, composite %d, Neo4j %d", d, outcome["one"], outcome["comp.one"], writes)
			}
		})
	}
}

// fabricBenchBatch is how many operations BenchmarkFabricAutocommitWrite
// runs on one fixture: the store a write grows stays bounded.
const fabricBenchBatch = 64

// BenchmarkFabricAutocommitWrite measures autocommit composite statements on
// local constituents: writes, which run in their constituent's
// sub-transaction, read-only statements, and the same writes on a single
// database as the control.
func BenchmarkFabricAutocommitWrite(b *testing.B) {
	for _, shape := range []struct{ name, query string }{
		{"create", "USE comp CALL { USE comp.one CREATE (:AcBench {i: $i}) RETURN 1 AS w } RETURN w"},
		{"create_then_read_other", "USE comp CALL { USE comp.one CREATE (:AcBench {i: $i}) RETURN 1 AS w } CALL { WITH w USE comp.two RETURN w + 1 AS z } RETURN z"},
		{"create_then_read_same", "USE comp CALL { USE comp.one CREATE (:AcBench {i: $i}) RETURN 1 AS w } CALL { WITH w USE comp.one MATCH (n:AcBench {i: $i}) RETURN count(n) AS z } RETURN z"},
		{"unwind_create_100", "USE comp CALL { USE comp.one UNWIND range(1, 100) AS k CREATE (:AcBench {i: $i, k: k}) RETURN count(*) AS w } RETURN w"},
		{"procedure_create", "USE comp CALL { USE comp.one CALL acatom.write($run) YIELD w RETURN w } RETURN w"},
		{"read_only", "USE comp CALL { USE comp.one MATCH (n:AcBenchMissing) RETURN count(n) AS c } RETURN c"},
		{"read_only_procedure", "USE comp CALL { USE comp.one CALL db.labels() YIELD label RETURN count(label) AS c } RETURN c"},
		{"read_batched_lookup", "USE comp CALL { USE comp.one UNWIND range(1, 300) AS k RETURN k } CALL { WITH k USE comp.one MATCH (n:AcBenchMissing) WHERE n.k = k RETURN count(n) AS c } RETURN sum(c) AS s"},
		// Single-database controls: the same writes on the constituent's own
		// database, which autocommit has always run in a transaction.
		{"single_create", "USE one CREATE (:AcBench {i: $i}) RETURN 1 AS w"},
		{"single_unwind_create_100", "USE one UNWIND range(1, 100) AS k CREATE (:AcBench {i: $i, k: k}) RETURN count(*) AS w"},
	} {
		b.Run(shape.name, func(b *testing.B) {
			fixture := newFabricAtomicityFixture(b)
			ctx := context.Background()
			params := map[string]interface{}{"i": int64(0), "run": ""}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i > 0 && i%fabricBenchBatch == 0 {
					// Each batch runs on a fresh fixture, built outside the
					// timed region, so an operation's cost doesn't grow
					// with b.N.
					b.StopTimer()
					fixture = newFabricAtomicityFixture(b)
					b.StartTimer()
				}
				params["i"] = int64(i)
				params["run"] = fmt.Sprintf("bench-%d", i)
				if _, err := fixture.exec.Execute(ctx, shape.query, params); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
