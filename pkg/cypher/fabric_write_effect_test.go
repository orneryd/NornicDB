package cypher

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// fabricRunnerProcedure is a READ user procedure whose body runs the
// statement it is given (apoc.cypher.run), which may write.
const fabricRunnerProcedure = "CREATE OR REPLACE PROCEDURE acatom.runner($stmt, $run) MODE READ AS CALL apoc.cypher.run($stmt, {run: $run}) YIELD value RETURN value.w AS w"

func (f *fabricAtomicityFixture) createRunner(t *testing.T) {
	t.Helper()
	_, err := f.exec.Execute(context.Background(), fabricRunnerProcedure, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = f.exec.Execute(context.Background(), "DROP PROCEDURE acatom.runner", nil) })
}

// fabricEffectParams are the parameters of the statements below: the
// writing bodies the dynamic procedures run.
func fabricEffectParams(run string) map[string]interface{} {
	return map[string]interface{}{
		"run": run, "body": fabricAtomicityBody, "stmt": fabricAtomicityBody, "inner": fabricAtomicityBody,
		"nested":       "CALL apoc.cypher.run($inner, {run: $run}) YIELD value RETURN value.w AS w",
		"profiled":     "PROFILE CREATE (:AcAtom {run: $run}) RETURN 1 AS w",
		"useone":       "USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w",
		"usetwo":       "USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS w",
		"compositeone": "USE comp CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS c } RETURN c AS w",
		"plainone":     "USE one CREATE (:AcAtom {run: $run}) RETURN 1 AS w",
		"set":          int64(1),
		"d":            int64(1),
	}
}

// A write a fragment makes claims its database when it runs, however the
// fragment's text shows it: a write clause in a subquery, FOREACH, MERGE, any
// letter case or comment, a WRITE procedure called in a subquery or spelled
// with backticks or spaces, or a statement a READ procedure runs (dynamic
// Cypher, nested, profiled, exported, a user procedure's body, one that
// selects another database with USE, or a composite statement). With a write
// on the other database, before or after it, the statement is refused with
// Neo.ClientError.Statement.AccessMode and leaves nothing on either.
func TestFabricWriteClaimsItsDatabaseWhereverItRuns(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	fixture.createRunner(t)
	// A READ plugin procedure whose handler runs a writing statement with
	// Execute.
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "acatom.plugin", Signature: "acatom.plugin(run :: STRING) :: (w :: INTEGER)",
		Mode: ProcedureModeRead, MinArgs: 1, MaxArgs: 1, Params: []ProcedureParam{{Name: "run", Type: "STRING"}}, Returns: []ProcedureColumn{{Name: "w", Type: "INTEGER"}}},
		func(ctx context.Context, exec *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
			return exec.Execute(ctx, fabricAtomicityBody, map[string]interface{}{"run": args[0]})
		}))
	// One that runs the statement it is given, which may select a database.
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "acatom.pluginrun", Signature: "acatom.pluginrun(stmt :: STRING, run :: STRING) :: (w :: INTEGER)",
		Mode: ProcedureModeRead, MinArgs: 2, MaxArgs: 2, Params: []ProcedureParam{{Name: "stmt", Type: "STRING"}, {Name: "run", Type: "STRING"}}, Returns: []ProcedureColumn{{Name: "w", Type: "INTEGER"}}},
		func(ctx context.Context, exec *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
			return exec.Execute(ctx, args[0].(string), map[string]interface{}{"run": args[1]})
		}))
	t.Cleanup(ClearUserProcedures)
	fragments := map[string]string{
		"unit_subquery":            "CALL { CREATE (:AcAtom {run: $run}) } RETURN 1 AS w",
		"foreach":                  "FOREACH (x IN [1] | CREATE (:AcAtom {run: $run})) RETURN 1 AS w",
		"lowercase":                "create (:AcAtom {run: $run}) RETURN 1 AS w",
		"comment_glued":            "/*c*/CREATE/*d*/(:AcAtom {run: $run}) RETURN 1 AS w",
		"line_comment":             "// note\nCREATE (:AcAtom {run: $run}) RETURN 1 AS w",
		"merge":                    "MERGE (n:AcAtom {run: $run}) RETURN 1 AS w",
		"procedure_in_subquery":    "CALL { CALL acatom.write($run) YIELD w RETURN w } RETURN w",
		"backticked_procedure":     "CALL `acatom`.`write`($run) YIELD w RETURN w",
		"spaced_procedure":         "CALL acatom . write ($run) YIELD w RETURN w",
		"lowercase_call":           "call acatom.write($run) yield w return w",
		"nested_dynamic":           "CALL apoc.cypher.run($nested, {run: $run, inner: $inner}) YIELD value RETURN value.w AS w",
		"user_read_runner":         "CALL acatom.runner($stmt, $run) YIELD w RETURN w",
		"profile_in_dynamic":       "CALL apoc.cypher.run($profiled, {run: $run}) YIELD value RETURN 1 AS w",
		"export_query":             "CALL apoc.export.json.query($body, null, {stream: true, params: {run: $run}}) YIELD rows RETURN 1 AS w",
		"dynamic_use_constituent":  "CALL apoc.cypher.run($useone, {run: $run}) YIELD value RETURN value.w AS w",
		"dynamic_use_database":     "CALL apoc.cypher.run($plainone, {run: $run}) YIELD value RETURN value.w AS w",
		"dynamic_composite":        "CALL apoc.cypher.run($compositeone, {run: $run}) YIELD value RETURN value.w AS w",
		"runner_selects_composite": "CALL acatom.runner($compositeone, $run) YIELD w RETURN w",
		"plugin_execute":           "CALL acatom.plugin($run) YIELD w RETURN w",
		"plugin_selects_database":  "CALL acatom.pluginrun($plainone, $run) YIELD w RETURN w",
		"plugin_selects_composite": "CALL acatom.pluginrun($compositeone, $run) YIELD w RETURN w",
	}
	for name, fragment := range fragments {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/other_before=%v", name, before), func(t *testing.T) {
				run := fmt.Sprintf("effect-%s-%v-%d", name, before, time.Now().UnixNano())
				one := "CALL { USE comp.one " + fragment + " } "
				two := "CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } "
				query := "USE comp " + one + two + "RETURN w, z"
				if before {
					query = "USE comp " + two + one + "RETURN w, z"
				}
				_, err := fixture.exec.Execute(context.Background(), query, fabricEffectParams(run))
				if name == "profile_in_dynamic" {
					// A statement a procedure runs is no PROFILE target: the
					// body fails on its own, before it writes.
					require.Error(t, err)
				} else {
					require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
				}
				require.Equal(t, int64(0), fixture.writes(t, "comp.one", run), "AcAtom nodes left on comp.one")
				require.Equal(t, int64(0), fixture.writes(t, "comp.two", run), "AcAtom nodes left on comp.two")
			})
		}
	}
}

// A composite statement commits as one, so it runs no CALL { ... } IN
// TRANSACTIONS batch: one in a fragment, after the statement wrote, on
// another database than its write, or in a statement a procedure runs inside
// it is refused with SyntaxError before it runs, autocommit or in an
// explicit transaction, and nothing the statement wrote is kept.
func TestFabricTransactionalCallIsRefused(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	ctx := context.Background()
	const refused = "transactional CALL on composite targets is not supported"
	const batch = "UNWIND [1] AS i CALL { WITH i CREATE (:AcBatch {run: $run}) } IN TRANSACTIONS OF 1 ROW RETURN 1 AS v"
	written := "USE comp CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } "
	statements := map[string]string{
		"fragment":       "USE comp CALL { USE comp.one " + batch + " } RETURN v",
		"after_write":    written + "CALL { WITH w USE comp.one " + batch + " } RETURN v",
		"other_database": written + "CALL { WITH w USE comp.two " + batch + " } RETURN v",
		"procedure":      written + "CALL apoc.cypher.doIt($batch, {run: $run}) YIELD value RETURN w",
	}
	batches := func(t *testing.T, run string) int64 {
		t.Helper()
		var total int64
		for _, constituent := range []string{"comp.one", "comp.two"} {
			result, err := fixture.exec.Execute(ctx, "USE "+constituent+" MATCH (n:AcBatch {run: $run}) RETURN count(n) AS c", map[string]interface{}{"run": run})
			require.NoError(t, err)
			total += result.Rows[0][0].(int64)
		}
		return total
	}
	for name, statement := range statements {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", name, explicit), func(t *testing.T) {
				run := fmt.Sprintf("batch-%s-%v-%d", name, explicit, time.Now().UnixNano())
				params := map[string]interface{}{"run": run, "batch": "USE one " + batch}
				session := fixture.exec
				if explicit {
					session = fixture.compositeSession(t)
					_, err := session.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				_, err := session.Execute(ctx, statement, params)
				require.ErrorContains(t, err, refused)
				if name != "procedure" {
					code, _ := nornicerrors.Neo4jStatus(err)
					require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
				}
				if explicit {
					_, err = session.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
				require.Equal(t, int64(0), fixture.writes(t, "comp.one", run), "nothing the statement wrote is kept")
				require.Equal(t, int64(0), batches(t, run), "no batch ran")
			})
		}
	}
}

// Concurrent MERGEs of a uniquely constrained key on a composite all match
// the node one of them creates, as on one database (#961): the key is
// reserved before the MERGE reads, without claiming the database, and held
// for a MERGE that writes the node it matches (ON MATCH SET, a SET after
// it), so none of them fails. Each round runs 16 statements at once, through
// a fragment, a procedure that selects the database, and an explicit
// transaction.
func TestFabricConcurrentMergeOfConstrainedKeyMatches(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	ctx := context.Background()
	_, err := fixture.exec.Execute(ctx, "USE comp.one CREATE CONSTRAINT acmerge_k IF NOT EXISTS FOR (n:AcMerge) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	shapes := map[string]string{
		"fragment":              "USE comp CALL { USE comp.one MERGE (n:AcMerge {k: $k}) RETURN n.k AS k } RETURN k",
		"on_match_set":          "USE comp CALL { USE comp.one MERGE (n:AcMerge {k: $k}) ON CREATE SET n.created = 1 ON MATCH SET n.matched = 1 RETURN n.k AS k } RETURN k",
		"merge_then_set":        "USE comp CALL { USE comp.one MERGE (n:AcMerge {k: $k}) SET n.seen = 1 RETURN n.k AS k } RETURN k",
		"procedure":             "USE comp CALL apoc.cypher.doIt('USE one MERGE (n:AcMerge {k: $k}) RETURN n.k AS k', {k: $k}) YIELD value RETURN value.k AS k",
		"explicit":              "USE comp CALL { USE comp.one MERGE (n:AcMerge {k: $k}) RETURN n.k AS k } RETURN k",
		"explicit_on_match_set": "USE comp CALL { USE comp.one MERGE (n:AcMerge {k: $k}) ON MATCH SET n.matched = 1 RETURN n.k AS k } RETURN k",
	}
	for name, query := range shapes {
		for round := 0; round < 8; round++ {
			t.Run(fmt.Sprintf("%s/%d", name, round), func(t *testing.T) {
				key := fmt.Sprintf("%s-%d-%d", name, round, time.Now().UnixNano())
				const n = 16
				errs := make([]error, n)
				var wg sync.WaitGroup
				for i := 0; i < n; i++ {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						params := map[string]interface{}{"k": key}
						if !strings.HasPrefix(name, "explicit") {
							_, errs[i] = fixture.exec.Execute(ctx, query, params)
							return
						}
						session := fixture.compositeSession(t)
						if _, errs[i] = session.Execute(ctx, "BEGIN", nil); errs[i] != nil {
							return
						}
						if _, errs[i] = session.Execute(ctx, query, params); errs[i] != nil {
							_, _ = session.Execute(ctx, "ROLLBACK", nil)
							return
						}
						_, errs[i] = session.Execute(ctx, "COMMIT", nil)
					}(i)
				}
				wg.Wait()
				for i, err := range errs {
					require.NoError(t, err, "statement %d", i)
				}
				result, err := fixture.exec.Execute(ctx, "USE comp.one MATCH (n:AcMerge {k: $k}) RETURN count(n) AS c", map[string]interface{}{"k": key})
				require.NoError(t, err)
				require.Equal(t, int64(1), result.Rows[0][0])
			})
		}
	}
}

// A composite statement reads a constituent in its transaction's snapshot,
// taken when the statement first reaches that database, as a statement on
// one database reads in its transaction's: a node another session changes
// and commits after the statement read it, before the statement writes it,
// is a conflict (Neo.TransientError.Transaction.Outdated), and the statement
// keeps nothing. The same statement on the database itself is the reference,
// autocommit and in an explicit transaction.
func TestFabricStatementReadsInItsTransactionSnapshot(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	ctx := context.Background()
	const body = "MATCH (n:AcSnap {run: $run}) WITH n, test.acatomSnapshotAwait(n.run) AS seen SET n.statement = 1 RETURN seen"
	outcome := func(t *testing.T, query string, explicit bool) (string, interface{}) {
		t.Helper()
		run := fmt.Sprintf("snapshot-%d", time.Now().UnixNano())
		params := map[string]interface{}{"run": run}
		_, err := fixture.exec.Execute(ctx, "USE one CREATE (:AcSnap {run: $run, v: 1})", params)
		require.NoError(t, err)
		session := fixture.exec
		if explicit {
			session = fixture.compositeSession(t)
			_, err = session.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
		}
		seen, resume := make(chan struct{}), make(chan struct{})
		var once, release sync.Once
		defer release.Do(func() { close(resume) })
		previous := PluginFunctionLookup
		PluginFunctionLookup = func(name string) (interface{}, bool) {
			if name == "test.acatomSnapshotAwait" {
				return func(value string) string {
					once.Do(func() { close(seen) })
					<-resume
					return value
				}, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		}
		defer func() { PluginFunctionLookup = previous }()
		finished := make(chan error, 1)
		go func() {
			_, err := session.Execute(ctx, query, params)
			finished <- err
		}()
		select {
		case <-seen:
		case err := <-finished:
			t.Fatalf("the statement ended before its read: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("the statement did not read")
		}
		_, err = fixture.exec.Execute(ctx, "USE one MATCH (n:AcSnap {run: $run}) SET n.v = 2", params)
		require.NoError(t, err, "the other session's change commits")
		release.Do(func() { close(resume) })
		err = <-finished
		if explicit {
			if err == nil {
				_, err = session.Execute(ctx, "COMMIT", nil)
			} else {
				_, _ = session.Execute(ctx, "ROLLBACK", nil)
			}
		}
		code, _ := nornicerrors.Neo4jStatus(err)
		if err == nil {
			code = ""
		}
		result, countErr := fixture.exec.Execute(ctx, "USE one MATCH (n:AcSnap {run: $run}) RETURN n.v, n.statement", params)
		require.NoError(t, countErr)
		return code, result.Rows[0]
	}
	reference, referenceRow := outcome(t, "USE one "+body, false)
	require.Equal(t, nornicerrors.TransientOutdated, reference, "the reference: one database")
	require.Equal(t, []interface{}{int64(2), nil}, referenceRow, "the other session's change stays, the statement's doesn't")
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			code, row := outcome(t, "USE comp CALL { USE comp.one "+body+" } RETURN seen", explicit)
			require.Equal(t, reference, code)
			require.Equal(t, referenceRow, row)
		})
	}
}

// Change data capture of a constituent is read inside a composite statement
// as on the database itself: db.cdc.current and db.cdc.query find the
// database's change capture through its participant storage, and a
// composite statement's write is captured once.
func TestFabricChangeCaptureInsideCompositeStatement(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	ctx := context.Background()
	store, err := fixture.mgr.GetStorage("one")
	require.NoError(t, err)
	capture, ok := storage.ChangeCaptureOf(store)
	require.True(t, ok)
	capture.SetChangeCapture("one", storage.ChangeCaptureFull)
	single, err := fixture.exec.Execute(ctx, "USE one CALL db.cdc.current() YIELD id RETURN id", nil)
	require.NoError(t, err)
	from := single.Rows[0][0]
	for _, query := range []string{
		"USE comp CALL { USE comp.one CALL db.cdc.current() YIELD id RETURN id } RETURN id",
		"USE comp CALL apoc.cypher.doIt('USE one CALL db.cdc.current() YIELD id RETURN id', {}) YIELD value RETURN value.id AS id",
	} {
		current, err := fixture.exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, from, current.Rows[0][0], query)
	}
	run := fmt.Sprintf("cdc-%d", time.Now().UnixNano())
	_, err = fixture.exec.Execute(ctx, "USE comp CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } RETURN w", map[string]interface{}{"run": run})
	require.NoError(t, err)
	for _, query := range []string{
		"USE one CALL db.cdc.query($from, []) YIELD id RETURN count(id) AS c",
		"USE comp CALL { USE comp.one CALL db.cdc.query($from, []) YIELD id RETURN count(id) AS c } RETURN c",
	} {
		events, err := fixture.exec.Execute(ctx, query, map[string]interface{}{"from": from})
		require.NoError(t, err, query)
		require.Equal(t, int64(1), events.Rows[0][0], query)
	}
}

// A WRITE procedure called on the composite database that writes one
// database (apoc.cypher.doIt, apoc.cypher.runMany, a persisted MODE WRITE
// procedure, the READ apoc.cypher.run, each also nested in CALL { }) writes
// that database and succeeds: the composite database is a router and never
// the write database. The same procedures writing two databases are
// refused and leave nothing.
func TestFabricWriteProcedureOnCompositeWritesOneDatabase(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	ctx := context.Background()
	_, err := fixture.exec.Execute(ctx, "CREATE OR REPLACE PROCEDURE acatom.usewrite($run) MODE WRITE AS USE one CREATE (:AcAtom {run: $run}) RETURN 1 AS w", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = fixture.exec.Execute(ctx, "DROP PROCEDURE acatom.usewrite", nil) })
	one := "USE one CREATE (:AcAtom {run: $run}) RETURN 1 AS w"
	two := "USE two CREATE (:AcAtom {run: $run}) RETURN 1 AS w"
	for _, tt := range []struct {
		name, query, body string
		refused           bool
		one               int64
	}{
		{"doit", "USE comp CALL apoc.cypher.doIt($body, {run: $run}) YIELD value RETURN value.w AS w", one, false, 1},
		{"run_many", "USE comp CALL apoc.cypher.runMany($body, {run: $run}, {statistics: false}) YIELD row, result RETURN 1 AS w", one, false, 1},
		{"persisted_write", "USE comp CALL acatom.usewrite($run) YIELD w RETURN w", "", false, 1},
		{"read_run", "USE comp CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w", one, false, 1},
		{"nested_doit", "USE comp CALL { CALL apoc.cypher.doIt($body, {run: $run}) YIELD value RETURN value.w AS w } RETURN w", one, false, 1},
		{"nested_persisted_write", "USE comp CALL { CALL acatom.usewrite($run) YIELD w RETURN w } RETURN w", "", false, 1},
		{"doit_then_other", "USE comp CALL apoc.cypher.doIt($body, {run: $run}) YIELD value CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN z", one, true, 0},
		{"run_many_two_databases", "USE comp CALL apoc.cypher.runMany($body, {run: $run}, {statistics: false}) YIELD row, result RETURN 1 AS w", one + ";" + two, true, 0},
		{"persisted_write_then_other", "USE comp CALL acatom.usewrite($run) YIELD w CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN w, z", "", true, 0},
		{"nested_doit_then_other", "USE comp CALL { CALL apoc.cypher.doIt($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN w, z", one, true, 0},
	} {
		for _, session := range []string{"composite", "default"} {
			t.Run(tt.name+"/"+session, func(t *testing.T) {
				run := fmt.Sprintf("composite-procedure-%s-%s-%d", tt.name, session, time.Now().UnixNano())
				exec := fixture.exec
				if session == "composite" {
					exec = fixture.compositeSession(t)
				}
				_, err := exec.Execute(ctx, tt.query, map[string]interface{}{"run": run, "body": tt.body})
				if tt.refused {
					require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tt.one, fixture.writes(t, "comp.one", run), "AcAtom nodes on comp.one")
				require.Equal(t, int64(0), fixture.writes(t, "comp.two", run), "AcAtom nodes on comp.two")
			})
		}
	}
}

// A write that changes no AcAtom node is still the write of its database: a
// SET on an existing node, a hidden node (kp.suppress) in a subquery.
func TestFabricWriteWithoutNewNodesClaimsItsDatabase(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	for name, fragment := range map[string]string{
		"set_existing":          "MATCH (n:AcSeed {run: $run}) SET n.v = 1 RETURN 1 AS w",
		"suppress_in_subquery":  "CALL { MATCH (n:AcSeed {run: $run}) CALL kp.suppress(n) RETURN count(*) AS c } RETURN c AS w",
		"remove_label_existing": "MATCH (n:AcSeed {run: $run}) REMOVE n:AcSeed RETURN 1 AS w",
	} {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/other_before=%v", name, before), func(t *testing.T) {
				run := fmt.Sprintf("seed-%s-%v-%d", name, before, time.Now().UnixNano())
				ctx := context.Background()
				_, err := fixture.exec.Execute(ctx, "USE comp.one CREATE (:AcSeed {run: $run})", map[string]interface{}{"run": run})
				require.NoError(t, err)
				one := "CALL { USE comp.one " + fragment + " } "
				two := "CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } "
				query := "USE comp " + one + two + "RETURN w, z"
				if before {
					query = "USE comp " + two + one + "RETURN w, z"
				}
				_, err = fixture.exec.Execute(ctx, query, map[string]interface{}{"run": run})
				require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
				seed, err := fixture.exec.Execute(ctx, "USE comp.one MATCH (n:AcSeed {run: $run}) RETURN count(n) AS c, max(n.v) AS v", map[string]interface{}{"run": run})
				require.NoError(t, err)
				require.Equal(t, []interface{}{int64(1), nil}, seed.Rows[0], "the node on comp.one is unchanged and visible")
				require.Equal(t, int64(0), fixture.writes(t, "comp.two", run), "AcAtom nodes left on comp.two")
			})
		}
	}
}

// A fragment whose graph is chosen per row (USE graph.byName) claims the
// graph it writes when it runs: writing two databases is refused and leaves
// nothing.
func TestFabricDynamicGraphWriteClaimsItsDatabase(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	for name, query := range map[string]string{
		"by_name_then_other":  "USE comp CALL { USE graph.byName('comp.one') CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN z",
		"other_then_by_name":  "USE comp CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } CALL { WITH z USE graph.byName('comp.one') CREATE (:AcAtom {run: $run}) RETURN 1 AS w } RETURN w",
		"per_row_two_graphs":  "USE comp UNWIND ['comp.one', 'comp.two'] AS g CALL { WITH g USE graph.byName(g) CREATE (:AcAtom {run: $run}) RETURN 1 AS w } RETURN w",
		"per_row_dynamic":     "USE comp UNWIND ['comp.one', 'comp.two'] AS g CALL { WITH g USE graph.byName(g) CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w } RETURN w",
		"per_row_one_graph":   "USE comp UNWIND ['comp.one', 'comp.one'] AS g CALL { WITH g USE graph.byName(g) CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN z",
		"per_row_procedure":   "USE comp UNWIND ['comp.one', 'comp.two'] AS g CALL { WITH g USE graph.byName(g) CALL acatom.write($run) YIELD w RETURN w } RETURN w",
		"by_name_in_subquery": "USE comp CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } CALL { WITH z USE graph.byName('comp.one') CALL { CREATE (:AcAtom {run: $run}) } RETURN 1 AS w } RETURN w",
	} {
		t.Run(name, func(t *testing.T) {
			run := fmt.Sprintf("by-name-%s-%d", name, time.Now().UnixNano())
			_, err := fixture.exec.Execute(context.Background(), query, fabricEffectParams(run))
			require.ErrorContains(t, err, "Writing to more than one database per transaction is not allowed")
			require.Equal(t, int64(0), fixture.writes(t, "comp.one", run), "AcAtom nodes left on comp.one")
			require.Equal(t, int64(0), fixture.writes(t, "comp.two", run), "AcAtom nodes left on comp.two")
		})
	}
}

// On a session whose database is the composite, a statement a procedure
// runs selects the database it writes with USE: that write is the
// statement's write. Two of them on different databases are refused and
// leave nothing; one is kept only when the whole statement succeeds.
func TestFabricStatementUseSelectsTheWriteDatabase(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	ctx := context.Background()
	for _, tt := range []struct {
		name, query string
		d           int64
		refused     string // the error, "any" for any, or "" for success
		one, two    int64
	}{
		{"use_comp_two_dynamic_uses", "USE comp CALL apoc.cypher.run($usetwo, {run: $run}) YIELD value WITH value CALL apoc.cypher.run($useone, {run: $run}) YIELD value AS v RETURN 1 AS x", 1, "Writing to more than one database per transaction is not allowed", 0, 0},
		{"use_comp_one_dynamic_use", "USE comp CALL apoc.cypher.run($usetwo, {run: $run}) YIELD value RETURN value.w AS w", 1, "", 0, 1},
		{"use_comp_one_dynamic_use_then_error", "USE comp CALL apoc.cypher.run($usetwo, {run: $run}) YIELD value RETURN value.w / $d AS w", 0, "any", 0, 0},
		{"use_comp_database_then_constituent", "USE comp CALL apoc.cypher.run($plainone, {run: $run}) YIELD value WITH value CALL apoc.cypher.run($usetwo, {run: $run}) YIELD value AS v RETURN 1 AS x", 1, "Writing to more than one database per transaction is not allowed", 0, 0},
		{"use_comp_runner_and_write", "USE comp CALL acatom.runner($useone, $run) YIELD w CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN w, z", 1, "Writing to more than one database per transaction is not allowed", 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "use_comp_runner_and_write" {
				fixture.createRunner(t)
			}
			run := fmt.Sprintf("use-%s-%d", tt.name, time.Now().UnixNano())
			params := fabricEffectParams(run)
			params["d"] = tt.d
			_, err := fixture.compositeSession(t).Execute(ctx, tt.query, params)
			switch tt.refused {
			case "":
				require.NoError(t, err)
			case "any":
				require.Error(t, err)
			default:
				require.ErrorContains(t, err, tt.refused)
			}
			require.Equal(t, tt.one, fixture.writes(t, "comp.one", run), "AcAtom nodes left on comp.one")
			require.Equal(t, tt.two, fixture.writes(t, "comp.two", run), "AcAtom nodes left on comp.two")
		})
	}
}

// A write that receives no row never runs and claims nothing, as Neo4j
// opens a write transaction on a graph when a write fragment runs there: the
// statement succeeds with what its writes that ran wrote.
func TestFabricWriteThatReceivesNoRowsClaimsNothing(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	for _, tt := range []struct {
		name, query string
		one         int64
	}{
		{"second_write_gets_no_rows", "USE comp CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.one MATCH (m:AcMissing) RETURN m.x AS k } CALL { WITH k USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN z", 1},
		{"no_write_gets_rows", "USE comp CALL { USE comp.one MATCH (m:AcMissing) RETURN m.x AS k } CALL { WITH k USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN w, z", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := fmt.Sprintf("no-rows-%s-%d", tt.name, time.Now().UnixNano())
			_, err := fixture.exec.Execute(context.Background(), tt.query, map[string]interface{}{"run": run})
			require.NoError(t, err)
			require.Equal(t, tt.one, fixture.writes(t, "comp.one", run), "AcAtom nodes on comp.one")
			require.Equal(t, int64(0), fixture.writes(t, "comp.two", run), "AcAtom nodes on comp.two")
		})
	}
}

// A read that looks like a write (a string literal, a comment, property or
// map key names, a backticked variable, a parameter named like a clause, a
// READ procedure) claims nothing: with a write on the other database the
// statement succeeds.
func TestFabricReadLookalikeClaimsNothing(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	for name, fragment := range map[string]string{
		"string_literal": "MATCH (n:AcAtom) WHERE n.run = 'CREATE SET DELETE MERGE REMOVE' RETURN count(n) AS w",
		"comment":        "MATCH (n:AcAtom) /* SET n.x = 1 */ RETURN count(n) AS w",
		"property_names": "MATCH (n:AcAtom) RETURN count(n.create) + count(n.set) AS w",
		"map_key":        "RETURN size(keys({create: 1, delete: 2})) AS w",
		"backtick_var":   "MATCH (`create`:AcAtom) RETURN count(`create`) AS w",
		"read_procedure": "CALL db.labels() YIELD label RETURN count(label) AS w",
		"string_call":    "RETURN 'CALL acatom.write($run)' AS w",
		"param_named":    "RETURN $set AS w",
	} {
		t.Run(name, func(t *testing.T) {
			run := fmt.Sprintf("lookalike-%s-%d", name, time.Now().UnixNano())
			query := "USE comp CALL { USE comp.one " + fragment + " } CALL { WITH w USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS z } RETURN w, z"
			_, err := fixture.exec.Execute(context.Background(), query, fabricEffectParams(run))
			require.NoError(t, err)
			require.Equal(t, int64(1), fixture.writes(t, "comp.two", run), "AcAtom nodes on comp.two")
		})
	}
}

// Concurrent autocommit statements, each writing one database (by a clause or
// dynamic Cypher) and reading the other, or trying to write both: each keeps
// exactly its own writes, and the ones that write both are refused.
func TestFabricConcurrentStatementsKeepTheirOwnWrites(t *testing.T) {
	fixture := newFabricAtomicityFixture(t)
	const n = 48
	var wg sync.WaitGroup
	errs := make([]error, n)
	runs := make([]string, n)
	queries := []string{
		"USE comp CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { WITH w USE comp.two MATCH (n:AcAtom) RETURN count(n) AS r } RETURN w, r",
		"USE comp CALL { USE comp.one MATCH (n:AcAtom) RETURN count(n) AS r } CALL { WITH r USE comp.two CREATE (:AcAtom {run: $run}) RETURN 1 AS w } RETURN w, r",
		"USE comp CALL { USE comp.one CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { WITH w USE comp.two MATCH (n:AcAtom) RETURN count(n) AS r } RETURN w, r",
		"USE comp CALL { USE comp.one CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS w } CALL { WITH w USE comp.two CALL apoc.cypher.run($body, {run: $run}) YIELD value RETURN value.w AS z } RETURN w, z",
	}
	for i := 0; i < n; i++ {
		runs[i] = fmt.Sprintf("concurrent-%d-%d", i, time.Now().UnixNano())
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = fixture.exec.Execute(context.Background(), queries[i%4], fabricEffectParams(runs[i]))
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		got := [2]int64{fixture.writes(t, "comp.one", runs[i]), fixture.writes(t, "comp.two", runs[i])}
		switch i % 4 {
		case 0, 2:
			require.NoError(t, errs[i], i)
			require.Equal(t, [2]int64{1, 0}, got, i)
		case 1:
			require.NoError(t, errs[i], i)
			require.Equal(t, [2]int64{0, 1}, got, i)
		case 3:
			require.ErrorContains(t, errs[i], "Writing to more than one database per transaction is not allowed", i)
			require.Equal(t, [2]int64{0, 0}, got, i)
		}
	}
}
