package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
				if name == "profile_in_dynamic" && !before {
					// A statement a procedure runs is no PROFILE target: the
					// body fails on its own when it runs first.
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
