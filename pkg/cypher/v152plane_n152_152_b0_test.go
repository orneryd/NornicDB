// NOR-85 / D18 reproduction: keep USE separate from an importing WITH in a
// native CALL body.
//
// Reported outcome: in the neo4j session fixture,
//
//	WITH 1 AS v CALL { WITH v USE neo4j RETURN v AS x } RETURN x
//
// must return [[1]]; current release reports "query could not be planned as a
// clause pipeline" on both parsers (the older report saw an undefined-variable
// error naming USE). The acceptance is that "WITH v USE neo4j" is recognized
// as two clauses and that execution and header facts agree.
//
// Contract basis. This shape is an intentional NornicDB extension, not Neo4j
// community syntax: the shared grammar (pkg/cypher/antlr/CypherParser.g4,
// subqueryBody) only admits USE as the leading clause of a CALL body, while
// pkg/fabric/planner.go (parseLeadingWithUse, SingleGraphStatement) documents
// and implements "a CALL body's leading USE, after the body's importing WITH
// too". The composite route already honors it end to end
// (TestNOR85ImportingWithUSECompositeRoute and fabric_participants_test.go
// "match_control", fabric_call_rows_test.go), and the plain-database route
// does not. Whether the Neo4j 5.26 reference accepts this shape is NOT claimed
// here; that question belongs to the NOR-85 parent decision.
//
// These tests are reproductions only. The native-session subtests that assert
// the required product result (the exact D18 statement, its quote/dynamic
// variants, the clause boundary, and the unknown-graph error) fail on the
// current source with the planning error; the controls that already hold (no
// USE, leading USE, a quoted variable or graph named USE, an undefined
// variable) pass in the same run. The fixture keeps the original report's
// session/catalog setup: a session on database "neo4j" served by a real
// multidb.DatabaseManager, so USE resolution and cross-database identity run
// through the manager rather than a stub.
package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// nor85Reproduction is the exact D18 statement.
const nor85Reproduction = "WITH 1 AS v CALL { WITH v USE neo4j RETURN v AS x } RETURN x"

// newNOR85NativeSession is the reported "neo4j session" fixture: a real
// DatabaseManager whose default (session) database is "neo4j", a second user
// database "other", and one marker per graph so a statement that runs on the
// wrong graph returns a distinguishable row. The executor is the public one
// installed on the session database; its USE resolution is the real manager.
func newNOR85NativeSession(t *testing.T) *StorageExecutor {
	t.Helper()
	manager := newCompositeNativeManager(t, multidb.NewConfigFromDefaultDatabase("neo4j"))
	adapter := &testDatabaseManagerAdapter{manager: manager}
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "other"}))
	for database, value := range map[string]string{"neo4j": "session", "other": "other"} {
		store, err := manager.GetStorage(database)
		require.NoError(t, err, database)
		seed := NewStorageExecutor(store)
		seed.SetDatabaseManager(adapter)
		_, err = seed.Execute(t.Context(), "CREATE (:NOR85Marker {value: $value})", map[string]interface{}{"value": value})
		require.NoError(t, err, database)
	}
	session, err := manager.GetStorage("neo4j")
	require.NoError(t, err)
	exec := NewStorageExecutor(session)
	exec.SetDatabaseManager(adapter)
	return exec
}

// newNOR85CompositeSession is the composite member of the same contract: a
// session on composite "cmp" with constituent "sh" over database "shard".
func newNOR85CompositeSession(t *testing.T) *StorageExecutor {
	t.Helper()
	manager := newCompositeNativeManager(t, nil)
	adapter := &testDatabaseManagerAdapter{manager: manager}
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "shard"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "cmp", []multidb.ConstituentRef{
		{Alias: "sh", DatabaseName: "shard", Type: "local", AccessMode: "read_write"},
	}))
	shard, err := manager.GetStorage("shard")
	require.NoError(t, err)
	seed := NewStorageExecutor(shard)
	seed.SetDatabaseManager(adapter)
	_, err = seed.Execute(t.Context(), "CREATE (:NOR85Marker {value: 'shard'})", nil)
	require.NoError(t, err)
	session, err := manager.GetStorage("cmp")
	require.NoError(t, err)
	exec := NewStorageExecutor(session)
	exec.SetDatabaseManager(adapter)
	return exec
}

// requireNOR85Rows asserts the statement's header facts (the declared result
// columns, read before any execution) and its execution columns and rows
// agree on exactly the required values.
func requireNOR85Rows(t *testing.T, exec *StorageExecutor, query string, columns []string, rows [][]interface{}) {
	t.Helper()
	header, err := exec.StatementResultColumns(query, RawSource)
	require.NoError(t, err, "header facts: %s", query)
	require.Equal(t, columns, header, "header facts: %s", query)
	result, err := exec.Execute(t.Context(), query, nil)
	require.NoError(t, err, query)
	require.NotNil(t, result, query)
	require.Equal(t, columns, result.Columns, query)
	require.Equal(t, rows, result.Rows, query)
}

// requireNOR85Error asserts the statement fails with the given status code and
// that its message retains the required wording. The result is returned, not
// required nil: a CALL body's USE resolution failure already reports an empty
// result beside the error in the leading-USE position (the controls below), so
// nil-on-error is not part of this contract.
func requireNOR85Error(t *testing.T, exec *StorageExecutor, query, code, message string) (*ExecuteResult, error) {
	t.Helper()
	result, err := exec.Execute(t.Context(), query, nil)
	require.Error(t, err, query)
	requireStatusCode(t, err, code)
	require.Contains(t, err.Error(), message, query)
	return result, err
}

// TestNOR85ImportingWithUSEOnNativeSession is the D18 reproduction on the
// reported session fixture. The statements that need the importing WITH and
// the USE clause read as two clauses are asserted as the required behavior;
// on the current source each one fails with the planning error while the
// controls pass, so this test is the reproduction, not a green gate.
func TestNOR85ImportingWithUSEOnNativeSession(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newNOR85NativeSession(t)

		t.Run("reproduction", func(t *testing.T) {
			// Header and plan facts already describe the statement; execution
			// must agree with them instead of failing to plan it.
			header, err := exec.StatementResultColumns(nor85Reproduction, RawSource)
			require.NoError(t, err)
			require.Equal(t, []string{"x"}, header)
			explained, err := exec.Execute(t.Context(), "EXPLAIN "+nor85Reproduction, nil)
			require.NoError(t, err, "EXPLAIN must plan the statement execution runs")
			require.Equal(t, []string{"x"}, explained.Columns)
			require.Empty(t, explained.Rows)
			requireNOR85Rows(t, exec, nor85Reproduction, []string{"x"}, [][]interface{}{{int64(1)}})
		})

		// The same clause shape without an outer import: the importer is the
		// body's own WITH, and USE still starts a second clause.
		t.Run("self contained importing with", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"CALL { WITH 1 AS v USE neo4j RETURN v AS x } RETURN x",
				[]string{"x"}, [][]interface{}{{int64(1)}})
		})

		// The boundary is the end of the projection list, not the first name:
		// both projected atoms must import and USE must still follow them.
		t.Run("multi atom importing with", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS v CALL { WITH v, 2 AS w USE neo4j RETURN v + w AS x } RETURN x",
				[]string{"x"}, [][]interface{}{{int64(3)}})
		})

		// The two clauses must both take effect: v is the imported value and
		// the remainder runs on the graph USE names (its marker, not the
		// session graph's).
		t.Run("boundary routes the remainder", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS v CALL { WITH v USE other MATCH (m:NOR85Marker) RETURN v AS x, m.value AS source } RETURN x, source",
				[]string{"x", "source"}, [][]interface{}{{int64(1), "other"}})
		})

		// A graph reference is read through backticks, like any other name.
		t.Run("quoted database name", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS v CALL { WITH v USE `neo4j` RETURN v AS x } RETURN x",
				[]string{"x"}, [][]interface{}{{int64(1)}})
		})

		// A real variable named USE (quoted) is an import, never the clause.
		t.Run("quoted variable named use", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS `USE` CALL { WITH `USE` USE neo4j RETURN `USE` AS x } RETURN x",
				[]string{"x"}, [][]interface{}{{int64(1)}})
		})

		// A dynamic reference keeps its own policy: on a session that is not a
		// composite database the answer is the dynamic-lookup rejection a
		// leading USE already gives, not a planning error.
		t.Run("dynamic string reference keeps its contract", func(t *testing.T) {
			requireNOR85Error(t, exec,
				"WITH 1 AS v CALL { WITH v USE graph.byName(\"neo4j\") RETURN v AS x } RETURN x",
				"Neo.ClientError.Statement.SyntaxError", "Dynamic graph lookup not allowed here")
		})

		// An unknown graph is a resolution failure, as a leading USE in a CALL
		// body already reports it, not a planning error.
		t.Run("unknown database", func(t *testing.T) {
			requireNOR85Error(t, exec,
				"WITH 1 AS v CALL { WITH v USE nosuch RETURN v AS x } RETURN x",
				"Neo.ClientError.Database.DatabaseNotFound", "Graph not found: nosuch")
		})

		t.Run("control without use", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS v CALL { WITH v RETURN v AS x } RETURN x",
				[]string{"x"}, [][]interface{}{{int64(1)}})
		})

		// USE's other admitted position in a CALL body (the grammar's leading
		// clause) must keep working.
		t.Run("control leading use", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"CALL { USE neo4j WITH 1 AS v RETURN v AS x } RETURN x",
				[]string{"x"}, [][]interface{}{{int64(1)}})
		})

		// The required dynamic-reference and unknown-graph answers above are
		// the ones this leading position already gives; these two controls
		// keep them pinned while the importing-WITH position is corrected.
		t.Run("control leading use dynamic reference", func(t *testing.T) {
			requireNOR85Error(t, exec,
				"CALL { USE graph.byName(\"neo4j\") RETURN 1 AS x } RETURN x",
				"Neo.ClientError.Statement.SyntaxError", "Dynamic graph lookup not allowed here")
		})

		t.Run("control leading use unknown database", func(t *testing.T) {
			requireNOR85Error(t, exec,
				"CALL { USE nosuch RETURN 1 AS x } RETURN x",
				"Neo.ClientError.Database.DatabaseNotFound", "Graph not found: nosuch")
		})

		// The split must not be reached by ignoring names: a name the body
		// never binds is still an undefined variable in execution and header.
		t.Run("control missing variable", func(t *testing.T) {
			const query = "WITH 1 AS v CALL { WITH v USE neo4j RETURN missing AS x } RETURN x"
			result, err := requireNOR85Error(t, exec, query,
				"Neo.ClientError.Statement.SyntaxError", "Variable `missing` not defined")
			require.Nil(t, result)
			_, headerErr := exec.StatementResultColumns(query, RawSource)
			require.EqualError(t, headerErr, err.Error(), "header and execution facts agree on the error")
		})
	})
}

// TestNOR85ImportingWithUSECompositeRoute is the established supported route
// for the same clause shape: on a composite session the Fabric planner reads
// the importing WITH and the USE clause separately and runs the remainder on
// the constituent (pkg/fabric/planner.go parseLeadingWithUse). These controls
// pass on the current source and must keep passing; they are the contract the
// native route is required to match.
func TestNOR85ImportingWithUSECompositeRoute(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newNOR85CompositeSession(t)

		t.Run("static constituent", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS k CALL { WITH k USE cmp.sh MATCH (m:NOR85Marker) RETURN k AS x, m.value AS source } RETURN x, source",
				[]string{"x", "source"}, [][]interface{}{{int64(1), "shard"}})
		})

		t.Run("quoted constituent", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS k CALL { WITH k USE `cmp`.`sh` RETURN k AS x } RETURN x",
				[]string{"x"}, [][]interface{}{{int64(1)}})
		})

		t.Run("dynamic string reference", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS k CALL { WITH k USE graph.byName(\"cmp.sh\") MATCH (m:NOR85Marker) RETURN k AS x, m.value AS source } RETURN x, source",
				[]string{"x", "source"}, [][]interface{}{{int64(1), "shard"}})
		})

		t.Run("quoted variable named use", func(t *testing.T) {
			requireNOR85Rows(t, exec,
				"WITH 1 AS `USE` CALL { WITH `USE` USE cmp.sh RETURN `USE` AS x } RETURN x",
				[]string{"x"}, [][]interface{}{{int64(1)}})
		})

		// The composite planner's unknown-target answer keeps its wording. It
		// is a plan error without a Bolt status class on the current source
		// (fabric_execution_integration_test.go asserts the same), so only the
		// message is pinned here.
		t.Run("unknown constituent", func(t *testing.T) {
			_, err := exec.Execute(t.Context(),
				"WITH 1 AS k CALL { WITH k USE cmp.nosuch RETURN k AS x } RETURN x", nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid USE target 'cmp.nosuch'")
		})

		t.Run("missing variable", func(t *testing.T) {
			requireNOR85Error(t, exec,
				"WITH 1 AS k CALL { WITH k USE cmp.sh RETURN missing AS x } RETURN x",
				"Neo.ClientError.Statement.SyntaxError", "Variable `missing` not defined")
		})
	})
}
