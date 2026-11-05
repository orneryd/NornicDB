package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-211: EXPLAIN of a database-creation administration command returned no
// result and the error "Cannot establish the statement scope and result
// columns". An administration command declares its result schema in its
// execute owner, and the system database runs administration commands, so
// EXPLAIN must return that command's schema with no rows and leave the
// catalog untouched — as it already does for CREATE COMPOSITE DATABASE.
//
// The parser is process-wide, so each parser's run is its own process.
func TestExplainDatabaseCreationCommandsNOR211(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {

		for _, route := range []struct{ database, name string }{
			// The system database is where NOR-211 was reproduced.
			{"system", "nor211_standard"},
			// The same statement, and so the same result-metadata owner, on a
			// user database.
			{"nornic", "nor211_standard"},
		} {
			t.Run(route.database, func(t *testing.T) {
				ctx := t.Context()
				manager := newCompositeNativeManager(t, nil)
				t.Cleanup(func() { require.NoError(t, manager.Close()) })
				exec := newCompositeTestExecutor(t, manager, route.database, &testDatabaseManagerAdapter{manager: manager})

				// A command that does run changes the catalog this test reads,
				// so "no mutation" below is not vacuous.
				_, err := exec.Execute(ctx, "CREATE DATABASE "+route.name+"_control", nil)
				require.NoError(t, err)
				require.True(t, manager.Exists(route.name+"_control"), "the fixture catalog records a creation")

				for _, command := range []string{
					"CREATE DATABASE " + route.name,
					"CREATE DATABASE " + route.name + " IF NOT EXISTS",
					"CREATE OR REPLACE DATABASE " + route.name + "_control",
					"CREATE OR   REPLACE DATABASE " + route.name + "_control",
					"CREATE COMPOSITE DATABASE " + route.name + "_composite ALIAS r FOR DATABASE nornic",
				} {
					t.Run(command, func(t *testing.T) {
						before := catalogCreateDatabaseSnapshot(manager)
						result, err := exec.Execute(ctx, "EXPLAIN "+command, nil)
						require.NoError(t, err)
						require.NotNil(t, result)
						require.Equal(t, []string{"name"}, result.Columns, "EXPLAIN declares the command's result schema")
						require.Empty(t, result.Rows, "EXPLAIN runs nothing")
						require.Equal(t, "EXPLAIN", result.Metadata["planType"], "EXPLAIN returns a plan")
						require.Equal(t, before, catalogCreateDatabaseSnapshot(manager), "EXPLAIN must not create or replace a database")
					})
				}
			})
		}
	})
}

// ExecuteResult columns are caller-owned; mutating one command result must not
// change subsequent result-metadata declarations.
func TestNOR211DatabaseColumnsAreCallerOwned(t *testing.T) {
	exec, _, _ := newCreateDatabaseRegressionExecutor(t)
	result, err := exec.Execute(t.Context(), "CREATE DATABASE nor211_column_one", nil)
	require.NoError(t, err)
	original := append([]string(nil), result.Columns...)
	t.Cleanup(func() { copy(result.Columns, original) })
	require.Equal(t, []string{"name"}, result.Columns)
	result.Columns[0] = "caller_changed"
	next, err := exec.Execute(t.Context(), "CREATE DATABASE nor211_column_two", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"name"}, next.Columns)
}
