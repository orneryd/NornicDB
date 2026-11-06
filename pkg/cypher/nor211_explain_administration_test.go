package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// NOR-211: EXPLAIN of a database-creation administration command returned no
// result and the error "Cannot establish the statement scope and result
// columns". An administration command declares its result schema in its
// execute owner, and the system database runs administration commands, so
// EXPLAIN must return that command's declared (empty) schema with no rows and
// leave the catalog untouched — as it already does for CREATE COMPOSITE
// DATABASE.
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
						require.Equal(t, []string{}, result.Columns, "EXPLAIN declares the command's empty result schema")
						require.Empty(t, result.Rows, "EXPLAIN runs nothing")
						require.Equal(t, "EXPLAIN", result.Metadata["planType"], "EXPLAIN returns a plan")
						require.Equal(t, before, catalogCreateDatabaseSnapshot(manager), "EXPLAIN must not create or replace a database")
					})
				}
			})
		}
	})
}

// Neo4j's database-creation commands return no columns and no records. The
// created identity reaches transports through the typed AdministrationOutcome,
// so every accepted form keeps that empty result schema while still creating,
// replacing or skipping the catalog change the command admits.
func TestDatabaseCreationOutputContractNOR211(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {

		t.Run("standard", func(t *testing.T) {
			exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
			ctx := t.Context()
			require.NoError(t, manager.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "nor211_existing"}))

			for _, step := range []struct {
				command string
				name    string
				// created is the identity the command reports, empty when it
				// creates nothing.
				created string
			}{
				{"CREATE DATABASE nor211_new", "nor211_new", "nor211_new"},
				{"CREATE DATABASE nor211_new IF NOT EXISTS", "nor211_new", ""},
				{"CREATE OR REPLACE DATABASE nor211_existing", "nor211_existing", "nor211_existing"},
				{"CREATE OR REPLACE DATABASE nor211_replace_new", "nor211_replace_new", "nor211_replace_new"},
			} {
				t.Run(step.command, func(t *testing.T) {
					result, err := exec.Execute(ctx, step.command, nil)
					require.NoError(t, err)
					require.Equal(t, []string{}, result.Columns, "CREATE DATABASE declares no result columns")
					require.Empty(t, result.Rows, "CREATE DATABASE returns no records")
					require.True(t, manager.Exists(step.name), step.command)
					if step.created == "" {
						require.Nil(t, result.Administration, "a no-op creates nothing")
						return
					}
					require.NotNil(t, result.Administration, step.command)
					require.Equal(t, step.created, result.Administration.CreatedDatabase, step.command)
				})
			}
		})

		t.Run("composite inline alias", func(t *testing.T) {
			manager := newCompositeNativeManager(t, nil)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			exec := newCompositeTestExecutor(t, manager, "nornic", &testDatabaseManagerAdapter{manager: manager})

			result, err := exec.Execute(t.Context(), "CREATE COMPOSITE DATABASE nor211_composite ALIAS r FOR DATABASE nornic", nil)
			require.NoError(t, err)
			require.Equal(t, []string{}, result.Columns, "CREATE COMPOSITE DATABASE declares no result columns")
			require.Empty(t, result.Rows, "CREATE COMPOSITE DATABASE returns no records")
			require.NotNil(t, result.Administration)
			require.Equal(t, "nor211_composite", result.Administration.CreatedDatabase)
			require.True(t, manager.IsCompositeDatabase("nor211_composite"))

			refs, err := manager.GetCompositeConstituents("nor211_composite")
			require.NoError(t, err)
			require.Len(t, refs, 1, "the inline ALIAS extension still adds its constituent")
		})
	})
}
