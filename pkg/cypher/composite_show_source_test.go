package cypher

// Author: credentials lane (Codex). Public command-head contract regressions
// for #1525; the catalog and shared lexical/name owners remain dependencies.
import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestCompositeShowSourceProducts(t *testing.T) {
	t.Run("alias selection", func(t *testing.T) {
		manager := newAliasTestDatabaseManager(t)
		for _, name := range []string{"tenant-a", "tenant.b", "other"} {
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
			require.NoError(t, manager.CreateAlias(t.Context(), "alias_"+name, name))
		}
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		executor.SetDatabaseManager(manager)
		for _, query := range []string{
			"SHOW ALIASES FOR DATABASE tenant-a",
			"SHOW ALIASES FOR DATABASE `tenant-a`",
			"SHOW ALIASES FOR DATABASE `tenant.b`",
			"SHOW ALIASES FOR /* gap */ DATABASE `tenant-a`",
			"SHOW ALIASES FOR DATABASE /* name */ `tenant-a` /* end */",
			"SHOW ALIASES FOR DATABASE `tenant-a` YIELD alias, database RETURN alias, database",
		} {
			t.Run(query, func(t *testing.T) {
				result, err := executor.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				name := "tenant-a"
				if query == "SHOW ALIASES FOR DATABASE `tenant.b`" {
					name = "tenant.b"
				}
				require.Equal(t, []string{"alias", "database"}, result.Columns)
				require.Equal(t, [][]interface{}{{"alias_" + name, name}}, result.Rows)
			})
		}
		for _, query := range []string{
			"SHOW ALIASES FOR DATABASE `tenant-a` garbage",
			"SHOW ALIASES FOR DATABASES garbage",
			"SHOW ALIASES FOR DATABASE `tenant-a` /* unfinished",
		} {
			t.Run(query, func(t *testing.T) {
				_, err := executor.Execute(t.Context(), query, nil)
				require.Error(t, err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
			})
		}
	})
	t.Run("composite listings", func(t *testing.T) {
		manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, manager.Close()) })
		require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "lookup_names", []multidb.ConstituentRef{
			{Alias: "local", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
		}))
		executor := NewStorageExecutor(newTestMemoryEngine(t))
		executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
		for _, query := range []string{
			"SHOW CONSTITUENTS FOR COMPOSITE DATABASE lookup_names",
			"SHOW CONSTITUENTS FOR COMPOSITE DATABASE `lookup_names`",
			"SHOW CONSTITUENTS FOR /* gap */ COMPOSITE DATABASE /* name */ `lookup_names` /* end */",
			"SHOW CONSTITUENTS FOR COMPOSITE DATABASE `lookup_names` // comment\r YIELD alias, database RETURN alias, database",
		} {
			t.Run(query, func(t *testing.T) {
				result, err := executor.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Equal(t, "local", result.Rows[0][0])
				require.Equal(t, "nornic", result.Rows[0][1])
			})
		}
		for _, query := range []string{
			"SHOW COMPOSITE DATABASES /* unfinished",
			"SHOW CONSTITUENTS FOR COMPOSITE DATABASE lookup_names /* unfinished",
			"SHOW CONSTITUENTS FOR COMPOSITE DATABASE lookup_names garbage",
		} {
			t.Run(query, func(t *testing.T) {
				_, err := executor.Execute(t.Context(), query, nil)
				require.Error(t, err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
			})
		}
	})
}
