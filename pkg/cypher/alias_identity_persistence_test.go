package cypher

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNativeAliasQuotedIdentityPersists(t *testing.T) {
	previous := config.GetParserType()
	config.SetParserType("nornic")
	t.Cleanup(func() { config.SetParserType(previous) })
	for _, operation := range []string{"create", "drop"} {
		t.Run(operation, func(t *testing.T) {
			dir := t.TempDir()
			open := func() (*multidb.DatabaseManager, *StorageExecutor) {
				base, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
				require.NoError(t, err)
				manager, err := multidb.NewDatabaseManager(t.Context(), base, multidb.DefaultConfig())
				require.NoError(t, err)
				engine, err := manager.GetStorage("nornic")
				require.NoError(t, err)
				exec := NewStorageExecutor(engine)
				exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
				return manager, exec
			}
			manager, exec := open()
			t.Cleanup(func() { _ = manager.Close() })
			require.NoError(t, manager.CreateAlias(t.Context(), "kept", "nornic"))
			want := map[string]string{"kept": "nornic"}
			for _, name := range []struct{ raw, quoted string }{
				{"r`one", "`r``one`"}, {"r,one", "`r,one`"}, {"dot.name", "`dot.name`"}, {"ordinary", "`ordinary`"},
			} {
				query := "CREATE ALIAS " + name.quoted + " FOR DATABASE `nornic`"
				if operation == "drop" {
					require.NoError(t, manager.CreateAlias(t.Context(), name.raw, "nornic"))
					query = "DROP ALIAS " + name.quoted
				} else {
					want[name.raw] = "nornic"
				}
				result, err := exec.Execute(t.Context(), query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{name.raw}}, result.Rows)
				require.Equal(t, want, manager.ListAliases(""))
			}
			// Invalid whitespace must not turn a different name into kept.
			_, err := exec.Execute(t.Context(), "DROP ALIAS `ke pt`", nil)
			require.Error(t, err)
			require.Equal(t, want, manager.ListAliases(""))
			require.NoError(t, manager.Close())
			manager, _ = open()
			require.Equal(t, want, manager.ListAliases(""), "catalog identity survives native reopen")
		})
	}
}
