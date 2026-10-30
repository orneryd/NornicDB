package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompositeAdministrationConsumesCompleteInput(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		for _, command := range []string{
			"CREATE COMPOSITE DATABASE complete_input IF NOT EXISTS invalid_suffix",
			"ALTER COMPOSITE DATABASE complete_input DROP ALIAS kept invalid_suffix",
		} {
			t.Run(parser+"/"+command, func(t *testing.T) {
				previous := config.GetParserType()
				config.SetParserType(parser)
				t.Cleanup(func() { config.SetParserType(previous) })
				manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, manager.Close()) })
				refs := []multidb.ConstituentRef{{Alias: "kept", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"}}
				require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "complete_input", refs))
				engine, err := manager.GetStorage("nornic")
				require.NoError(t, err)
				executor := NewStorageExecutor(engine)
				executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
				_, err = executor.Execute(t.Context(), command, nil)
				assert.Error(t, err, "invalid trailing source must not be accepted")
				if err != nil {
					assert.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
				}
				actual, readErr := manager.GetCompositeConstituents("complete_input")
				require.NoError(t, readErr)
				assert.Equal(t, refs, actual, "invalid administration input must preserve the existing alias")
				_, err = executor.Execute(t.Context(), "CREATE COMPOSITE DATABASE complete_input IF NOT EXISTS ALIAS ignored FOR DATABASE missing_runtime_target", nil)
				require.NoError(t, err, "a syntactically valid no-op must not validate an unused target")
				actual, err = manager.GetCompositeConstituents("complete_input")
				require.NoError(t, err)
				require.Equal(t, refs, actual)
			})
		}
	}
}

func TestStandaloneAliasDropConsumesCompleteInput(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		for _, exists := range []bool{true, false} {
			t.Run(parser+"/"+map[bool]string{true: "existing", false: "missing"}[exists], func(t *testing.T) {
				previous := config.GetParserType()
				config.SetParserType(parser)
				t.Cleanup(func() { config.SetParserType(previous) })
				manager := newAliasTestDatabaseManager(t)
				require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "drop_target"}))
				if exists {
					require.NoError(t, manager.CreateAlias(t.Context(), "kept", "drop_target"))
				}
				before := manager.ListAliases("drop_target")
				executor := NewStorageExecutor(newTestMemoryEngine(t))
				executor.SetDatabaseManager(manager)
				_, err := executor.Execute(t.Context(), "DROP ALIAS kept IF EXISTS FOR DATABASE AT 'http://example.invalid'", nil)
				assert.Error(t, err, "trailing options must not bypass complete input admission")
				if err != nil {
					assert.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
				}
				assert.Equal(t, before, manager.ListAliases("drop_target"), "rejected input must preserve aliases")
				_, err = executor.Execute(t.Context(), "DROP ALIAS kept IF EXISTS FOR DATABASE", nil)
				require.NoError(t, err)
				require.Empty(t, manager.ListAliases("drop_target"))
			})
		}
	}
}
