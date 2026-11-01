package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newCreateDatabaseRegressionExecutor is one catalog the test inspects both
// through the public executor and through the manager it changes.
func newCreateDatabaseRegressionExecutor(t *testing.T) (*StorageExecutor, *multidb.DatabaseManager, *testDatabaseManagerAdapter) {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	adapter := &testDatabaseManagerAdapter{manager: manager}
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetDatabaseManager(adapter)
	return exec, manager, adapter
}

// catalogCreateDatabaseSnapshot records names, creation metadata and options.
func catalogCreateDatabaseSnapshot(manager *multidb.DatabaseManager) map[string]string {
	snapshot := make(map[string]string)
	for _, db := range manager.ListDatabases() {
		snapshot[db.Name] = db.Name + "|" + db.CreatedAt.UTC().String() + "|" + db.Status + "|" + db.Type
		if provider, ok := interface{}(manager).(DatabaseOptionsManager); ok {
			options, err := provider.DatabaseOptions(db.Name)
			if err == nil {
				for key, value := range options {
					snapshot[db.Name+"/"+key] = value
				}
			}
		}
	}
	return snapshot
}

// TestCreateDatabaseFlexibleWhitespaceOrReplace covers the flexible keyword
// whitespace both parsers accept for CREATE OR REPLACE DATABASE. The keyword
// extent is the source's, not the canonical spelling's, so the name reader
// must start after the OR   REPLACE text actually present.
func TestCreateDatabaseFlexibleWhitespaceOrReplace(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			for _, tc := range []struct {
				name  string
				query string
			}{
				{"single spaces", "CREATE OR REPLACE DATABASE ab"},
				{"three spaces", "CREATE OR   REPLACE DATABASE ab"},
				{"tabs", "CREATE OR\tREPLACE\tDATABASE ab"},
				{"newlines", "CREATE OR\nREPLACE\nDATABASE ab"},
				{"leading spaces", "CREATE   OR   REPLACE   DATABASE   ab"},
				{"quoted name", "CREATE OR   REPLACE DATABASE `ab`"},
				{"space before database", "CREATE OR REPLACE    DATABASE ab"},
				{"lower case", "create or   replace database ab"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
					require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "ab"}))
					store, err := manager.GetStorage("ab")
					require.NoError(t, err)
					_, err = store.CreateNode(&storage.Node{ID: "marker", Properties: map[string]interface{}{"value": "old"}})
					require.NoError(t, err)

					result, err := exec.Execute(t.Context(), tc.query, nil)
					require.NoError(t, err)
					require.Equal(t, []string{"name"}, result.Columns)
					require.Equal(t, [][]interface{}{{"ab"}}, result.Rows)
					require.NotNil(t, result.Administration)
					require.Equal(t, "ab", result.Administration.CreatedDatabase)
					require.True(t, manager.Exists("ab"))

					// Replacement drops the pre-existing database, data included.
					replaced, err := manager.GetStorage("ab")
					require.NoError(t, err)
					_, err = replaced.GetNode("marker")
					require.Error(t, err, "CREATE OR REPLACE must replace the existing database")
				})
			}
		})
	}
}

// TestCreateDatabaseOrReplaceOptionsRejected preserves the OPTIONS owner: only
// the regular CREATE DATABASE form sets initial options.
func TestCreateDatabaseOrReplaceOptionsRejected(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			for _, query := range []string{
				"CREATE OR REPLACE DATABASE ab OPTIONS {txLogEnrichment: 'FULL'}",
				"CREATE OR   REPLACE DATABASE ab OPTIONS {txLogEnrichment: 'FULL'}",
			} {
				t.Run(query, func(t *testing.T) {
					exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
					require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "ab"}))
					store, err := manager.GetStorage("ab")
					require.NoError(t, err)
					_, err = store.CreateNode(&storage.Node{ID: "marker", Properties: map[string]interface{}{"value": "old"}})
					require.NoError(t, err)
					before := catalogCreateDatabaseSnapshot(manager)

					_, err = exec.Execute(t.Context(), query, nil)
					require.Error(t, err)
					requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminDatabaseNameEmpty,
						"invalid CREATE DATABASE syntax: database name cannot be empty")
					require.Equal(t, before, catalogCreateDatabaseSnapshot(manager))
					marker, err := store.GetNode("marker")
					require.NoError(t, err)
					require.Equal(t, "old", marker.Properties["value"])
				})
			}
		})
	}
}

// TestCreateDatabaseIfNotExistsSuffixRejected covers the malformed statement
// the public native path accepted as a no-op: IF NOT EXISTS followed by
// anything else is invalid syntax, and the invalid statement leaves every
// recorded catalog metadata untouched.
func TestCreateDatabaseIfNotExistsSuffixRejected(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, existing := range []bool{false, true} {
				state := "missing target"
				if existing {
					state = "existing target"
				}
				t.Run(state, func(t *testing.T) {
					for _, query := range []string{
						"CREATE DATABASE ab IF NOT EXISTS extra",
						"CREATE DATABASE ab IF NOT EXISTS extra IF NOT EXISTS",
						"CREATE DATABASE `ab` IF   NOT   EXISTS trailing",
					} {
						t.Run(query, func(t *testing.T) {
							exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
							if existing {
								require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "ab"}))
							}
							require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "keeper"}))
							store, err := manager.GetStorage("keeper")
							require.NoError(t, err)
							_, err = store.CreateNode(&storage.Node{ID: "marker", Properties: map[string]interface{}{"value": "untouched"}})
							require.NoError(t, err)
							before := catalogCreateDatabaseSnapshot(manager)
							_, err = exec.Execute(t.Context(), query, nil)
							require.Error(t, err, "a trailing clause after IF NOT EXISTS is invalid syntax")
							require.Equal(t, before, catalogCreateDatabaseSnapshot(manager))
							require.Equal(t, existing, manager.Exists("ab"))
							marker, err := store.GetNode("marker")
							require.NoError(t, err)
							require.Equal(t, "untouched", marker.Properties["value"])
						})
					}
				})
			}
		})
	}
}

// TestCreateDatabaseControls pins the behaviours the unification must keep.
func TestCreateDatabaseControls(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			t.Run("plain create", func(t *testing.T) {
				exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
				result, err := exec.Execute(t.Context(), "CREATE DATABASE fresh", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"fresh"}}, result.Rows)
				require.Equal(t, "fresh", result.Administration.CreatedDatabase)
				require.True(t, manager.Exists("fresh"))
			})

			t.Run("parallel create", func(t *testing.T) {
				exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
				result, err := exec.Execute(t.Context(), "CREATE   DATABASE   fresh", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"fresh"}}, result.Rows)
				require.True(t, manager.Exists("fresh"))
			})

			t.Run("existing name", func(t *testing.T) {
				exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
				require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "ab"}))
				before := catalogCreateDatabaseSnapshot(manager)
				_, err := exec.Execute(t.Context(), "CREATE DATABASE ab", nil)
				requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminDatabaseAlreadyExists, "database 'ab' already exists")
				require.Equal(t, before, catalogCreateDatabaseSnapshot(manager))
			})

			t.Run("if not exists no-op", func(t *testing.T) {
				exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
				require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "ab"}))
				store, err := manager.GetStorage("ab")
				require.NoError(t, err)
				_, err = store.CreateNode(&storage.Node{ID: "marker", Properties: map[string]interface{}{"value": "old"}})
				require.NoError(t, err)
				before := catalogCreateDatabaseSnapshot(manager)

				result, err := exec.Execute(t.Context(), "CREATE DATABASE ab IF NOT EXISTS", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"ab"}}, result.Rows)
				require.Nil(t, result.Administration, "IF NOT EXISTS on an existing name creates nothing")
				require.Equal(t, before, catalogCreateDatabaseSnapshot(manager))
				marker, err := store.GetNode("marker")
				require.NoError(t, err)
				require.Equal(t, "old", marker.Properties["value"])
			})

			t.Run("if not exists flexible", func(t *testing.T) {
				exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
				require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "ab"}))
				result, err := exec.Execute(t.Context(), "CREATE DATABASE ab IF\tNOT\tEXISTS", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"ab"}}, result.Rows)
			})

			t.Run("if not exists creates", func(t *testing.T) {
				exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
				result, err := exec.Execute(t.Context(), "CREATE DATABASE fresh IF NOT EXISTS", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"fresh"}}, result.Rows)
				require.True(t, manager.Exists("fresh"))
			})

			t.Run("options", func(t *testing.T) {
				exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
				result, err := exec.Execute(t.Context(), "CREATE DATABASE fresh2 IF NOT EXISTS OPTIONS {txLogEnrichment: 'full'}", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"fresh2"}}, result.Rows)
				provider, ok := interface{}(manager).(DatabaseOptionsManager)
				require.True(t, ok)
				options, err := provider.DatabaseOptions("fresh2")
				require.NoError(t, err)
				require.Equal(t, map[string]string{"txLogEnrichment": "FULL"}, options)
			})

			t.Run("missing manager", func(t *testing.T) {
				exec := NewStorageExecutor(newTestMemoryEngine(t))
				for _, query := range []string{"CREATE DATABASE ab", "CREATE OR REPLACE DATABASE ab"} {
					_, err := exec.Execute(t.Context(), query, nil)
					requireCypherAdminLocalizedError(t, err, localization.MessageCypherAdminDatabaseManagerUnavailable,
						"database manager not available - CREATE DATABASE requires multi-database support")
				}
			})

		})
	}
}

// TestCreateDatabaseInvalidStatementsPreserveCatalog covers the remaining
// malformed forms: each reports its admission diagnostic and changes nothing.
func TestCreateDatabaseInvalidStatementsPreserveCatalog(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })

			for _, tc := range []struct {
				query string
				id    localization.MessageID
			}{
				{"CREATE OR   REPLACE DATABASE", localization.MessageCypherAdminDatabaseNameEmpty},
				{"CREATE DATABASE `broken`name`", localization.MessageCypherTransactionsSyntaxUnclosedQuote},
				{"CREATE OR   REPLACE DATABASE `broken`name`", localization.MessageCypherTransactionsSyntaxUnclosedQuote},
				{"CREATE DATABASE ab extra", localization.MessageCypherAdminInvalidDatabaseName},
			} {
				t.Run(tc.query, func(t *testing.T) {
					exec, manager, _ := newCreateDatabaseRegressionExecutor(t)
					require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "ab"}))
					before := catalogCreateDatabaseSnapshot(manager)

					_, err := exec.Execute(t.Context(), tc.query, nil)
					require.Error(t, err)
					var localized *localization.LocalizedError
					require.ErrorAs(t, err, &localized)
					// The administration owner reads every CREATE [OR REPLACE]
					// DATABASE form on either parser, as the default parser did.
					require.Equal(t, tc.id, localized.Message.ID, tc.query)
					require.Equal(t, before, catalogCreateDatabaseSnapshot(manager))
				})
			}
		})
	}
}
