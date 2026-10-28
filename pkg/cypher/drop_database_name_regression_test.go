package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseDropTargetAdmission(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, tc := range []struct {
				name, query string
				composite   bool
				reject      bool
			}{
				{"composite joined names", "DROP COMPOSITE DATABASE a b", true, true},
				{"composite joined tabs", "DROP COMPOSITE DATABASE a\tb", true, true},
				{"composite joined lines", "DROP COMPOSITE DATABASE a\nb", true, true},
				{"standard joined names", "DROP DATABASE a b", false, true},
				{"standard extra suffix", "DROP DATABASE ab IF EXISTS extra", false, true},
				{"standard quoted suffix", "DROP DATABASE `ab` IF EXISTS extra", false, true},
				{"composite plain", "DROP COMPOSITE DATABASE ab", true, false},
				{"composite quoted", "DROP COMPOSITE DATABASE `ab`", true, false},
				{"composite flexible spacing", "DROP\tCOMPOSITE\tDATABASE\tab", true, false},
				{"standard plain", "DROP DATABASE ab", false, false},
				{"standard quoted", "DROP DATABASE `ab`", false, false},
				{"standard if exists", "DROP DATABASE ab IF\tEXISTS", false, false},
				{"standard suffix comment", "DROP DATABASE ab IF EXISTS /* cleanup */", false, false},
				{"standard name comment", "DROP DATABASE `ab` /* cleanup */", false, false},
				{"composite name comment", "DROP COMPOSITE DATABASE ab // cleanup\n", true, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, manager.Close()) })
					adapter := &testDatabaseManagerAdapter{manager: manager}
					require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "source"}))
					if tc.composite {
						require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "ab", []interface{}{
							map[string]interface{}{"alias": "r", "database_name": "source", "type": "local", "access_mode": "read"},
						}))
					} else {
						require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "ab"}))
					}
					store, err := manager.GetStorage("source")
					require.NoError(t, err)
					_, err = store.CreateNode(&storage.Node{ID: "marker", Properties: map[string]interface{}{"value": "preserved"}})
					require.NoError(t, err)
					exec := NewStorageExecutor(store)
					exec.SetDatabaseManager(adapter)
					result, err := exec.Execute(t.Context(), tc.query, nil)
					if tc.reject {
						assert.Error(t, err, "invalid syntax must not admit a catalog deletion")
						assert.True(t, manager.Exists("ab"), "invalid DROP must preserve the existing target")
					} else {
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{"ab"}}, result.Rows)
						require.False(t, manager.Exists("ab"))
					}
					require.True(t, manager.Exists("source"), "DROP must preserve the other database")
					marker, err := store.GetNode("marker")
					require.NoError(t, err)
					require.Equal(t, "preserved", marker.Properties["value"])
				})
			}
		})
	}

}
