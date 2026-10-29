package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameAdministration(t *testing.T) {
	for _, quoted := range []bool{false, true} {
		spelling := func(name string) string {
			if quoted {
				return cyphertext.QuoteName(name)
			}
			return name
		}
		for _, operation := range []string{"create_alias", "create_alias_target", "create_composite", "drop_alias", "show_aliases", "show_constituents", "alter_drop_alias", "drop_composite"} {
			t.Run(operation+map[bool]string{false: "/plain", true: "/quoted"}[quoted], func(t *testing.T) {
				base := newTestMemoryEngine(t)
				manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
				require.NoError(t, err)
				adapter := &testDatabaseManagerAdapter{manager: manager}
				require.NoError(t, adapter.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "r5_target"}))
				exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
				exec.SetDatabaseManager(adapter)
				ctx := context.Background()
				if operation == "create_alias" || operation == "create_alias_target" {
					alias, target := spelling("r5_alias"), "r5_target"
					if operation == "create_alias_target" {
						alias, target = "r5_alias", spelling("r5_target")
					}
					result, err := exec.Execute(ctx, "CREATE ALIAS "+alias+" FOR DATABASE "+target, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"r5_alias"}}, result.Rows)
					resolved, err := adapter.ResolveDatabase("r5_alias")
					require.NoError(t, err)
					require.Equal(t, "r5_target", resolved)
					return
				}
				require.NoError(t, adapter.CreateAlias(t.Context(), "r5_alias", "r5_target"))
				switch operation {
				case "drop_alias":
					_, err := exec.Execute(ctx, "DROP ALIAS "+spelling("r5_alias"), nil)
					require.NoError(t, err)
					require.Empty(t, adapter.ListAliases("r5_target"))
				case "create_composite":
					result, err := exec.Execute(ctx, "CREATE COMPOSITE DATABASE "+spelling("r5_created")+" ALIAS r5_part FOR DATABASE r5_target", nil)
					require.NoError(t, err)
					require.Equal(t, "r5_created", result.Rows[0][0])
					require.True(t, adapter.IsCompositeDatabase("r5_created"))
				case "show_aliases":
					result, err := exec.Execute(ctx, "SHOW ALIASES FOR DATABASE "+spelling("r5_target"), nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{"r5_alias", "r5_target"}}, result.Rows)
				default:
					require.NoError(t, adapter.CreateCompositeDatabase(t.Context(), "r5_composite", []interface{}{map[string]interface{}{"alias": "r5_part", "database_name": "r5_target", "type": "local", "access_mode": "read_write"}}))
					switch operation {
					case "show_constituents":
						result, err := exec.Execute(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE "+spelling("r5_composite"), nil)
						require.NoError(t, err)
						require.Len(t, result.Rows, 1)
						require.Equal(t, "r5_part", result.Rows[0][0])
					case "alter_drop_alias":
						_, err := exec.Execute(ctx, "ALTER COMPOSITE DATABASE "+spelling("r5_composite")+" DROP ALIAS "+spelling("r5_part"), nil)
						require.NoError(t, err)
						refs, err := adapter.GetCompositeConstituents("r5_composite")
						require.NoError(t, err)
						require.Empty(t, refs)
					case "drop_composite":
						_, err := exec.Execute(ctx, "DROP COMPOSITE DATABASE "+spelling("r5_composite"), nil)
						require.NoError(t, err)
						require.False(t, adapter.Exists("r5_composite"))
					}
				}
			})
		}
	}
}
