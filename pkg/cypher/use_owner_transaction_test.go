package cypher

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUseOwnerExplicitTransaction(t *testing.T) {
	for _, internal := range []bool{false, true} {
		for _, selector := range []string{"USE cmp.a", "USE GRAPH cmp.a"} {
			name := "public/" + selector
			if internal {
				name = "internal/" + selector
			}
			t.Run(name, func(t *testing.T) {
				base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, manager.Close()) })
				require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "shard_a"}))
				require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "cmp", []multidb.ConstituentRef{
					{Alias: "a", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
				}))
				store, err := manager.GetStorage("cmp")
				require.NoError(t, err)
				exec := NewStorageExecutor(store)
				exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
				ctx := context.Background()
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() {
					if exec.txContext != nil {
						_, err := exec.Execute(ctx, "ROLLBACK", nil)
						require.NoError(t, err)
					}
				})
				tx := exec.txContext.tx.(*fabric.FabricTransaction)
				run := func(query string) (*ExecuteResult, error) {
					if internal {
						return exec.executeInTransaction(ctx, query, upperASCII(query))
					}
					return exec.Execute(ctx, query, nil)
				}
				result, err := run("RETURN 'CREATE (:Probe) USE GRAPH cmp.a' AS template")
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"CREATE (:Probe) USE GRAPH cmp.a"}}, result.Rows)
				// The real write must work even after source text mentioning another
				// apparent target. Only resolved participants may reserve a writer.
				result, err = run(selector + " CREATE (:Probe) RETURN 1 AS written")
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				require.Equal(t, "local:shard_a", tx.WriteShard())
				require.NotContains(t, tx.Participants(), "GRAPH")
				require.NotContains(t, tx.Participants(), "cmp.a")
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
				shard, err := manager.GetStorage("shard_a")
				require.NoError(t, err)
				nodes, err := shard.GetNodesByLabel("Probe")
				require.NoError(t, err)
				require.Empty(t, nodes)
			})
		}
	}
}
