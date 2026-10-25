package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This uses the production Fabric adapter built by Execute, including its
// correlated record bindings. A header-only adapter cannot prove this contract.
func TestDirectNameFabricRecordIdentity(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	if err != nil {
		require.NoError(t, base.Close())
	}
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, manager)
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "direct_leaf"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "direct_cmp", []multidb.ConstituentRef{{Alias: "src", DatabaseName: "direct_leaf", Type: "local", AccessMode: "read_write"}}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := context.Background()
	_, err = exec.Execute(ctx, "USE direct_leaf CREATE (:DirectFabric {k:1,v:7})", nil)
	require.NoError(t, err)
	for _, name := range []string{"binding", "key", " ", "a`b", "`a`"} {
		for _, quotedBinding := range []bool{false, true} {
			for _, quotedReference := range []bool{false, true} {
				t.Run(name+map[bool]string{false: "/plain_binding", true: "/quoted_binding"}[quotedBinding]+map[bool]string{false: "/plain_reference", true: "/quoted_reference"}[quotedReference], func(t *testing.T) {
					declaration, reference := name, name
					if quotedBinding || !cyphertext.IsIdentifier(name) {
						declaration = cyphertext.QuoteName(name)
					}
					if quotedReference || !cyphertext.IsIdentifier(name) {
						reference = cyphertext.QuoteName(name)
					}
					query := "USE direct_cmp WITH 1 AS " + declaration + " CALL { WITH " + declaration + " USE direct_cmp.src MATCH (n:DirectFabric) WHERE n.k=" + reference + " RETURN n.v AS value } RETURN value"
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					require.Equal(t, [][]any{{int64(7)}}, result.Rows)
				})
			}
		}
	}
	t.Run("multiple_internal_names", func(t *testing.T) {
		first, second := cyphertext.QuoteName("a`b"), cyphertext.QuoteName(" ")
		query := "USE direct_cmp WITH 1 AS " + first + ", 1 AS " + second + " CALL { WITH " + first + ", " + second + " USE direct_cmp.src MATCH (n:DirectFabric) WHERE n.k=" + first + " AND n.k=" + second + " RETURN n.v AS value } RETURN value"
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(7)}}, result.Rows)
	})

}
