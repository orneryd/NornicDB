package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestCompositeSourceParticipantLifetime(t *testing.T) {
	for _, terminal := range []string{"COMMIT", "ROLLBACK", "second writer"} {
		t.Run(terminal, func(t *testing.T) {
			_, manager := newCompositeIdentityExecutor(t, false)
			require.NoError(t, manager.CreateAlias("two_alias", "two"))
			require.NoError(t, manager.AddConstituent("comp", multidb.ConstituentRef{Alias: "mirror", DatabaseName: "two_alias", Type: "local", AccessMode: "read_write"}))
			engine, err := manager.GetStorage("comp")
			require.NoError(t, err)
			executor := NewStorageExecutor(engine)
			executor.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ctx := context.Background()
			_, err = executor.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
			result, err := executor.Execute(ctx, compositeIdentityQueryPrefix+"SET m.changed=42 RETURN m.changed", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(42)}}, result.Rows)
			result, err = executor.Execute(ctx, "USE comp CALL { USE comp.mirror MATCH (m:P) RETURN m } SET m.changed=43 RETURN m.changed", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(43)}}, result.Rows)
			result, err = executor.Execute(ctx, compositeIdentityQueryPrefix+"RETURN m.changed", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(43)}}, result.Rows)
			transaction := executor.txContext.tx.(*fabric.FabricTransaction)
			require.Len(t, transaction.Participants(), 2, "the logical scope and duplicate alias are not participants")
			writer, err := transaction.GetOrOpenLocation(&fabric.LocationLocal{DBName: "two"}, false)
			require.NoError(t, err)
			require.Equal(t, writer.ShardName, transaction.WriteShard())
			if terminal == "second writer" {
				_, err = executor.Execute(ctx, compositeIdentityQueryPrefix+"SET n.changed=99 RETURN n.changed", nil)
				require.ErrorIs(t, err, fabric.ErrSecondWriteShard)
				_, err = executor.Execute(ctx, "COMMIT", nil)
				require.Error(t, err)
				_, _ = executor.Execute(ctx, "ROLLBACK", nil)
			} else {
				_, err = executor.Execute(ctx, terminal, nil)
				require.NoError(t, err)
			}
			for _, database := range []string{"one", "two"} {
				store, err := manager.GetStorage(database)
				require.NoError(t, err)
				node, err := store.GetNode("same")
				require.NoError(t, err)
				var expected interface{}
				if terminal == "COMMIT" && database == "two" {
					expected = int64(43)
				}
				require.Equal(t, expected, node.Properties["changed"])
			}
		})
	}
}

func TestCompositeSourcePermissionPrecedesMutation(t *testing.T) {
	executor, manager := newCompositeIdentityExecutor(t, false)
	ctx := WithDatabasePermissionResolver(context.Background(), "comp", func(database, permission string) bool {
		return database != "two" || permission != "write"
	})
	_, err := executor.Execute(ctx, compositeIdentityQueryPrefix+"SET m.changed=42 RETURN m.changed", nil)
	requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
	for _, database := range []string{"one", "two"} {
		store, err := manager.GetStorage(database)
		require.NoError(t, err)
		node, err := store.GetNode("same")
		require.NoError(t, err)
		require.Nil(t, node.Properties["changed"])
	}
}
