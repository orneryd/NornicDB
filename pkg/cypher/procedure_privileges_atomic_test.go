// Author: Codex, NOR-180 private rollback context signature migration,29October2026.
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The privilege batches' mutations: the second selected operation fails in the
// real staged transaction, after the first has been staged. The shared
// storage fault fixture owns the selection and the staged view.
func TestProcedurePrivilegeBatchFailureChangesNothing(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "grant", true: "revoke"}[revoke], func(t *testing.T) {
			ctx := context.Background()
			system := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
			store := auth.NewProcedurePrivilegeStore(system)
			query := "GRANT EXECUTE PROCEDURE db.a, db.b ON DBMS TO viewer"
			operation := storagefault.MutationCreate
			if revoke {
				require.NoError(t, store.Grant(ctx, "viewer", "db.a", false, false))
				require.NoError(t, store.Grant(ctx, "viewer", "db.b", false, false))
				query = "REVOKE EXECUTE PROCEDURE db.a, db.b ON DBMS FROM viewer"
				operation = storagefault.MutationDelete
			}
			before, err := store.List(ctx)
			require.NoError(t, err)
			faults := storagefault.NewMutationFailureEngine(system, operation, errSystemStorage, 2, auth.WithStoreTransaction)
			exec := NewStorageExecutor(faults)
			_, err = exec.Execute(ctx, query, nil)
			require.ErrorIs(t, err, errSystemStorage)
			after, err := store.List(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestImmutableUpgradeFailureChangesNothing(t *testing.T) {
	ctx := WithAuthenticationDisabled(context.Background())
	system := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
	grants := auth.NewProcedurePrivilegeStore(system)
	require.NoError(t, grants.Grant(ctx, "viewer", "db.a", false, false))
	require.NoError(t, grants.Grant(ctx, "viewer", "db.b", false, false))
	before, err := grants.List(ctx)
	require.NoError(t, err)
	faults := storagefault.NewMutationFailureEngine(system, storagefault.MutationUpdate, errSystemStorage, 2, auth.WithStoreTransaction)
	exec := NewStorageExecutor(faults)
	_, err = exec.Execute(ctx, "GRANT IMMUTABLE EXECUTE PROCEDURE db.a, db.b ON DBMS TO viewer", nil)
	require.ErrorIs(t, err, errSystemStorage)
	after, err := grants.List(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRoleDeletionJoinsExplicitTransaction(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "commit"}[commit], func(t *testing.T) {
			ctx := context.Background()
			system := storage.NewNamespacedEngine(newTestMemoryEngine(t), "system")
			require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, "ops"))
			grants := auth.NewProcedurePrivilegeStore(system)
			require.NoError(t, grants.Grant(ctx, "ops", "db.a", true, true))
			exec := NewStorageExecutor(system)
			_, err := exec.handleBegin()
			require.NoError(t, err)
			view := &transactionStorageWrapper{tx: exec.txContext.tx.(*storage.BadgerTransaction), underlying: system, namespace: "system", separator: ":"}
			roles := auth.NewRoleStore(view)
			require.NoError(t, roles.Load(ctx))
			require.NoError(t, roles.DeleteRole(ctx, "ops"))
			_, err = view.GetNode("role:ops")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = system.GetNode("role:ops")
			require.NoError(t, err)
			if commit {
				_, err = exec.handleCommit(context.Background())
			} else {
				_, err = exec.handleRollback(context.Background(), nil)
			}
			require.NoError(t, err)
			_, err = system.GetNode("role:ops")
			if commit {
				require.ErrorIs(t, err, storage.ErrNotFound)
			} else {
				require.NoError(t, err)
			}
			listed, err := grants.List(ctx)
			require.NoError(t, err)
			if commit {
				require.Empty(t, listed)
			} else {
				require.Len(t, listed, 1)
			}
		})
	}
}
