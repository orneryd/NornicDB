package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type privilegeBatchFailure struct {
	storage.Engine
	create, deletes int
	revoke          bool
}

func (e *privilegeBatchFailure) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return auth.WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		staged := *e
		staged.Engine = view
		return mutate(&staged)
	})
}

func (e *privilegeBatchFailure) CreateNode(node *storage.Node) (storage.NodeID, error) {
	e.create++
	if !e.revoke && e.create == 2 {
		return "", errSystemStorage
	}
	return e.Engine.CreateNode(node)
}
func (e *privilegeBatchFailure) DeleteNode(id storage.NodeID) error {
	e.deletes++
	if e.revoke && e.deletes == 2 {
		return errSystemStorage
	}
	return e.Engine.DeleteNode(id)
}

func TestProcedurePrivilegeBatchFailureChangesNothing(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(map[bool]string{false: "grant", true: "revoke"}[revoke], func(t *testing.T) {
			ctx := context.Background()
			inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = inner.Close() })
			system := storage.NewNamespacedEngine(inner, "system")
			store := auth.NewProcedurePrivilegeStore(system)
			query := "GRANT EXECUTE PROCEDURE db.a, db.b ON DBMS TO viewer"
			if revoke {
				require.NoError(t, store.Grant(ctx, "viewer", "db.a", false, false))
				require.NoError(t, store.Grant(ctx, "viewer", "db.b", false, false))
				query = "REVOKE EXECUTE PROCEDURE db.a, db.b ON DBMS FROM viewer"
			}
			before, err := store.List(ctx)
			require.NoError(t, err)
			exec := NewStorageExecutor(&privilegeBatchFailure{Engine: system, revoke: revoke})
			_, err = exec.Execute(ctx, query, nil)
			require.ErrorIs(t, err, errSystemStorage)
			after, err := store.List(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestRoleDeletionJoinsExplicitTransaction(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "commit"}[commit], func(t *testing.T) {
			ctx := context.Background()
			inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = inner.Close() })
			system := storage.NewNamespacedEngine(inner, "system")
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
				_, err = exec.handleCommit()
			} else {
				_, err = exec.handleRollback()
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
