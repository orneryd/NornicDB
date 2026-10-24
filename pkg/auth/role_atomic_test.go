package auth

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

var errRoleMutation = errors.New("injected role mutation failure")

type roleFailureEngine struct {
	storage.Engine
	grantDeletes    int
	failGrantDelete int
	failRoleDelete  bool
}

func (e *roleFailureEngine) DeleteNode(id storage.NodeID) error {
	if strings.HasPrefix(string(id), "proc_priv:") {
		e.grantDeletes++
		if e.grantDeletes == e.failGrantDelete {
			return errRoleMutation
		}
	}
	if e.failRoleDelete && id == "role:ops" {
		return errRoleMutation
	}
	return e.Engine.DeleteNode(id)
}

func (e *roleFailureEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		return mutate(&roleFailureView{Engine: view, failure: e})
	})
}

type roleFailureView struct {
	storage.Engine
	failure *roleFailureEngine
}

func (v *roleFailureView) DeleteNode(id storage.NodeID) error {
	original := v.failure.Engine
	v.failure.Engine = v.Engine
	defer func() { v.failure.Engine = original }()
	return v.failure.DeleteNode(id)
}

func TestDeleteRoleFailureChangesNothing(t *testing.T) {
	for _, stage := range []string{"second grant delete", "role delete"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			inner := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			base := storage.NewNamespacedEngine(inner, "system")
			t.Cleanup(func() { _ = base.Close() })
			engine := &roleFailureEngine{Engine: base}
			roles := NewRoleStore(engine)
			require.NoError(t, roles.CreateRole(ctx, "ops"))
			privileges := NewProcedurePrivilegeStore(engine)
			require.NoError(t, privileges.Grant(ctx, "ops", "db.a", true, false))
			require.NoError(t, privileges.Grant(ctx, "ops", "db.b", false, true))
			before, err := privileges.List(ctx)
			require.NoError(t, err)
			if stage == "second grant delete" {
				engine.failGrantDelete = 2
			} else {
				engine.failRoleDelete = true
			}
			require.ErrorIs(t, roles.DeleteRole(ctx, "ops"), errRoleMutation)
			require.True(t, roles.Exists("ops"))
			_, err = base.GetNode("role:ops")
			require.NoError(t, err)
			after, err := privileges.List(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after, "failed deletion must preserve every grant")
		})
	}
}
