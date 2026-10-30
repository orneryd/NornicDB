package auth

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newMutationTestAuthenticator builds the authenticator the auth mutation
// tests share: a caller-owned system namespace and no default admin, at test
// bcrypt cost.
func newMutationTestAuthenticator(t *testing.T, system storage.Engine) *Authenticator {
	t.Helper()
	config := DefaultAuthConfig()
	config.BcryptCost = 4
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	config.DefaultAdminUsername = ""
	users, err := NewAuthenticator(config, system)
	require.NoError(t, err)
	return users
}

func combinedUserFixture(t *testing.T) (*Authenticator, *mutationFault) {
	t.Helper()
	fault := &mutationFault{Engine: newSystemTestEngine(t)}
	users := newMutationTestAuthenticator(t, fault)
	_, err := users.CreateUser("worker", "password123", []Role{RoleViewer})
	require.NoError(t, err)
	return users, fault
}

func TestCombinedUserChangeFailureChangesNothing(t *testing.T) {
	for _, kind := range []string{"roles", "disable", "enable", "commit"} {
		t.Run(kind, func(t *testing.T) {
			users, fault := combinedUserFixture(t)
			disabled := kind != "enable"
			if kind == "enable" {
				require.NoError(t, users.DisableUser("worker"))
			}
			before := cloneStoredUser(users.users["worker"])
			beforeNode, err := fault.Engine.GetNode("user:worker")
			require.NoError(t, err)
			var events []AuditEvent
			users.auditLog = func(event AuditEvent) { events = append(events, event) }
			ctx := context.Background()
			if kind == "commit" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else if kind == "roles" {
				fault.rejectUpdate = func(*storage.Node) bool { return true }
			} else {
				fault.rejectUpdate = func(node *storage.Node) bool { return node.Properties["disabled"] == disabled }
			}
			err = users.UpdateRolesAndStatus(ctx, "worker", []Role{RoleAdmin}, &disabled)
			if kind == "commit" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, errRoleMutation)
			}
			require.Equal(t, before, users.users["worker"])
			node, err := fault.Engine.GetNode("user:worker")
			require.NoError(t, err)
			require.Equal(t, beforeNode, node)
			require.Empty(t, events)
		})
	}
}

func TestCombinedUserChangePublishesAfterCommit(t *testing.T) {
	users, fault := combinedUserFixture(t)
	user := users.users["worker"]
	user.Disabled, user.FailedLogins = true, 3
	user.LockedUntil = time.Now().Add(time.Hour)
	require.NoError(t, users.saveUser(user, nil))
	var events []AuditEvent
	users.auditLog = func(event AuditEvent) {
		node, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		require.Equal(t, `["admin"]`, node.Properties["roles"])
		require.Equal(t, false, node.Properties["disabled"])
		events = append(events, event)
	}
	disabled := false
	require.NoError(t, users.UpdateRolesAndStatus(context.Background(), "worker", []Role{RoleAdmin}, &disabled))
	require.Equal(t, []Role{RoleAdmin}, users.users["worker"].Roles)
	require.False(t, users.users["worker"].Disabled)
	require.Zero(t, users.users["worker"].FailedLogins)
	require.True(t, users.users["worker"].LockedUntil.IsZero())
	require.Len(t, events, 2)
}

func TestCombinedUserChangeOptionalFields(t *testing.T) {
	users, _ := combinedUserFixture(t)
	ctx := context.Background()
	require.NoError(t, users.UpdateRolesAndStatus(ctx, "missing", nil, nil))
	require.ErrorIs(t, users.UpdateRolesAndStatus(ctx, "missing", []Role{RoleAdmin}, nil), ErrUserNotFound)
	require.NoError(t, users.UpdateRolesAndStatus(ctx, "worker", []Role{RoleEditor}, nil))
	disabled := true
	require.NoError(t, users.UpdateRolesAndStatus(ctx, "worker", nil, &disabled))
	require.Equal(t, []Role{RoleEditor}, users.users["worker"].Roles)
	require.True(t, users.users["worker"].Disabled)
}
