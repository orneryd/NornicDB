package auth

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
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
	return mustNewAuthenticator(t, config, system)
}

func combinedUserFixture(t *testing.T) (*Authenticator, *storagefault.MutationFailureEngine) {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	fault := storagefault.NewMutationFailureEngine(storage.NewNamespacedEngine(base, "system"), "", errRoleMutation, 0, WithStoreTransaction)
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
				require.NoError(t, users.ChangeUserState("worker", UserDisable))
			}
			before := cloneStoredUser(users.users["worker"])
			beforeNode, err := fault.Engine.GetNode("user:worker")
			require.NoError(t, err)
			events := recordAuditEvents(users)
			ctx := context.Background()
			if kind == "commit" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else if kind == "roles" {
				fault.RejectUpdate = func(*storage.Node) bool { return true }
			} else {
				fault.RejectUpdate = func(node *storage.Node) bool { return node.Properties["disabled"] == disabled }
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
			require.Empty(t, *events)
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
	observeStoredUserAudit(t, users, fault.Engine, func(event AuditEvent, node *storage.Node) {
		require.Equal(t, `["admin"]`, node.Properties["roles"])
		require.Equal(t, false, node.Properties["disabled"])
		events = append(events, event)
	})
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

// observeStoredUserAudit owns the committed-record read performed by success
// audit proofs. Each case retains its original seed, transition and assertions.
func observeStoredUserAudit(t *testing.T, users *Authenticator, engine storage.Engine, observe func(AuditEvent, *storage.Node)) {
	t.Helper()
	users.auditLog = func(event AuditEvent) {
		node, err := engine.GetNode("user:worker")
		require.NoError(t, err, "audit observes a durably committed record")
		observe(event, node)
	}
}
