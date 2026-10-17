package auth

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type userChangeFault struct {
	storage.Engine
	kind     string
	disabled bool
}

func (e *userChangeFault) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		return mutate(&userChangeFault{Engine: view, kind: e.kind, disabled: e.disabled})
	})
}

func (e *userChangeFault) UpdateNode(node *storage.Node) error {
	if e.kind == "roles" || e.kind == "node:"+string(node.ID) || (e.kind == "status" && node.Properties["disabled"] == e.disabled) {
		return errRoleMutation
	}
	return e.Engine.UpdateNode(node)
}

func combinedUserFixture(t *testing.T) (*Authenticator, *userChangeFault) {
	t.Helper()
	base := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "system")
	t.Cleanup(func() { _ = base.Close() })
	fault := &userChangeFault{Engine: base}
	config := DefaultAuthConfig()
	config.BcryptCost = 4
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	config.DefaultAdminUsername = ""
	users, err := NewAuthenticator(config, fault)
	require.NoError(t, err)
	_, err = users.CreateUser("worker", "password123", []Role{RoleViewer})
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
				fault.kind = kind
			} else {
				fault.kind, fault.disabled = "status", disabled
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
	require.NoError(t, users.saveUser(user))
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

// Several users' roles change together: a failure writing a later user
// leaves the earlier one as it was, stored and cached, with no audit event;
// a missing user changes no one.
func TestModifyUsersRolesAllOrNothing(t *testing.T) {
	users, fault := combinedUserFixture(t)
	ctx := context.Background()
	_, err := users.CreateUser("second", "password123", []Role{RoleViewer})
	require.NoError(t, err)
	before := cloneStoredUser(users.users["worker"])
	beforeNode, err := fault.Engine.GetNode("user:worker")
	require.NoError(t, err)
	var events []AuditEvent
	users.auditLog = func(event AuditEvent) { events = append(events, event) }
	toAdmin := func([]Role) ([]Role, bool) { return []Role{RoleAdmin}, true }

	fault.kind = "node:user:second"
	require.ErrorIs(t, users.ModifyUsersRoles(ctx, []string{"worker", "second"}, toAdmin, false), errRoleMutation)
	require.Equal(t, before, users.users["worker"])
	node, err := fault.Engine.GetNode("user:worker")
	require.NoError(t, err)
	require.Equal(t, beforeNode, node)
	require.Empty(t, events)

	fault.kind = ""
	require.ErrorIs(t, users.ModifyUsersRoles(ctx, []string{"worker", "missing"}, toAdmin, false), ErrUserNotFound)
	require.Equal(t, before, users.users["worker"])
	require.Empty(t, events)

	require.NoError(t, users.ModifyUsersRoles(ctx, []string{"worker", "missing", "second"}, toAdmin, true))
	require.Equal(t, []Role{RoleAdmin}, users.users["worker"].Roles)
	require.Equal(t, []Role{RoleAdmin}, users.users["second"].Roles)
	require.Len(t, events, 2)
}

// Two role changes of one user never lose each other: while the first is
// between reading the user's roles and committing, the second waits, and
// then changes the roles the first committed.
func TestModifyUsersRolesConcurrentChangesKeepBoth(t *testing.T) {
	users, _ := combinedUserFixture(t)
	ctx := context.Background()
	adding := func(role Role) func([]Role) ([]Role, bool) {
		return func(current []Role) ([]Role, bool) { return append(current, role), true }
	}
	paused, release := make(chan struct{}), make(chan struct{})
	first := make(chan error)
	go func() {
		first <- users.ModifyUsersRoles(ctx, []string{"worker"}, func(current []Role) ([]Role, bool) {
			close(paused)
			<-release
			return append(current, RoleEditor), true
		}, false)
	}()
	<-paused
	second := make(chan error)
	go func() { second <- users.ModifyUsersRoles(ctx, []string{"worker"}, adding(RoleAdmin), false) }()
	select {
	case err := <-second:
		t.Fatalf("the second change finished while the first was between its read and its commit: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	require.ElementsMatch(t, []Role{RoleViewer, RoleEditor, RoleAdmin}, users.users["worker"].Roles)
	stored, err := users.storage.GetNode("user:worker")
	require.NoError(t, err)
	for _, role := range []string{"viewer", "editor", "admin"} {
		require.Contains(t, stored.Properties["roles"], role)
	}
}

// A change that keeps a user's roles writes nothing and logs nothing, and a
// change naming only missing users that are skipped is no change.
func TestModifyUsersRolesNoChange(t *testing.T) {
	users, _ := combinedUserFixture(t)
	ctx := context.Background()
	before := cloneStoredUser(users.users["worker"])
	var events []AuditEvent
	users.auditLog = func(event AuditEvent) { events = append(events, event) }
	unchanged := func(current []Role) ([]Role, bool) { return current, false }
	require.NoError(t, users.ModifyUsersRoles(ctx, []string{"worker"}, unchanged, false))
	require.NoError(t, users.ModifyUsersRoles(ctx, []string{"missing"}, unchanged, true))
	require.Equal(t, before.Roles, users.users["worker"].Roles)
	require.Empty(t, events)
}
