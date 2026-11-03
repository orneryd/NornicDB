package auth

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCreateUserRefusesCanceledAdmission proves the durable owner's admission
// ordering: a canceled caller is refused before the user is hashed, stored,
// published to the live cache or audited, so it cannot report a user that was
// never committed.
func TestCreateUserRefusesCanceledAdmission(t *testing.T) {
	users, fault := combinedUserFixture(t)
	events := recordAuditEvents(users)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	created, err := users.CreateUser(ctx, "canceled-user", "password123", []Role{RoleViewer})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, created)
	require.NotContains(t, users.users, "canceled-user", "no live user is published")
	require.Empty(t, *events, "a refused create records no audit event")

	_, err = fault.Engine.GetNode(context.Background(), "user:canceled-user")
	require.ErrorIs(t, err, storage.ErrNotFound, "no stored user record is written")

	// The same entry point still works under a live caller.
	created, err = users.CreateUser(context.Background(), "canceled-user", "password123", []Role{RoleViewer})
	require.NoError(t, err)
	require.Equal(t, "canceled-user", created.Username)
	require.Contains(t, users.users, "canceled-user")
}

// TestChangePasswordCanceledChangesNothing proves a canceled password change
// leaves the stored credential, the live cache, the listener and the audit
// trail untouched, and reports its own error instead of a password outcome.
func TestChangePasswordCanceledChangesNothing(t *testing.T) {
	users, fault := combinedUserFixture(t)
	before := cloneStoredUser(users.users["worker"])
	events := recordAuditEvents(users)
	listenerCalls := recordPasswordListeners(users)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, users.ChangePassword(ctx, "worker", "password123", "replacement123"), context.Canceled)
	require.Equal(t, before, users.users["worker"], "live credentials are unchanged")
	require.Empty(t, *events, "no password-change audit event")
	require.Empty(t, *listenerCalls, "no cached credential is forgotten")

	node, err := fault.Engine.GetNode(context.Background(), "user:worker")
	require.NoError(t, err)
	require.Equal(t, before.PasswordHash, node.Properties["password_hash"])
	require.Equal(t, int64(before.FailedLogins), node.Properties["failed_logins"])

	require.NoError(t, users.ChangePassword(context.Background(), "worker", "password123", "replacement123"))
	require.NotEqual(t, before.PasswordHash, users.users["worker"].PasswordHash)
	require.Len(t, *events, 1, "the accepted change records exactly one audit event")
	require.Equal(t, []string{"worker"}, *listenerCalls)
}

// TestDeleteUserCanceledRetainsUser proves the stored delete is the accepted
// step: a canceled caller keeps the stored record and the live user, and
// records no delete audit event.
func TestDeleteUserCanceledRetainsUser(t *testing.T) {
	users, fault := combinedUserFixture(t)
	events := recordAuditEvents(users)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, users.DeleteUser(ctx, "worker"), context.Canceled)
	require.Contains(t, users.users, "worker")
	require.Empty(t, *events)

	_, err := fault.Engine.GetNode(context.Background(), "user:worker")
	require.NoError(t, err, "the stored record is retained")
}

// TestUpdateRolesCanceledKeepsRoles proves an accepted-context-only mutation
// never publishes a role set it did not persist.
func TestUpdateRolesCanceledKeepsRoles(t *testing.T) {
	users, _ := combinedUserFixture(t)
	before := cloneStoredUser(users.users["worker"])
	events := recordAuditEvents(users)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, users.UpdateRoles(ctx, "worker", []Role{RoleAdmin}), context.Canceled)
	require.Equal(t, before.Roles, users.users["worker"].Roles)
	require.Empty(t, *events)
}

// TestAuthenticateCanceledReportsNoOutcome proves the login entry point does
// not answer with an uncommitted outcome: a canceled caller gets nil token,
// nil user and its own error, and no failed-login bookkeeping is recorded.
func TestAuthenticateCanceledReportsNoOutcome(t *testing.T) {
	users, _ := combinedUserFixture(t)
	before := cloneStoredUser(users.users["worker"])
	events := recordAuditEvents(users)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	token, user, err := users.Authenticate(ctx, "worker", "invalid-password", "127.0.0.1", "cancel-contract")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, token, "no token is issued for a refused call")
	require.Nil(t, user, "no user is returned for a refused call")
	require.Empty(t, *events, "a refused call records no login audit event")
	require.Equal(t, before.FailedLogins, users.users["worker"].FailedLogins,
		"the refused call is not a rejected credential and counts no failure")
}

// TestAuthenticateAcceptedCredentialSurvivesBookkeepingRefusal pins the
// distinction the login owner must keep: credential acceptance decides the
// login, while the last-login counters are best-effort. A storage refusal of
// that bookkeeping must not turn an accepted credential into a failed login.
func TestAuthenticateAcceptedCredentialSurvivesBookkeepingRefusal(t *testing.T) {
	users, fault := combinedUserFixture(t)
	fault.RejectUpdate = func(*storage.Node) bool { return true }

	token, user, err := users.Authenticate(context.Background(), "worker", "password123", "127.0.0.1", "bookkeeping-refusal")
	require.NoError(t, err, "an accepted credential is not failed by a bookkeeping refusal")
	require.NotNil(t, token)
	require.NotEmpty(t, token.AccessToken)
	require.NotNil(t, user)
	require.Equal(t, "worker", user.Username)
}

// TestRoleMutationRefusalKeepsPublishedState proves the role-record owner
// still converges on one transaction: a refused create publishes no live role
// cache entry and surfaces its own native error.
func TestRoleMutationRefusalKeepsPublishedState(t *testing.T) {
	_, fault := combinedUserFixture(t)
	store := NewRoleStore(fault)
	require.NoError(t, store.CreateRole(context.Background(), "auditor"))

	fault.Select(storagefault.MutationCreate, "", 0)
	require.ErrorIs(t, store.CreateRole(context.Background(), "broken"), errRoleMutation,
		"the refused staged write surfaces its own native error")
	require.False(t, store.Exists("broken"), "the live role cache does not publish a refused write")

	fault.Select("", "", 0)
	require.NoError(t, store.CreateRole(context.Background(), "reviewer"))
	require.True(t, store.Exists("reviewer"))
}
