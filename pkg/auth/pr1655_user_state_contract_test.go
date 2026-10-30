package auth

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This file proves the converged account state mutation owner
// (Authenticator.mutateStoredUser + ChangeUserState) preserves the behavior of
// the retired EnableUser, DisableUser and UnlockUser methods exactly. The
// baseline is the pre-convergence snapshot
// fresh-checker-convergence/auth-before-state-convergence.go, in which:
//
//	EnableUser:  Disabled=false, FailedLogins=0, LockedUntil=zero
//	DisableUser: Disabled=true,  FailedLogins and LockedUntil preserved
//	UnlockUser:  FailedLogins=0, LockedUntil=zero, Disabled preserved
//
// Every case asserts the full record (live cache, read-back node properties and
// a reload from storage) plus the emitted audit events, so a regression in any
// single field or in the persist/publish ordering fails.
//
// Numeric expectations are the exact canonical stored types: a committed user
// record reads back with every integer property as int64 (property_codec.go
// decodes every stored integer as int64) and every boolean as bool. The write
// boundary keeps the historical wire dialect (user_record.go writes
// failed_logins as the Go int held by User.FailedLogins and locked_until as
// int64; the compatibility fixture testdata/user_record_compatibility.json
// records exactly that), but that dialect is not observable through a storage
// read or a transaction view, both of which hand back the canonical types
// above. Every numeric value is asserted, never their presence alone, and no
// expectation accepts "either type": a production or storage-payload change
// that stores a different integer type fails these tests instead of passing.

// stateTransition is one input account state crossed with one typed transition,
// with the exact expected output state.
type stateTransition struct {
	name      string
	change    UserStateChange
	eventType string
	operation string

	// Input account state.
	disabled, locked bool
	failed           int

	// Expected output account state.
	wantDisabled bool
	wantLocked   bool
	wantFailed   int
}

// stateTransitions covers every account state field against every typed
// transition.
var stateTransitions = []stateTransition{
	{
		name: "enable from disabled locked and counted", change: UserEnable,
		eventType: "user_enable", operation: "user enable",
		disabled: true, failed: 4, locked: true,
		wantDisabled: false, wantFailed: 0, wantLocked: false,
	},
	{
		name: "enable from disabled with expired lock", change: UserEnable,
		eventType: "user_enable", operation: "user enable",
		disabled: true, failed: 2, locked: false,
		wantDisabled: false, wantFailed: 0, wantLocked: false,
	},
	{
		name: "disable from active", change: UserDisable,
		eventType: "user_disable", operation: "user disable",
		disabled: false, failed: 0, locked: false,
		wantDisabled: true, wantFailed: 0, wantLocked: false,
	},
	{
		// Disable must preserve the lockout counter and deadline: it only
		// records suspension, it does not clear or extend the lockout.
		name: "disable preserves lockout counter and deadline", change: UserDisable,
		eventType: "user_disable", operation: "user disable",
		disabled: false, failed: 3, locked: true,
		wantDisabled: true, wantFailed: 3, wantLocked: true,
	},
	{
		// Disable on an already disabled account is idempotent and keeps the
		// lockout state it finds.
		name: "disable from already disabled preserves lockout", change: UserDisable,
		eventType: "user_disable", operation: "user disable",
		disabled: true, failed: 5, locked: true,
		wantDisabled: true, wantFailed: 5, wantLocked: true,
	},
	{
		name: "unlock clears counter and deadline", change: UserUnlock,
		eventType: "user_unlock", operation: "user unlock",
		disabled: false, failed: 5, locked: true,
		wantDisabled: false, wantFailed: 0, wantLocked: false,
	},
	{
		// Unlock must not re-enable a disabled account.
		name: "unlock preserves disabled", change: UserUnlock,
		eventType: "user_unlock", operation: "user unlock",
		disabled: true, failed: 5, locked: true,
		wantDisabled: true, wantFailed: 0, wantLocked: false,
	},
	{
		name: "unlock on an active account is a no-op transition", change: UserUnlock,
		eventType: "user_unlock", operation: "user unlock",
		disabled: false, failed: 0, locked: false,
		wantDisabled: false, wantFailed: 0, wantLocked: false,
	},
}

// lockedDeadline is the lockout deadline for a locked input. It is truncated
// to whole Unix seconds so the stored int64 deadline round-trips exactly.
var lockedDeadline = time.Unix(time.Now().Add(time.Hour).Unix(), 0).UTC()

// seedStateUser forces the requested input state into both storage and the live
// cache, then returns the pre-mutation record so callers can prove nothing else
// moved. CreatedAt is the account's real creation time: saveUser preserves the
// stored creation time, so seeding a different one is not a state of any
// account the owner can be handed. UpdatedAt is seeded an hour in the past, as
// the retired owner expected, so "UpdatedAt advances" is a real ordering claim.
func seedStateUser(t *testing.T, users *Authenticator, disabled bool, failed int, locked bool) *User {
	t.Helper()
	user := users.users["worker"]
	user.Disabled, user.FailedLogins = disabled, failed
	user.LockedUntil = time.Time{}
	if locked {
		user.LockedUntil = lockedDeadline
	}
	user.LastLogin = time.Unix(time.Now().Add(-time.Minute).Unix(), 0).UTC()
	user.UpdatedAt = time.Unix(time.Now().Add(-time.Hour).Unix(), 0).UTC()
	user.Email = "worker@example.com"
	user.Roles = []Role{RoleEditor, RoleViewer}
	user.Metadata = map[string]string{"team": "ops"}
	require.NoError(t, users.saveUser(user, nil))
	return cloneStoredUser(user)
}

// assertStateOwnsOnlyItsFields proves a transition moves exactly Disabled,
// FailedLogins, LockedUntil and UpdatedAt: identity, credentials, profile,
// roles, metadata, LastLogin and CreatedAt are preserved.
func assertStateOwnsOnlyItsFields(t *testing.T, created, before, after *User, transition stateTransition) {
	t.Helper()
	require.Equal(t, transition.wantDisabled, after.Disabled)
	require.Equal(t, transition.wantFailed, after.FailedLogins)
	require.Equal(t, transition.wantLocked, !after.LockedUntil.IsZero())
	if transition.wantLocked {
		require.True(t, lockedDeadline.Equal(after.LockedUntil), "lockout deadline preserved exactly")
	}
	require.Equal(t, before.ID, after.ID)
	require.Equal(t, before.Username, after.Username)
	require.Equal(t, before.PasswordHash, after.PasswordHash)
	require.Equal(t, before.Email, after.Email)
	require.Equal(t, before.Roles, after.Roles)
	require.Equal(t, before.Metadata, after.Metadata)
	require.True(t, before.LastLogin.Equal(after.LastLogin), "LastLogin is not a state-transition field")
	require.Equal(t, created.CreatedAt.Unix(), after.CreatedAt.Unix(), "CreatedAt is not a state-transition field")
	require.True(t, after.UpdatedAt.After(before.UpdatedAt), "UpdatedAt advances")
}

// assertStoredState proves the committed node agrees with the expected state,
// with the exact canonical stored types, and still carries the untouched
// credential record. Both the storage read path and a transaction view are
// checked so the claim does not rest on one accessor.
func assertStoredState(t *testing.T, engine storage.Engine, before *User, change stateTransition) {
	t.Helper()
	wantLockedUntil := int64(0)
	if change.wantLocked {
		wantLockedUntil = lockedDeadline.Unix()
	}
	assertRecord := func(node *storage.Node) {
		t.Helper()
		require.Equal(t, change.wantDisabled, node.Properties["disabled"])
		require.IsType(t, false, node.Properties["disabled"], "disabled stored type is bool")
		require.Equal(t, int64(change.wantFailed), node.Properties["failed_logins"], "failed_logins value")
		require.IsType(t, int64(0), node.Properties["failed_logins"], "failed_logins stored type is int64")
		require.Equal(t, wantLockedUntil, node.Properties["locked_until"], "locked_until value")
		require.IsType(t, int64(0), node.Properties["locked_until"], "locked_until stored type is int64")
		require.Equal(t, before.PasswordHash, node.Properties["password_hash"])
	}

	node, err := engine.GetNode("user:worker")
	require.NoError(t, err)
	assertRecord(node)
	require.NoError(t, WithStoreTransaction(context.Background(), engine, func(view storage.Engine) error {
		viewNode, err := view.GetNode("user:worker")
		if err != nil {
			return err
		}
		assertRecord(viewNode)
		return nil
	}))
}

// recordAuditEvents replaces the audit logger and returns the captured slice.
func recordAuditEvents(users *Authenticator) *[]AuditEvent {
	events := []AuditEvent{}
	users.auditLog = func(event AuditEvent) { events = append(events, event) }
	return &events
}

// recordPasswordListeners appends a capturing listener after any existing
// ones and returns the captured calls.
func recordPasswordListeners(users *Authenticator) *[]string {
	calls := []string{}
	users.OnPasswordChange(func(username string) { calls = append(calls, username) })
	return &calls
}

// assertStateAuditEvent pins the success audit event the shared owner emits.
func assertStateAuditEvent(t *testing.T, event AuditEvent, eventType, username, userID string, notBefore time.Time) {
	t.Helper()
	require.Equal(t, eventType, event.EventType)
	require.Equal(t, username, event.Username)
	require.Equal(t, userID, event.UserID)
	require.True(t, event.Success)
	require.Empty(t, event.Details, "state transitions carry no extra detail text")
	require.Empty(t, event.IPAddress)
	require.Empty(t, event.UserAgent)
	require.Empty(t, event.RequestPath)
	require.False(t, event.Timestamp.IsZero(), "audit logger stamps the event")
	require.False(t, event.Timestamp.Before(notBefore.Add(-time.Second)), "event stamped during this change")
}

func TestChangeUserStateFieldTransitionMatrix(t *testing.T) {
	for _, transition := range stateTransitions {
		t.Run(transition.name, func(t *testing.T) {
			users, fault := combinedUserFixture(t)
			created := cloneStoredUser(users.users["worker"])
			before := seedStateUser(t, users, transition.disabled, transition.failed, transition.locked)
			events := recordAuditEvents(users)
			listenerCalls := recordPasswordListeners(users)
			changeStarted := time.Now()

			require.NoError(t, users.ChangeUserState("worker", transition.change))

			require.Len(t, *events, 1, "exactly one success audit event")
			assertStateAuditEvent(t, (*events)[0], transition.eventType, "worker", before.ID, changeStarted)
			require.Empty(t, *listenerCalls, "state transitions are not password changes")

			assertStateOwnsOnlyItsFields(t, created, before, users.users["worker"], transition)
			assertStoredState(t, fault.Engine, before, transition)

			// Reload from storage: a fresh authenticator sees the same state.
			fresh, err := NewAuthenticator(users.config, fault.Engine)
			require.NoError(t, err)
			loaded := fresh.users["worker"]
			require.NotNil(t, loaded)
			assertStateOwnsOnlyItsFields(t, created, before, loaded, transition)
		})
	}
}

// TestChangeUserStateGatesAuthentication proves the transitions have their
// product effect through the real login path, not just in stored fields. Every
// rejection is also proven to publish no success audit, leave the stored record
// untouched and change no cached credentials.
func TestChangeUserStateGatesAuthentication(t *testing.T) {
	users, fault := combinedUserFixture(t)
	users.config.MaxFailedLogins = 2
	events := recordAuditEvents(users)

	// Disable: the correct password stops authenticating, and the refusal is
	// audited as a failed login without touching the stored record.
	require.NoError(t, users.ChangeUserState("worker", UserDisable))
	beforeNode, err := fault.Engine.GetNode("user:worker")
	require.NoError(t, err)
	*events = (*events)[:0]
	_, _, err = users.Authenticate("worker", "password123", "127.0.0.1", "test")
	require.ErrorIs(t, err, ErrInvalidCredentials)
	require.Len(t, *events, 1, "the disabled refusal is audited exactly once")
	require.Equal(t, "login", (*events)[0].EventType)
	require.Equal(t, "account disabled", (*events)[0].Details)
	require.False(t, (*events)[0].Success)
	afterNode, err := fault.Engine.GetNode("user:worker")
	require.NoError(t, err)
	require.Equal(t, beforeNode, afterNode, "a disabled refusal persists nothing")

	// Unlock alone leaves the account disabled.
	require.NoError(t, users.ChangeUserState("worker", UserUnlock))
	*events = (*events)[:0]
	_, _, err = users.Authenticate("worker", "password123", "127.0.0.1", "test")
	require.ErrorIs(t, err, ErrInvalidCredentials)
	require.Len(t, *events, 1)
	require.Equal(t, "account disabled", (*events)[0].Details)
	require.False(t, (*events)[0].Success)

	// Enable re-admits the account and clears the lockout budget.
	require.NoError(t, users.ChangeUserState("worker", UserEnable))
	_, _, err = users.Authenticate("worker", "password123", "127.0.0.1", "test")
	require.NoError(t, err)

	// A lockout then blocks even the correct password, and Unlock restores
	// access without re-enabling anything.
	for i := 0; i < users.config.MaxFailedLogins; i++ {
		_, _, err = users.Authenticate("worker", "wrong-password", "127.0.0.1", "test")
		require.ErrorIs(t, err, ErrInvalidCredentials)
	}
	require.True(t, users.users["worker"].LockedUntil.After(time.Now()), "the failure budget locked the account")
	*events = (*events)[:0]
	_, _, err = users.Authenticate("worker", "password123", "127.0.0.1", "test")
	require.ErrorIs(t, err, ErrAccountLocked)
	require.Len(t, *events, 1)
	require.Equal(t, "account locked", (*events)[0].Details)
	require.False(t, (*events)[0].Success)
	require.NoError(t, users.ChangeUserState("worker", UserUnlock))
	require.False(t, users.users["worker"].Disabled, "unlock never disables")
	_, _, err = users.Authenticate("worker", "password123", "127.0.0.1", "test")
	require.NoError(t, err)
}

// TestChangeUserStateRejectsUnknownActionWithoutChanges proves the zero value
// and every unknown action are refused before any persist, cache, audit or
// listener side effect.
func TestChangeUserStateRejectsUnknownActionWithoutChanges(t *testing.T) {
	for _, change := range []UserStateChange{0, 4, 5, 255} {
		t.Run(fmt.Sprintf("action_%d", change), func(t *testing.T) {
			users, fault := combinedUserFixture(t)
			seedStateUser(t, users, true, 3, true)
			before := cloneStoredUser(users.users["worker"])
			beforeNode, err := fault.Engine.GetNode("user:worker")
			require.NoError(t, err)
			events := recordAuditEvents(users)
			listenerCalls := recordPasswordListeners(users)

			err = users.ChangeUserState("worker", change)
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("invalid user state change %d", change))
			require.NotErrorIs(t, err, ErrUserNotFound)

			require.Equal(t, before, users.users["worker"], "live cache untouched")
			afterNode, err := fault.Engine.GetNode("user:worker")
			require.NoError(t, err)
			require.Equal(t, beforeNode, afterNode, "stored record untouched")
			require.Empty(t, *events, "a rejected action audits nothing")
			require.Empty(t, *listenerCalls)
		})
	}
	// A rejected action is rejected before the username is even resolved.
	users, _ := combinedUserFixture(t)
	require.Error(t, users.ChangeUserState("missing", 0))
	require.Len(t, users.users, 1)
}

// TestChangeUserStateUnknownUser proves the shared owner keeps the original
// ErrUserNotFound result for a missing account.
func TestChangeUserStateUnknownUser(t *testing.T) {
	for _, change := range []UserStateChange{UserEnable, UserDisable, UserUnlock} {
		t.Run(fmt.Sprintf("action_%d", change), func(t *testing.T) {
			users, _ := combinedUserFixture(t)
			events := recordAuditEvents(users)
			before := cloneStoredUser(users.users["worker"])
			require.ErrorIs(t, users.ChangeUserState("missing", change), ErrUserNotFound)
			require.Empty(t, *events)
			require.Equal(t, before, users.users["worker"])
		})
	}
}

// TestChangeUserStatePersistFailureChangesNothing proves a staged write
// failure publishes nothing: no live cache change, no success audit, no
// listener, and the stored record is unchanged.
func TestChangeUserStatePersistFailureChangesNothing(t *testing.T) {
	for _, transition := range stateTransitions {
		t.Run(transition.name, func(t *testing.T) {
			users, fault := combinedUserFixture(t)
			before := seedStateUser(t, users, transition.disabled, transition.failed, transition.locked)
			beforeNode, err := fault.Engine.GetNode("user:worker")
			require.NoError(t, err)
			events := recordAuditEvents(users)
			listenerCalls := recordPasswordListeners(users)
			// Fail the staged UPDATE of the user record itself, after the
			// transaction admitted its view and read the Before image.
			fault.Operation, fault.ID = storagefault.MutationUpdate, "user:worker"

			err = users.ChangeUserState("worker", transition.change)
			require.ErrorIs(t, err, errRoleMutation)
			require.Contains(t, err.Error(), "failed to persist "+transition.operation)
			require.Equal(t, before, users.users["worker"], "live cache untouched")
			afterNode, err := fault.Engine.GetNode("user:worker")
			require.NoError(t, err)
			require.Equal(t, beforeNode, afterNode, "stored record untouched")
			require.Empty(t, *events, "no success audit on persistence failure")
			require.Empty(t, *listenerCalls)
		})
	}
}

// TestChangeUserStatePublishesAfterDurableCommit proves the ordering
// contract: when the success audit runs, the committed record is already
// durable and readable, and the live cache already holds the new state.
func TestChangeUserStatePublishesAfterDurableCommit(t *testing.T) {
	users, fault := combinedUserFixture(t)
	seedStateUser(t, users, false, 3, true)
	var observed bool
	observeStoredUserAudit(t, users, fault.Engine, func(event AuditEvent, node *storage.Node) {
		require.Equal(t, true, node.Properties["disabled"])
		require.Equal(t, int64(3), node.Properties["failed_logins"], "committed value")
		require.IsType(t, int64(0), node.Properties["failed_logins"], "committed reads are canonical int64")
		require.Equal(t, lockedDeadline.Unix(), node.Properties["locked_until"])
		require.True(t, users.users["worker"].Disabled, "live cache published before audit")
		require.Equal(t, "user_disable", event.EventType)
		observed = true
	})
	require.NoError(t, users.ChangeUserState("worker", UserDisable))
	require.True(t, observed)
}

// TestChangePasswordContractPreserved pins the shared owner's password
// behavior against the pre-convergence ChangePassword: the incorrect-password
// failure audit, the minimum-length and hash-failure rejections that publish
// nothing, and the persist -> cache -> success audit -> listener ordering with
// the listener running under mu.
func TestChangePasswordContractPreserved(t *testing.T) {
	t.Run("incorrect old password audits failure and changes nothing", func(t *testing.T) {
		users, fault := combinedUserFixture(t)
		before := cloneStoredUser(users.users["worker"])
		beforeNode, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		events := recordAuditEvents(users)
		listenerCalls := recordPasswordListeners(users)

		require.ErrorIs(t, users.ChangePassword("worker", "wrong-password", "replacement123"), ErrInvalidCredentials)
		require.Len(t, *events, 1)
		failure := (*events)[0]
		require.Equal(t, "password_change", failure.EventType)
		require.Equal(t, "worker", failure.Username)
		require.Equal(t, before.ID, failure.UserID)
		require.False(t, failure.Success)
		require.Equal(t, "old password incorrect", failure.Details)
		require.Equal(t, before, users.users["worker"])
		afterNode, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		require.Equal(t, beforeNode, afterNode)
		require.Empty(t, *listenerCalls, "a refused change forgets no cached credentials")
	})

	t.Run("minimum length rejects without publishing", func(t *testing.T) {
		users, fault := combinedUserFixture(t)
		require.Equal(t, 8, users.config.MinPasswordLength)
		before := cloneStoredUser(users.users["worker"])
		beforeNode, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		events := recordAuditEvents(users)
		listenerCalls := recordPasswordListeners(users)

		err = users.ChangePassword("worker", "password123", "short")
		require.ErrorIs(t, err, ErrPasswordTooShort)
		require.Contains(t, err.Error(), "minimum 8 characters required")
		require.Empty(t, *events, "the length rejection emits no audit event")
		require.Equal(t, before, users.users["worker"])
		afterNode, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		require.Equal(t, beforeNode, afterNode)
		require.Empty(t, *listenerCalls)
	})

	t.Run("hash rejection publishes nothing", func(t *testing.T) {
		users, fault := combinedUserFixture(t)
		// A bcrypt cost outside 4..31 makes GenerateFromPassword fail after the
		// old password verified and the length check passed.
		users.config.BcryptCost = 32
		before := cloneStoredUser(users.users["worker"])
		beforeNode, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		events := recordAuditEvents(users)
		listenerCalls := recordPasswordListeners(users)

		err = users.ChangePassword("worker", "password123", "replacement123")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to hash password")
		require.NotErrorIs(t, err, ErrInvalidCredentials)
		require.Empty(t, *events)
		require.Equal(t, before, users.users["worker"])
		afterNode, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		require.Equal(t, beforeNode, afterNode)
		require.Empty(t, *listenerCalls)
	})

	t.Run("persistence failure changes nothing and forgets nothing", func(t *testing.T) {
		users, fault := combinedUserFixture(t)
		before := cloneStoredUser(users.users["worker"])
		beforeNode, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		events := recordAuditEvents(users)
		listenerCalls := recordPasswordListeners(users)
		fault.Operation, fault.ID = storagefault.MutationUpdate, "user:worker"

		err = users.ChangePassword("worker", "password123", "replacement123")
		require.ErrorIs(t, err, errRoleMutation)
		require.Contains(t, err.Error(), "failed to persist password change")
		require.Empty(t, *events)
		require.Equal(t, before, users.users["worker"])
		afterNode, err := fault.Engine.GetNode("user:worker")
		require.NoError(t, err)
		require.Equal(t, beforeNode, afterNode)
		require.Empty(t, *listenerCalls)
	})

	t.Run("listener runs after persist and audit while mu is held", func(t *testing.T) {
		users, fault := combinedUserFixture(t)
		cache := NewBasicAuthCache(DefaultAuthCacheEntries, DefaultAuthCacheTTL)
		users.OnPasswordChange(cache.ForgetUser)
		cache.SetAtGeneration("worker", "password123", &JWTClaims{Username: "worker"}, cache.Generation())
		cache.SetAtGeneration("other", "password123", &JWTClaims{Username: "other"}, cache.Generation())

		// This scenario's subject is the password change. The logger below is
		// swapped for a fresh capture before the verification login, so only
		// this scenario's own audit events are counted here.
		var auditRuns int
		users.auditLog = func(event AuditEvent) {
			require.Equal(t, "password_change", event.EventType)
			require.True(t, event.Success)
			auditRuns++
		}
		var observed bool
		users.OnPasswordChange(func(username string) {
			require.Equal(t, "worker", username)
			// The listener runs under a.mu after durable success and audit:
			// the committed record and the live cache already agree, and the
			// success audit has already been emitted.
			node, err := fault.Engine.GetNode("user:worker")
			require.NoError(t, err)
			require.NotEmpty(t, users.users["worker"].PasswordHash)
			require.Equal(t, users.users["worker"].PasswordHash, node.Properties["password_hash"],
				"live cache and durable record agree when the listener runs")
			require.Equal(t, 1, auditRuns, "password success audit runs before password listeners")
			require.False(t, users.mu.TryLock(), "the listener runs while the authenticator is locked")
			observed = true
		})

		require.NoError(t, users.ChangePassword("worker", "password123", "replacement123"))
		require.True(t, observed)
		require.Equal(t, 1, auditRuns, "exactly one success audit event")
		_, cached := cache.Get("worker", "password123")
		require.False(t, cached, "the listener forgot this user's cached credentials")
		_, cached = cache.Get("other", "password123")
		require.True(t, cached, "another user's cached entry stays")

		// The new password authenticates through the real login path. Its own
		// success login event is captured separately rather than counted as a
		// password change.
		events := recordAuditEvents(users)
		_, _, err := users.Authenticate("worker", "replacement123", "127.0.0.1", "test")
		require.NoError(t, err)
		require.Len(t, *events, 1)
		require.Equal(t, "login", (*events)[0].EventType)
		require.True(t, (*events)[0].Success)
	})
}

// TestUpdateRolesAndStatusUsesTheAdmittedView proves the combined mutation
// keeps saving through the transaction it was handed: every user record write
// is staged in the transaction view, and the change publishes only after
// commit.
func TestUpdateRolesAndStatusUsesTheAdmittedView(t *testing.T) {
	users, fault := combinedUserFixture(t)
	seedStateUser(t, users, false, 2, true)
	before := cloneStoredUser(users.users["worker"])
	beforeNode, err := fault.Engine.GetNode("user:worker")
	require.NoError(t, err)
	events := recordAuditEvents(users)
	fault.Operation, fault.ID = storagefault.MutationUpdate, "user:worker"

	disabled := true
	err = users.UpdateRolesAndStatus(context.Background(), "worker", []Role{RoleAdmin}, &disabled)
	require.ErrorIs(t, err, errRoleMutation)
	require.Equal(t, before, users.users["worker"])
	afterNode, err := fault.Engine.GetNode("user:worker")
	require.NoError(t, err)
	require.Equal(t, beforeNode, afterNode)
	require.Empty(t, *events)

	fault.Operation, fault.ID = "", ""
	require.NoError(t, users.UpdateRolesAndStatus(context.Background(), "worker", []Role{RoleAdmin}, &disabled))
	require.Equal(t, []Role{RoleAdmin}, users.users["worker"].Roles)
	require.True(t, users.users["worker"].Disabled)
	require.Equal(t, 2, users.users["worker"].FailedLogins, "disable preserves the counter")
	require.True(t, lockedDeadline.Equal(users.users["worker"].LockedUntil), "disable preserves the deadline")
	require.Len(t, *events, 2, "role change and state change audit once each after commit")
	require.Equal(t, "role_change", (*events)[0].EventType)
	require.Equal(t, "user_disable", (*events)[1].EventType)

	// Enabling through the combined path clears the same three fields.
	enabled := false
	require.NoError(t, users.UpdateRolesAndStatus(context.Background(), "worker", nil, &enabled))
	require.False(t, users.users["worker"].Disabled)
	require.Zero(t, users.users["worker"].FailedLogins)
	require.True(t, users.users["worker"].LockedUntil.IsZero())
	require.Equal(t, []Role{RoleAdmin}, users.users["worker"].Roles, "a nil roles argument leaves roles unchanged")
}
