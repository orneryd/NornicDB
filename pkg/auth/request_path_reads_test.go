package auth

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// An authenticated request reads the Authenticator (CurrentClaims, GetUser),
// the role entitlements (PermissionsForRoles), the database allowlist, the
// database privileges and the role store. No writer of any of them may hold
// the lock those reads need while it waits on storage, or every authenticated
// request waits behind one administrative change (round 2 of #1768).

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type requestPathStores struct {
	users        *Authenticator
	store        *stallingUserStore
	roles        *RoleStore
	allowlist    *AllowlistStore
	privileges   *PrivilegesStore
	entitlements *RoleEntitlementsStore
}

func newRequestPathStores(t *testing.T) *requestPathStores {
	t.Helper()
	users, store := newStallingAuthenticator(t)
	stores := &requestPathStores{
		users:        users,
		store:        store,
		roles:        NewRoleStore(store),
		allowlist:    NewAllowlistStore(store),
		privileges:   NewPrivilegesStore(store),
		entitlements: NewRoleEntitlementsStore(store),
	}
	ctx := context.Background()
	require.NoError(t, stores.roles.CreateRole(ctx, "ops"))
	require.NoError(t, stores.allowlist.SaveRoleDatabases(ctx, "ops", []string{"nornic"}))
	require.NoError(t, stores.privileges.SavePrivilege(ctx, "ops", "nornic", true, false))
	require.NoError(t, stores.entitlements.SetMappings(ctx, map[string][]string{"ops": {"read"}}))
	require.NoError(t, users.UpdateRoles(ctx, "bob", []Role{"ops", RoleViewer}))
	return stores
}

func (s *requestPathStores) all() RoleMutationStores {
	return RoleMutationStores{Allowlist: s.allowlist, Privileges: s.privileges, Entitlements: s.entitlements, Users: s.users}
}

func TestRequestPathReadsDoNotWaitForAuthStorageWrites(t *testing.T) {
	writers := []struct {
		name  string
		write func(context.Context, *requestPathStores) error
	}{
		{"login", func(ctx context.Context, s *requestPathStores) error {
			_, _, err := s.users.Authenticate(ctx, "bob", "password123", "127.0.0.1", "test")
			return err
		}},
		{"change password", func(ctx context.Context, s *requestPathStores) error {
			return s.users.ChangePassword(ctx, "bob", "password123", "newPassword456")
		}},
		{"admin roles and status", func(ctx context.Context, s *requestPathStores) error {
			disabled := true
			return s.users.UpdateRolesAndStatus(ctx, "bob", []Role{RoleEditor}, &disabled)
		}},
		{"create role", func(ctx context.Context, s *requestPathStores) error {
			return s.roles.CreateRole(ctx, "auditors")
		}},
		{"role rename with every store", func(ctx context.Context, s *requestPathStores) error {
			return s.roles.RenameRoleWithStores(ctx, "ops", "operators", s.all())
		}},
		{"role delete with every store", func(ctx context.Context, s *requestPathStores) error {
			return s.roles.DeleteRoleWithStores(ctx, "ops", s.all())
		}},
		{"database allowlist", func(ctx context.Context, s *requestPathStores) error {
			return s.allowlist.SaveRoleDatabases(ctx, "ops", []string{"nornic", "other"})
		}},
		{"database privilege", func(ctx context.Context, s *requestPathStores) error {
			return s.privileges.SavePrivilege(ctx, "ops", "nornic", true, true)
		}},
		{"privilege matrix", func(ctx context.Context, s *requestPathStores) error {
			return s.privileges.PutMatrix(ctx, []struct {
				Role     string `json:"role"`
				Database string `json:"database"`
				Read     bool   `json:"read"`
				Write    bool   `json:"write"`
			}{{Role: "ops", Database: "nornic", Read: true}})
		}},
		{"role entitlements", func(ctx context.Context, s *requestPathStores) error {
			return s.entitlements.SetMappings(ctx, map[string][]string{"ops": {"read", "write"}})
		}},
	}
	for _, writer := range writers {
		t.Run(writer.name, func(t *testing.T) {
			s := newRequestPathStores(t)
			token, _, err := s.users.Authenticate(context.Background(), "carol", "password123", "127.0.0.1", "test")
			require.NoError(t, err)
			claims, err := s.users.ValidateToken(token.AccessToken)
			require.NoError(t, err)

			s.store.stall.Store(true)
			released := false
			release := func() {
				if !released {
					released = true
					close(s.store.gate)
				}
			}
			t.Cleanup(release)
			done := make(chan error, 1)
			go func() { done <- writer.write(context.Background(), s) }()
			select {
			case <-s.store.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the write never reached storage")
			}

			// Every read an authenticated request makes answers meanwhile.
			read := make(chan error, 1)
			go func() {
				current, err := s.users.CurrentClaims(claims)
				if err == nil {
					_, err = s.users.GetUser("carol")
				}
				if err == nil && !EntitlementsAllow(PermissionsForRoles(current.Roles, s.entitlements), string(PermRead)) {
					err = ErrInvalidCredentials
				}
				if err == nil {
					_ = s.allowlist.CanAccessDatabaseForRoles(current.Roles, "nornic")
					_, _ = s.privileges.Granted([]string{"ops"}, "nornic")
					_ = s.roles.Exists("ops")
				}
				read <- err
			}()
			select {
			case err := <-read:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				release()
				<-done
				t.Fatalf("an authenticated request's checks waited for the %s storage write", writer.name)
			}
			release()
			require.NoError(t, <-done)
		})
	}
}

// From the round-2 review of #1768 (gpt-6.1-sol): a role rename or delete
// with the live entitlement store must not hold back an unrelated request's
// permission check.
func TestReview1768RoleMutationDoesNotBlockRequestPermissions(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "rename"
		if remove {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			a, store := newStallingAuthenticator(t)
			roles := NewRoleStore(store)
			require.NoError(t, roles.CreateRole(t.Context(), "ops"))
			require.NoError(t, a.UpdateRoles(t.Context(), "bob", []Role{"ops", RoleViewer}))
			entitlements := NewRoleEntitlementsStore(store)
			token, _, err := a.Authenticate(t.Context(), "carol", "password123", "127.0.0.1", "review")
			require.NoError(t, err)
			claims, err := a.ValidateToken(token.AccessToken)
			require.NoError(t, err)
			store.stall.Store(true)
			released := false
			release := func() {
				if !released {
					released = true
					close(store.gate)
				}
			}
			t.Cleanup(release)
			done := make(chan error, 1)
			go func() {
				stores := RoleMutationStores{Users: a, Entitlements: entitlements}
				if remove {
					done <- roles.DeleteRoleWithStores(context.Background(), "ops", stores)
				} else {
					done <- roles.RenameRoleWithStores(context.Background(), "ops", "operators", stores)
				}
			}()
			select {
			case <-store.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("role mutation never reached storage")
			}
			checked := make(chan error, 1)
			go func() {
				current, err := a.CurrentClaims(claims)
				if err != nil {
					checked <- err
					return
				}
				// The HTTP auth middleware calls this after CurrentClaims, including
				// requests by users with only built-in roles unrelated to this mutation.
				permissions := PermissionsForRoles(current.Roles, entitlements)
				if !EntitlementsAllow(permissions, string(PermRead)) {
					checked <- ErrInvalidCredentials
					return
				}
				checked <- nil
			}()
			blocked := false
			select {
			case err := <-checked:
				require.NoError(t, err)
			case <-time.After(200 * time.Millisecond):
				blocked = true
			}
			release()
			require.NoError(t, <-done)
			if blocked {
				require.NoError(t, <-checked)
				t.Fatal("unrelated authenticated request permission check waited for role storage write")
			}
		})
	}
}
