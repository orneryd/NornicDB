package auth

// Written by the prune-lockup lane (Claude Opus 5.5), 9 November 2026.
//
// A login that waits on storage must not hold the lock every token and
// cached-credential check needs. Before this fix Authenticate held the
// Authenticator's lock across its storage write, so one slow login write froze
// every authenticated request. A changed password must still stop working at
// once (#1200).

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// stallingUserStore holds every user-record write while stall is set.
type stallingUserStore struct {
	storage.Engine
	stall   atomic.Bool
	entered chan struct{}
	gate    chan struct{}
}

func (s *stallingUserStore) hold() {
	if s.stall.Load() {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-s.gate
	}
}

func (s *stallingUserStore) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	s.hold()
	return WithStoreTransaction(ctx, s.Engine, mutate)
}

// CreateNode holds direct writes too (a role's creation writes outside a
// store transaction).
func (s *stallingUserStore) CreateNode(ctx context.Context, node *storage.Node) (storage.NodeID, error) {
	s.hold()
	return s.Engine.CreateNode(ctx, node)
}

func newStallingAuthenticator(t *testing.T) (*Authenticator, *stallingUserStore) {
	t.Helper()
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	// User and role records live in the system database, as in production.
	store := &stallingUserStore{Engine: storage.NewNamespacedEngine(engine, "system"), entered: make(chan struct{}, 1), gate: make(chan struct{})}
	config := DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := NewAuthenticator(config, store)
	require.NoError(t, err)
	for _, user := range []string{"bob", "carol"} {
		_, err := authenticator.CreateUser(context.Background(), user, "password123", []Role{RoleViewer})
		require.NoError(t, err)
	}
	return authenticator, store
}

func TestLoginStorageWriteDoesNotBlockTokenChecks(t *testing.T) {
	authenticator, store := newStallingAuthenticator(t)
	token, _, err := authenticator.Authenticate(context.Background(), "carol", "password123", "127.0.0.1", "test")
	require.NoError(t, err)
	claims, err := authenticator.ValidateToken(token.AccessToken)
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
	login := make(chan error, 1)
	go func() {
		_, _, err := authenticator.Authenticate(context.Background(), "bob", "password123", "127.0.0.1", "test")
		login <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the login never reached its storage write")
	}

	// The login is waiting on storage: a token check and a user lookup answer.
	checked := make(chan error, 1)
	go func() {
		if _, err := authenticator.CurrentClaims(claims); err != nil {
			checked <- err
			return
		}
		_, err := authenticator.GetUser("carol")
		checked <- err
	}()
	select {
	case err := <-checked:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		release()
		<-login
		t.Fatal("a token check waited for another login's storage write")
	}
	release()
	require.NoError(t, <-login)
}

// Every user-record writer, not only a login, runs its storage write without
// the lock token checks need: account changes, creation, deletion, the admin
// role/status update and role rename or removal.
func TestUserRecordWritesDoNotBlockTokenChecks(t *testing.T) {
	writers := []struct {
		name  string
		write func(context.Context, *Authenticator, *RoleStore) error
	}{
		{"change password", func(ctx context.Context, a *Authenticator, _ *RoleStore) error {
			return a.ChangePassword(ctx, "bob", "password123", "newPassword456")
		}},
		{"update roles", func(ctx context.Context, a *Authenticator, _ *RoleStore) error {
			return a.UpdateRoles(ctx, "bob", []Role{RoleEditor})
		}},
		{"change state", func(ctx context.Context, a *Authenticator, _ *RoleStore) error {
			return a.ChangeUserState(ctx, "bob", UserDisable)
		}},
		{"update profile", func(ctx context.Context, a *Authenticator, _ *RoleStore) error {
			return a.UpdateUser(ctx, "bob", "bob@example.com", nil)
		}},
		{"create user", func(ctx context.Context, a *Authenticator, _ *RoleStore) error {
			_, err := a.CreateUser(ctx, "dave", "password123", []Role{RoleViewer})
			return err
		}},
		{"delete user", func(ctx context.Context, a *Authenticator, _ *RoleStore) error {
			return a.DeleteUser(ctx, "bob")
		}},
		{"admin roles and status", func(ctx context.Context, a *Authenticator, _ *RoleStore) error {
			disabled := true
			return a.UpdateRolesAndStatus(ctx, "bob", []Role{RoleEditor}, &disabled)
		}},
		{"role rename", func(ctx context.Context, a *Authenticator, roles *RoleStore) error {
			return roles.RenameRoleWithStores(ctx, "ops", "operators", RoleMutationStores{Users: a})
		}},
		{"role delete", func(ctx context.Context, a *Authenticator, roles *RoleStore) error {
			return roles.DeleteRoleWithStores(ctx, "ops", RoleMutationStores{Users: a})
		}},
	}
	for _, writer := range writers {
		t.Run(writer.name, func(t *testing.T) {
			authenticator, store := newStallingAuthenticator(t)
			roles := NewRoleStore(store)
			require.NoError(t, roles.CreateRole(context.Background(), "ops"))
			require.NoError(t, authenticator.UpdateRoles(context.Background(), "bob", []Role{"ops", RoleViewer}))
			token, _, err := authenticator.Authenticate(context.Background(), "carol", "password123", "127.0.0.1", "test")
			require.NoError(t, err)
			claims, err := authenticator.ValidateToken(token.AccessToken)
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
			go func() { done <- writer.write(context.Background(), authenticator, roles) }()
			select {
			case <-store.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the write never reached storage")
			}
			checked := make(chan error, 1)
			go func() {
				if _, err := authenticator.CurrentClaims(claims); err != nil {
					checked <- err
					return
				}
				_, err := authenticator.GetUser("carol")
				checked <- err
			}()
			select {
			case err := <-checked:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				release()
				<-done
				t.Fatalf("a token check waited for the %s storage write", writer.name)
			}
			release()
			require.NoError(t, <-done)
		})
	}
}

func TestPasswordChangeRacingLoginsStaysDurable(t *testing.T) {
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	config := DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	config.BcryptCost = 4
	// The racing wrong-password attempts must not lock the account.
	config.MaxFailedLogins = 1 << 30
	authenticator, err := NewAuthenticator(config, engine)
	require.NoError(t, err)
	_, err = authenticator.CreateUser(context.Background(), "bob", "password123", []Role{RoleViewer})
	require.NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _, _ = authenticator.Authenticate(context.Background(), "bob", "password123", "127.0.0.1", "test")
				_, _, _ = authenticator.Authenticate(context.Background(), "bob", "newPassword456", "127.0.0.1", "test")
			}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, authenticator.ChangePassword(context.Background(), "bob", "password123", "newPassword456"))
	// Once the change returned, the old password is refused.
	_, _, err = authenticator.Authenticate(context.Background(), "bob", "password123", "127.0.0.1", "test")
	require.ErrorIs(t, err, ErrInvalidCredentials)
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	// No login's bookkeeping wrote the old hash back: a fresh load agrees.
	reloaded, err := NewAuthenticator(config, engine)
	require.NoError(t, err)
	_, _, err = reloaded.Authenticate(context.Background(), "bob", "password123", "127.0.0.1", "test")
	require.ErrorIs(t, err, ErrInvalidCredentials)
	_, _, err = reloaded.Authenticate(context.Background(), "bob", "newPassword456", "127.0.0.1", "test")
	require.NoError(t, err)
}
