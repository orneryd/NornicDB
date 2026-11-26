package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type restoreUsersReadFault struct {
	storage.Engine
	fail bool
}

var errRestoreUsersRead = errors.New("restored user directory unavailable")

func (engine *restoreUsersReadFault) StreamNodesWithOptions(ctx context.Context, options storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if engine.fail {
		return errRestoreUsersRead
	}
	return engine.Engine.StreamNodesWithOptions(ctx, options, visit)
}

func TestReloadUsersAfterRestoreFailsClosed(t *testing.T) {
	base := testfixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	config := AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}
	seed, err := NewAuthenticator(config, base)
	require.NoError(t, err)
	_, err = seed.CreateUser(t.Context(), "old-account", "password123", []Role{RoleAdmin})
	require.NoError(t, err)
	engine := &restoreUsersReadFault{Engine: base}
	authenticator, err := NewAuthenticator(config, engine)
	require.NoError(t, err)
	_, _, err = authenticator.Authenticate(t.Context(), "old-account", "password123", "", "")
	require.NoError(t, err)

	engine.fail = true
	require.ErrorIs(t, authenticator.ReloadUsersAfterRestore(t.Context()), errRestoreUsersRead)
	_, _, err = authenticator.Authenticate(t.Context(), "old-account", "password123", "", "")
	require.ErrorIs(t, err, ErrInvalidCredentials)
}
