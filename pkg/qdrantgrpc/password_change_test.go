package qdrantgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/base64"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Over gRPC Basic, the old password stops working as soon as it is
// changed, though the server caches the password check.
func TestChangedPasswordStopsWorkingOverGRPC(t *testing.T) {
	store := newTestCollectionStore(t)
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("0123456789abcdef0123456789abcdef")
	config.SecurityEnabled = true
	authStore := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = authStore.Close() })
	authenticator, err := auth.NewAuthenticator(config, authStore)
	require.NoError(t, err)
	_, err = authenticator.CreateUser("viewer4", "ViewerPass123!", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	serverStore := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = serverStore.Close() })
	server, err := NewServer(DefaultConfig(), store, serverStore, nil, authenticator)
	require.NoError(t, err)
	basic := func(password string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte("viewer4:"+password))
	}
	for range 2 {
		_, err = server.handleBasicAuth(context.Background(), basic("ViewerPass123!"))
		require.NoError(t, err)
	}
	require.NoError(t, authenticator.ChangePassword("viewer4", "ViewerPass123!", "NewViewerPass456!"))
	_, err = server.handleBasicAuth(context.Background(), basic("ViewerPass123!"))
	require.Error(t, err)
	_, err = server.handleBasicAuth(context.Background(), basic("NewViewerPass456!"))
	require.NoError(t, err)
}
