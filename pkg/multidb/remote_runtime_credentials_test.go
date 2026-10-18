package multidb

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Check the manager's runtime factory boundary and the unchanged persisted
// representation, rather than providing already-decrypted credentials to Fabric.
func TestRemoteRuntimeCredentials(t *testing.T) {
	const password = "synthetic-runtime-password-91364827"
	for _, test := range []struct{ name, uri, user, password, mode, token, wantUser, wantPass, wantMode string }{
		{"explicit", "http://uri:ignored@127.0.0.1:1", "admin", password, "user_password", "", "admin", password, "user_password"},
		{"http-uri", "http://admin:" + password + "@127.0.0.1:1", "", "", "", "", "admin", password, "user_password"},
		{"bolt-uri", "bolt://admin:" + password + "@127.0.0.1:1", "", "", "", "", "admin", password, "user_password"},
		{"encoded-uri", "http://service%40tenant:synthetic%3Aencoded%40password@127.0.0.1:1", "", "", "", "", "service@tenant", "synthetic:encoded@password", "user_password"},
		{"forwarded-token", "http://admin:" + password + "@127.0.0.1:1", "", "", "oidc_forwarding", "Bearer synthetic-forwarded-token-61827394", "", "", "oidc_forwarding"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultConfig()
			config.RemoteCredentialEncryptionKey = "synthetic-runtime-key-61829374"
			var received []ConstituentRef
			config.RemoteEngineFactory = func(ref ConstituentRef, token string) (storage.Engine, error) {
				received = append(received, ref)
				require.Equal(t, test.token, token)
				return storage.NewMemoryEngine(), nil
			}
			manager, err := NewDatabaseManager(storage.NewMemoryEngine(), config)
			require.NoError(t, err)
			defer manager.Close()
			ref := ConstituentRef{Alias: "remote", DatabaseName: "nornic", Type: "remote", AccessMode: "read_write", URI: test.uri, User: test.user, Password: test.password, AuthMode: test.mode}
			require.NoError(t, manager.CreateCompositeDatabase("runtime", []ConstituentRef{ref}))
			stored, err := manager.GetCompositeConstituents("runtime")
			require.NoError(t, err)
			if test.mode == "user_password" {
				require.True(t, strings.HasPrefix(stored[0].Password, remoteCredentialPrefix))
				require.NotEqual(t, password, stored[0].Password)
			}
			for _, name := range []string{"runtime.remote", "runtime"} {
				engine, err := manager.GetStorageWithAuth(name, test.token)
				require.NoError(t, err)
				defer engine.Close()
				actual := received[len(received)-1]
				require.Equal(t, test.wantUser, actual.User)
				require.Equal(t, test.wantPass, actual.Password)
				require.Equal(t, test.wantMode, actual.AuthMode)
				require.NotContains(t, actual.URI, "@")
				require.NotContains(t, actual.URI, password)
			}
			after, err := manager.GetCompositeConstituents("runtime")
			require.NoError(t, err)
			require.Equal(t, stored, after)
		})
	}
}

func TestRemoteRuntimeCredentialErrorsAreRedacted(t *testing.T) {
	const password = "synthetic-invalid-uri-password-54819376"
	config := DefaultConfig()
	config.RemoteCredentialEncryptionKey = "synthetic-key-81936472"
	config.RemoteEngineFactory = func(ConstituentRef, string) (storage.Engine, error) {
		t.Fatal("invalid endpoint reached factory")
		return nil, nil
	}
	manager, err := NewDatabaseManager(storage.NewMemoryEngine(), config)
	require.NoError(t, err)
	defer manager.Close()
	require.NoError(t, manager.CreateCompositeDatabase("invalid_uri", []ConstituentRef{{Alias: "remote", DatabaseName: "nornic", Type: "remote", AccessMode: "read", URI: "http://admin:" + password + "@%invalid"}}))
	_, err = manager.GetStorage("invalid_uri.remote")
	require.Error(t, err)
	require.NotContains(t, err.Error(), password)
}
