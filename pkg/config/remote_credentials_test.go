package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteCredentialKeyRequiresDurableMaterial(t *testing.T) {
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "")
	cfg := LoadDefaults()
	key, source := cfg.RemoteCredentialEncryptionKey()
	require.Empty(t, key, "process-local defaults must never encrypt durable credentials")
	require.Empty(t, source)
	copy := *cfg
	key, _ = copy.RemoteCredentialEncryptionKey()
	require.Empty(t, key, "copying defaults does not make their secret durable")
	cfg.Auth.JWTSecret = "synthetic-explicit-jwt"
	key, source = cfg.RemoteCredentialEncryptionKey()
	require.Equal(t, "synthetic-explicit-jwt", key)
	require.Equal(t, "jwt_signing_secret", source)
	cfg.Database.EncryptionPassword = "synthetic-database-key"
	key, source = cfg.RemoteCredentialEncryptionKey()
	require.Equal(t, "synthetic-database-key", key)
	require.Equal(t, "database_encryption_password", source)
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "synthetic-dedicated-key")
	key, source = cfg.RemoteCredentialEncryptionKey()
	require.Equal(t, "synthetic-dedicated-key", key)
	require.Equal(t, "environment", source)
}

func TestRemoteCredentialKeyExplicitConfigurationProvenance(t *testing.T) {
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "")
	t.Setenv("NORNICDB_AUTH_JWT_SECRET", "")
	cfg := LoadDefaults()
	// A deliberately persisted/configured value is stable even when its spelling
	// matches a generated default. Eligibility follows origin, not a prefix rule.
	configured := cfg.Auth.JWTSecret
	t.Setenv("NORNICDB_AUTH_JWT_SECRET", configured)
	require.NoError(t, ApplyEnvVars(cfg))
	key, source := cfg.RemoteCredentialEncryptionKey()
	require.Equal(t, configured, key)
	require.Equal(t, "jwt_signing_secret", source)
	t.Setenv("NORNICDB_AUTH_JWT_SECRET", "")
	path := filepath.Join(t.TempDir(), "nornic.yaml")
	require.NoError(t, os.WriteFile(path, []byte("auth:\n  jwt_secret: "+configured+"\n"), 0600))
	loaded, err := LoadFromFile(path)
	require.NoError(t, err)
	key, source = loaded.RemoteCredentialEncryptionKey()
	require.Equal(t, configured, key)
	require.Equal(t, "jwt_signing_secret", source)
}
