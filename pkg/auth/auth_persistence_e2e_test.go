package auth

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

func TestAdminPasswordChangeSurvivesBootstrapRestart(t *testing.T) {
	const (
		username        = "admin"
		initialPassword = "password"
		changedPassword = "changed-console-password"
	)

	dataDir := t.TempDir()
	authConfig := DefaultAuthConfig()
	authConfig.BcryptCost = bcrypt.MinCost
	authConfig.JWTSecret = []byte("test-jwt-secret-at-least-32-bytes")

	openAndBootstrap := func() (*Authenticator, *storage.BadgerEngine, storage.Engine, error) {
		engine, err := storage.NewBadgerEngine(dataDir)
		if err != nil {
			return nil, nil, nil, err
		}
		systemStorage := storage.NewNamespacedEngine(engine, "system")
		authenticator, err := NewAuthenticator(authConfig, systemStorage)
		if err != nil {
			_ = engine.Close()
			return nil, nil, nil, err
		}
		_, bootstrapErr := authenticator.CreateUser(username, initialPassword, []Role{RoleAdmin})
		return authenticator, engine, systemStorage, bootstrapErr
	}

	authenticator, engine, systemStorage, err := openAndBootstrap()
	require.NoError(t, err)
	_, _, err = authenticator.Authenticate(username, initialPassword, "127.0.0.1", "e2e")
	require.NoError(t, err)

	initialNode, err := systemStorage.GetNode(storage.NodeID("user:" + username))
	require.NoError(t, err)
	require.NotContains(t, initialNode.Properties, "password")
	for _, value := range initialNode.Properties {
		require.NotEqual(t, initialPassword, value, "plaintext password must not be a persisted property value")
	}
	initialHash, ok := initialNode.Properties["password_hash"].(string)
	require.True(t, ok)
	require.NotEqual(t, initialPassword, initialHash)
	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(initialHash), []byte(initialPassword)))
	require.GreaterOrEqual(t, len(initialHash), 29)

	independentHash, err := bcrypt.GenerateFromPassword([]byte(initialPassword), authConfig.BcryptCost)
	require.NoError(t, err)
	require.NotEqual(t, initialHash[:29], string(independentHash[:29]),
		"bcrypt must generate a distinct salt for each hash")

	require.NoError(t, authenticator.ChangePassword(username, initialPassword, changedPassword))
	changedNode, err := systemStorage.GetNode(storage.NodeID("user:" + username))
	require.NoError(t, err)
	changedHash, ok := changedNode.Properties["password_hash"].(string)
	require.True(t, ok)
	require.NotEqual(t, initialHash, changedHash)
	require.NoError(t, bcrypt.CompareHashAndPassword([]byte(changedHash), []byte(changedPassword)))
	require.NoError(t, engine.Close())

	// The literal default is part of the schema key "password_hash", so only
	// the unique changed credential can be checked reliably at raw-byte level.
	assertDirectoryExcludesPlaintext(t, dataDir, changedPassword)

	reloadedAuth, reloadedEngine, reloadedStorage, bootstrapErr := openAndBootstrap()
	t.Cleanup(func() { _ = reloadedEngine.Close() })
	require.ErrorIs(t, bootstrapErr, ErrUserExists,
		"bootstrap must not overwrite an existing admin user")

	reloadedNode, err := reloadedStorage.GetNode(storage.NodeID("user:" + username))
	require.NoError(t, err)
	require.Equal(t, changedHash, reloadedNode.Properties["password_hash"],
		"restart bootstrap must preserve the password changed through the console")
	_, _, err = reloadedAuth.Authenticate(username, initialPassword, "127.0.0.1", "e2e")
	require.ErrorIs(t, err, ErrInvalidCredentials)
	_, _, err = reloadedAuth.Authenticate(username, changedPassword, "127.0.0.1", "e2e")
	require.NoError(t, err)
}

func assertDirectoryExcludesPlaintext(t *testing.T, root string, secrets ...string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, secret := range secrets {
			if bytes.Contains(contents, []byte(secret)) {
				return errors.New("plaintext password found in persisted data file")
			}
		}
		return nil
	}))
}
