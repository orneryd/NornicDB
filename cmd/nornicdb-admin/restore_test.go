package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRestoreNativeBackupCommand(t *testing.T) {
	root := t.TempDir()
	source, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(filepath.Join(root, "source")))
	require.NoError(t, err)
	_, err = source.CreateNode(t.Context(), &storage.Node{ID: "shared:retained", Labels: []string{"Retained"}})
	require.NoError(t, err)
	backup := filepath.Join(root, "shared.backup")
	require.NoError(t, source.Backup(backup))
	destination := filepath.Join(root, "restored")
	t.Chdir(root)
	execute := func() error {
		manager, err := newCommandLocalizer()
		require.NoError(t, err)
		command := newTestAdminRoot(manager)
		command.SetArgs([]string{"database", "restore", "./shared.backup", "--data-dir", "./restored"})
		return command.ExecuteContext(t.Context())
	}
	require.NoError(t, execute())
	require.ErrorIs(t, execute(), os.ErrExist)
	restored, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(destination))
	require.NoError(t, err)
	node, err := restored.GetNode(t.Context(), "shared:retained")
	require.NoError(t, err)
	require.Equal(t, []string{"Retained"}, node.Labels)
}

func TestEncryptedAdminStorageCommands(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	settingsFile := filepath.Join(dir, "server.yaml")
	require.NoError(t, os.WriteFile(settingsFile, []byte("database:\n  encryption_enabled: true\n  encryption_password: synthetic-admin-password\n"), 0600))
	settings, err := config.LoadFromFile(settingsFile)
	require.NoError(t, err)
	key, err := nornicdb.InitializeStorageEncryption(data, settings)
	require.NoError(t, err)
	engine, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(
		storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: data, EncryptionKey: key}),
	)
	require.NoError(t, err)
	_, err = engine.CreateNode(t.Context(), &storage.Node{ID: "shared:kept", Labels: []string{"Kept"}})
	require.NoError(t, err)
	backup := filepath.Join(dir, "encrypted-source.backup")
	require.NoError(t, engine.Backup(backup))
	csv := filepath.Join(dir, "nodes.csv")
	require.NoError(t, os.WriteFile(csv, []byte(":ID\na\n"), 0600))
	execute := func(args ...string) (string, error) {
		manager, err := newCommandLocalizer()
		require.NoError(t, err)
		root := newTestAdminRoot(manager)
		var output bytes.Buffer
		root.SetOut(&output)
		root.SetArgs(append([]string{"--config", settingsFile}, args...))
		err = root.ExecuteContext(t.Context())
		return strings.TrimSpace(output.String()), err
	}
	id, err := execute("--data-dir", data, "database", "import", "full", "staged", "--stage", "--nodes", "Item="+csv)
	require.NoError(t, err)
	stage, _, err := storage.ReadPreparedDatabase(data, id, "staged")
	require.NoError(t, err)
	require.Error(t, storage.VerifyBadgerEncryptionKey(stage, nil))
	require.NoError(t, storage.VerifyBadgerEncryptionKey(stage, key))
	require.NoError(t, engine.Close())
	_, err = execute("--data-dir", data, "database", "check-ids")
	require.NoError(t, err)
	restored := filepath.Join(dir, "restored")
	_, err = execute("--data-dir", restored, "database", "restore", backup)
	require.NoError(t, err)
	restoredKey, err := nornicdb.ReadStorageEncryptionKey(restored, settings)
	require.NoError(t, err)
	require.NotEqual(t, key, restoredKey)
	require.Error(t, storage.VerifyBadgerEncryptionKey(restored, nil))
	result, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(
		storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: restored, EncryptionKey: restoredKey}),
	)
	require.NoError(t, err)
	_, err = result.GetNode(t.Context(), "shared:kept")
	require.NoError(t, err)
}
