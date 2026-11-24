package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRestoreNativeBackupCommand(t *testing.T) {
	root := t.TempDir()
	source, err := storage.NewBadgerEngine(filepath.Join(root, "source"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
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
	restored, err := storage.NewBadgerEngine(destination)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	node, err := restored.GetNode(t.Context(), "shared:retained")
	require.NoError(t, err)
	require.Equal(t, []string{"Retained"}, node.Labels)
}
