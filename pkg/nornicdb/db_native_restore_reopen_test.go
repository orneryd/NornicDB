package nornicdb

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNativeRestoreReopensAfterTargetHadWrites(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	source, err := Open(sourceDir, nil)
	require.NoError(t, err)
	want, err := source.CreateNode(ctx, []string{"Restored"}, map[string]any{"name": "from-backup"})
	require.NoError(t, err)
	backupPath := filepath.Join(t.TempDir(), "native.backup")
	require.NoError(t, source.Backup(ctx, backupPath))
	require.NoError(t, source.Close())

	targetDir := t.TempDir()
	target, err := Open(targetDir, nil)
	require.NoError(t, err)
	system := storage.NewNamespacedEngine(target.GetBaseStorageForManager(), "system")
	_, err = system.CreateNode(ctx, &storage.Node{ID: "system-before-restore", Labels: []string{"System"}, Properties: map[string]any{"name": "before-restore"}})
	require.NoError(t, err)
	_, err = target.CreateNode(ctx, []string{"Old"}, map[string]any{"name": "before-restore"})
	require.NoError(t, err)
	preRestoreSequence := target.wal.Sequence()
	require.NoError(t, target.Restore(ctx, backupPath))
	markerData, err := os.ReadFile(filepath.Join(targetDir, nativeRestoreMarker))
	require.NoError(t, err)
	var boundary nativeRestoreBoundary
	require.NoError(t, json.Unmarshal(markerData, &boundary))
	require.Equal(t, preRestoreSequence, boundary.WALSequence)
	restored, err := target.GetNode(ctx, want.ID)
	require.NoError(t, err)
	require.Equal(t, "from-backup", restored.Properties["name"])
	require.NoError(t, target.Close())

	reopened, err := Open(targetDir, nil)
	require.NoError(t, err, "a successful native restore must survive close and reopen")
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	restored, err = reopened.GetNode(ctx, want.ID)
	require.NoError(t, err)
	require.Equal(t, "from-backup", restored.Properties["name"])
}
