package nornicdb

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestJSONBackupDetectionRejectsBinaryLengthPrefix(t *testing.T) {
	for _, opening := range []byte{'{', '['} {
		t.Run(string(opening), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "native.backup")
			// The first eight bytes are a little-endian record length; the
			// remaining bytes make that length plausible for this file.
			data := append([]byte{opening, 0, 0, 0, 0, 0, 0, 0}, make([]byte, int(opening))...)
			require.NoError(t, os.WriteFile(path, data, 0o600))
			jsonBackup, err := isJSONBackupFile(path)
			require.NoError(t, err)
			require.False(t, jsonBackup)
		})
	}
	path := filepath.Join(t.TempDir(), "legacy.json")
	require.NoError(t, os.WriteFile(path, []byte(" \n\t{\"version\":\"2.0\",\"nodes\":[],\"edges\":[]}"), 0o600))
	jsonBackup, err := isJSONBackupFile(path)
	require.NoError(t, err)
	require.True(t, jsonBackup)
}

// A Badger stream starts with a little-endian record length, whose low byte
// can equal a JSON opening delimiter. Both files must stay native restores.
func TestNativeRestoreAcceptsJSONOpeningBytes(t *testing.T) {
	ctx := context.Background()
	source, err := Open(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	node, err := source.CreateNode(ctx, []string{"Pad"}, map[string]any{"value": ""})
	require.NoError(t, err)

	wanted := map[byte]string{'{': "brace", '[': "bracket"}
	found := make(map[byte]string)
	backupDir := t.TempDir()
	for size := 0; size < 3000 && len(found) < len(wanted); size++ {
		_, err = source.UpdateNode(ctx, node.ID, map[string]any{"value": strings.Repeat("x", size)})
		require.NoError(t, err)
		backup := filepath.Join(backupDir, fmt.Sprintf("native-%d.backup", size))
		require.NoError(t, source.Backup(ctx, backup))
		file, err := os.Open(backup)
		require.NoError(t, err)
		var prefix [8]byte
		_, readErr := io.ReadFull(file, prefix[:])
		require.NoError(t, file.Close())
		require.NoError(t, readErr)
		if _, needed := wanted[prefix[0]]; needed && found[prefix[0]] == "" {
			found[prefix[0]] = backup
		} else {
			require.NoError(t, os.Remove(backup))
		}
	}
	for opening, name := range wanted {
		backup := found[opening]
		require.NotEmpty(t, backup, "no native backup started with %q", opening)
		t.Run(name, func(t *testing.T) {
			control, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, control.Close()) })
			require.NoError(t, control.Restore(backup), "native engine control")
			target, err := Open(t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, target.Close()) })
			require.NoError(t, target.Restore(ctx, backup), "DB.Restore must not classify a native backup as JSON")
			_, err = target.GetNode(ctx, node.ID)
			require.NoError(t, err)
		})
	}
}
