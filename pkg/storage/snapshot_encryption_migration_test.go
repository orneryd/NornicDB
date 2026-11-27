package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptSnapshotFileMigratesPlaintextAtomically(t *testing.T) {
	engine := newSmallEncryptedSnapshotEngine(t)
	path := filepath.Join(t.TempDir(), "snapshot-legacy.json")
	secret := []byte("sensitive-snapshot-envelope-value-3f6c")
	require.NoError(t, SaveStreamingSnapshot(t.Context(), engine, path, SnapshotOptions{Sequence: 41}))
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(before), string(secret))

	key := testSnapshotEncryptionKey()
	require.NoError(t, EncryptSnapshotFile(t.Context(), path, key))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(after), string(secret))
	require.True(t, bytes.HasPrefix(after, []byte(encryptedSnapshotMagic)))
	_, err = os.Stat(path + ".tmp")
	require.True(t, os.IsNotExist(err))
	snapshot, err := LoadSnapshotWithKey(t.Context(), path, key)
	require.NoError(t, err)
	require.Equal(t, uint64(41), snapshot.Sequence)
	require.Len(t, snapshot.Nodes, 2)
	require.Len(t, snapshot.Edges, 1)
	// A second startup validates and leaves the already-encrypted file intact.
	require.NoError(t, EncryptSnapshotFile(t.Context(), path, key))
	repeated, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, after, repeated)
	require.Error(t, EncryptSnapshotFile(t.Context(), path, testSnapshotEncryptionWrongKey()))
	still, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, after, still)
}

func TestEncryptSnapshotFileRefusesDamagedPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot-damaged.json")
	bad := []byte("NDS2\x02truncated")
	require.NoError(t, os.WriteFile(path, bad, 0600))
	require.Error(t, EncryptSnapshotFile(t.Context(), path, testSnapshotEncryptionKey()))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, bad, after)
}
