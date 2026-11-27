package nornicdb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEncryptedSnapshotProbe(t *testing.T) {
	dir := t.TempDir()
	cfg := testEncryptionConfig("probe-password-12345")
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = true
	cfg.Database.WALSnapshotInterval = 20 * time.Millisecond
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	secret := "sensitive-snapshot-probe-7bf7d315e28f4d7f"
	created, err := db.CreateNode(context.Background(), []string{"Probe"}, map[string]any{"secret": secret})
	require.NoError(t, err)
	key, err := ReadStorageEncryptionKey(dir, cfg)
	require.NoError(t, err)
	var snapshotPath string
	require.Eventually(t, func() bool {
		files, _ := filepath.Glob(filepath.Join(dir, "snapshots", "snapshot-*.json"))
		for _, path := range files {
			snapshot, readErr := storage.LoadSnapshotWithKey(context.Background(), path, key)
			if readErr != nil {
				continue
			}
			for _, node := range snapshot.Nodes {
				if node.ID == storage.NodeID("nornic:"+created.ID) && node.Properties["secret"] == secret {
					snapshotPath = path
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	data, err := os.ReadFile(snapshotPath)
	require.NoError(t, err)
	require.False(t, bytes.Contains(data, []byte(secret)), "encrypted database snapshot contains plaintext property value")
}

func TestEncryptedWALProbe(t *testing.T) {
	dir := t.TempDir()
	cfg := testEncryptionConfig("probe-password-12345")
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = false
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	secret := "sensitive-journal-probe-c271728ca1fe4ddb"
	_, err = db.CreateNode(context.Background(), []string{"Probe"}, map[string]any{"secret": secret})
	require.NoError(t, err)
	require.NoError(t, db.Close())
	found := false
	require.NoError(t, filepath.WalkDir(filepath.Join(dir, "wal"), func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		found = found || bytes.Contains(data, []byte(secret))
		return nil
	}))
	require.False(t, found, "encrypted database WAL contains plaintext property value")
}

func TestEncryptedArtifactInventoryProbe(t *testing.T) {
	dir := t.TempDir()
	cfg := testEncryptionConfig("probe-password-12345")
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = true
	cfg.Database.WALSnapshotInterval = 20 * time.Millisecond
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	secret := "sensitive-artifact-probe-8170d29f86644af6"
	_, err = db.CreateNode(context.Background(), []string{"Probe"}, map[string]any{"secret": secret})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		files, _ := filepath.Glob(filepath.Join(dir, "snapshots", "snapshot-*.json"))
		return len(files) > 0
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, db.Close())
	var exposed []string
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(data, []byte(secret)) {
			rel, _ := filepath.Rel(dir, path)
			exposed = append(exposed, rel)
		}
		return nil
	}))
	require.Empty(t, exposed, "plaintext property appears in encrypted data directory")
}

func TestEncryptedStoreUpgradesPlaintextRecoveryArtifactsBeforeOpen(t *testing.T) {
	dir := t.TempDir()
	cfg := testEncryptionConfig("upgrade-password-12345")
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.WALAutoCompactionEnabled = false
	db, err := Open(dir, cfg)
	require.NoError(t, err)
	secret := "sensitive-upgrade-probe-c970d61adce4"
	created, err := db.CreateNode(t.Context(), []string{"Probe"}, map[string]any{"secret": secret})
	require.NoError(t, err)
	snapshotPath := filepath.Join(dir, "snapshots", "snapshot-1.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(snapshotPath), 0700))
	// Recreate the two plaintext recovery artifacts emitted by older encrypted
	// installations. Badger itself remains encrypted throughout the upgrade.
	require.NoError(t, storage.SaveStreamingSnapshot(t.Context(), db.baseStorage, snapshotPath, storage.SnapshotOptions{Sequence: 1}))
	require.NoError(t, db.Close())
	plainSnapshot, err := os.ReadFile(snapshotPath)
	require.NoError(t, err)
	require.Contains(t, string(plainSnapshot), secret)
	walDir := filepath.Join(dir, "wal")
	require.NoError(t, os.Remove(filepath.Join(walDir, "wal.log")))
	plainWAL, err := storage.NewWAL(walDir, &storage.WALConfig{Dir: walDir, SyncMode: "immediate"})
	require.NoError(t, err)
	require.NoError(t, plainWAL.AppendWithDatabase(storage.OpCheckpoint, map[string]any{"marker": secret}, "nornic"))
	require.NoError(t, plainWAL.Close())
	plainLog, err := os.ReadFile(filepath.Join(walDir, "wal.log"))
	require.NoError(t, err)
	require.Contains(t, string(plainLog), secret)

	db, err = Open(dir, cfg)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	key, err := ReadStorageEncryptionKey(dir, cfg)
	require.NoError(t, err)
	snapshot, err := storage.LoadSnapshotWithKey(t.Context(), snapshotPath, key)
	require.NoError(t, err)
	require.NotEmpty(t, snapshot.Nodes)
	found := false
	for _, node := range snapshot.Nodes {
		if node.ID == storage.NodeID("nornic:"+created.ID) && node.Properties["secret"] == secret {
			found = true
		}
	}
	require.True(t, found, "converted snapshot retains the node")
	for _, path := range []string{snapshotPath, filepath.Join(walDir, "wal.log")} {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NotContains(t, string(data), secret, "converted recovery artifact %s", path)
	}
}
