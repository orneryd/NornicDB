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
