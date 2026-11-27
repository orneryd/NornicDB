package nornicdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// migrateEncryptedDurabilityArtifacts runs after the Badger key has been
// accepted and before any WAL or snapshot writer starts. Each file is
// atomically replaced by its authenticated form. A crash between files leaves
// a mixed directory that the next startup converts before serving traffic.
func migrateEncryptedDurabilityArtifacts(ctx context.Context, dataDir, replicationWALDir string, key []byte) error {
	if len(key) != storage.WALEncryptionKeySize {
		return storage.ErrWALEncryptionKeyInvalid
	}
	walDirs := []string{filepath.Join(dataDir, "wal")}
	if replicationWALDir != "" && replicationWALDir != walDirs[0] {
		walDirs = append(walDirs, replicationWALDir)
	}
	for _, dir := range walDirs {
		info, err := os.Lstat(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("encrypted WAL directory %s is not a directory", dir)
		}
		if err := storage.EncryptWALDirectory(ctx, dir, key); err != nil {
			return fmt.Errorf("encrypt WAL directory %s: %w", dir, err)
		}
	}
	return migrateEncryptedSnapshotDirectory(ctx, filepath.Join(dataDir, "snapshots"), key)
}

func migrateEncryptedSnapshotDirectory(ctx context.Context, dir string, key []byte) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	removedTemp := false
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "snapshot-") {
			continue
		}
		path := filepath.Join(dir, name)
		if strings.HasSuffix(name, ".json.tmp") {
			// An interrupted snapshot was never published. Keeping its old
			// plaintext temporary copy would defeat the upgraded file format.
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove interrupted snapshot %s: %w", path, err)
			}
			removedTemp = true
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		if err := storage.EncryptSnapshotFile(ctx, path, key); err != nil {
			return fmt.Errorf("encrypt snapshot %s: %w", path, err)
		}
	}
	if removedTemp {
		return storage.SyncDir(dir)
	}
	return nil
}
