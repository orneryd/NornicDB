package nornicdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const historicalRecoveryBaselinePending = ".historical-recovery-baseline-pending"

func historicalRecoveryBaselinePath(dataDir string) string {
	return filepath.Join(dataDir, historicalRecoveryBaselinePending)
}

func historicalRecoveryBaselineIsPending(dataDir string) (bool, error) {
	if dataDir == "" {
		return false, nil
	}
	path := historicalRecoveryBaselinePath(dataDir)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("historical recovery baseline marker is not a regular file: %s", path)
	}
	return true, nil
}

// markHistoricalRecoveryBaselinePending is durable before storage migration.
// A crash after migration but before the new snapshot must retry this work.
func markHistoricalRecoveryBaselinePending(dataDir string) error {
	path := historicalRecoveryBaselinePath(dataDir)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("historical recovery baseline marker is not a regular file: %s", path)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("create historical recovery baseline marker: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync historical recovery baseline marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close historical recovery baseline marker: %w", err)
	}
	return storage.SyncDir(dataDir)
}

func completeHistoricalRecoveryBaseline(ctx context.Context, dataDir string, walEngine *storage.WALEngine) error {
	path := historicalRecoveryBaselinePath(dataDir)
	pending, err := historicalRecoveryBaselineIsPending(dataDir)
	if err != nil {
		return err
	}
	if !pending {
		return nil
	}
	if err := walEngine.CreateRecoveryBaseline(ctx, filepath.Join(dataDir, "snapshots")); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove historical recovery baseline marker: %w", err)
	}
	return storage.SyncDir(dataDir)
}
