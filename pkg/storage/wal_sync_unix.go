//go:build !windows
// +build !windows

package storage

import (
	"fmt"
	"os"
)

// syncDir fsyncs a directory to ensure metadata changes (file creation, rename) are durable.
//
// This is critical for crash recovery on Unix systems - without it, file operations
// may not survive a crash even if the files themselves were fsync'd.
//
// Why this matters:
// - Creating a file: The directory entry must be synced to disk
// - Renaming a file: Both old and new directory entries must be synced
// - Without dir sync: Crash could lose the directory entry (file becomes orphaned)
//
// This is a Unix requirement. Windows handles this differently (see wal_sync_windows.go).
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: failed to open directory for sync: %w", err)
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync directory: %w", err)
	}
	return nil
}
