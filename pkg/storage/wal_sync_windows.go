//go:build windows
// +build windows

package storage

import (
	"fmt"
	"os"
)

// syncDir is a no-op on Windows because directory fsync is not supported.
//
// On Windows, the file system guarantees that file metadata (creation, rename)
// is durable without explicit directory sync. This is different from Unix
// where directory fsync is required for metadata durability.
//
// Background:
// - Windows NTFS/ReFS automatically flush directory metadata
// - os.Open() on directories + Sync() fails with "Access is denied"
// - FlushFileBuffers() Windows API only works on file handles, not directories
//
// This means:
// - File creation via os.OpenFile(..., O_CREATE) is immediately durable
// - File renames via os.Rename() are immediately durable
// - No explicit sync needed (unlike Unix where dir fsync prevents data loss)
//
// Safety: This is safe because:
// 1. NTFS/ReFS are journaling filesystems (metadata changes are atomic)
// 2. File-level fsync (file.Sync()) still works and ensures file content durability
// 3. WAL files themselves are fsync'd after writes (syncMode setting)
//
// See: https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers
func syncDir(dir string) error {
	// Verify directory exists (fail fast if path is wrong)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("wal: directory does not exist: %w", err)
	}

	// Windows: Directory metadata changes are automatically durable
	// No action needed - this is not a bug, it's how Windows filesystems work
	return nil
}
