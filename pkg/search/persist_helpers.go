package search

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/vmihailenco/msgpack/v5"
)

// writeMsgpackSnapshot creates parent directories, writes snapshot to file, and encodes msgpack.
func writeMsgpackSnapshot(path string, snapshot any) error {
	// lgtm[go/path-injection] -- path is derived from the configured search persistence root.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := security.CreateRootedFile(path, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	return msgpack.NewEncoder(file).Encode(snapshot)
}

// writeMsgpackSnapshots writes multiple msgpack snapshots under one directory.
// Filenames are deterministic (sorted keys) to keep persistence behavior stable.
func writeMsgpackSnapshots(dir string, snapshots map[string]any) error {
	// lgtm[go/path-injection] -- dir is derived from the configured search persistence root.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	names := make([]string, 0, len(snapshots))
	for name := range snapshots {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := writeMsgpackSnapshot(filepath.Join(dir, name), snapshots[name]); err != nil {
			return err
		}
	}
	return nil
}

// writeMsgpackSnapshotsAtomic swaps a full snapshot bundle into place using
// directory rename operations. This is intended for multipart snapshot sets
// (e.g. codebooks + postings + metadata) that must move together.
func writeMsgpackSnapshotsAtomic(dir string, snapshots map[string]any) error {
	parent := filepath.Dir(dir)
	// lgtm[go/path-injection] -- parent is derived from the configured search persistence root.
	if err := os.MkdirAll(parent, 0755); err != nil {
		return err
	}
	tmpDir := filepath.Join(parent, fmt.Sprintf(".tmp-%d", time.Now().UnixNano()))
	backupDir := filepath.Join(parent, fmt.Sprintf(".bak-%d", time.Now().UnixNano()))

	if err := writeMsgpackSnapshots(tmpDir, snapshots); err != nil {
		// lgtm[go/path-injection] -- deterministic temporary bundle path under configured parent.
		_ = os.RemoveAll(tmpDir)
		return err
	}

	if _, err := os.Stat(dir); err == nil {
		// lgtm[go/path-injection] -- deterministic bundle paths under configured parent.
		if err := os.Rename(dir, backupDir); err != nil {
			_ = os.RemoveAll(tmpDir)
			return err
		}
	}
	// lgtm[go/path-injection] -- deterministic bundle paths under configured parent.
	if err := os.Rename(tmpDir, dir); err != nil {
		_ = os.Rename(backupDir, dir)
		_ = os.RemoveAll(tmpDir)
		return err
	}
	if _, err := os.Stat(backupDir); err == nil {
		// lgtm[go/path-injection] -- deterministic backup bundle under configured parent.
		_ = os.RemoveAll(backupDir)
	}
	return nil
}
