package search

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/vmihailenco/msgpack/v5"
)

const msgpackSnapshotBufferBytes = 1 << 20

var msgpackSnapshotWriterPool = sync.Pool{
	New: func() any {
		return bufio.NewWriterSize(io.Discard, msgpackSnapshotBufferBytes)
	},
}

// encodeMsgpackBuffered coalesces the encoder's many small writes into large
// sequential writes. This is shared by every large search snapshot format.
func encodeMsgpackBuffered(writer io.Writer, snapshot any) error {
	buffered := msgpackSnapshotWriterPool.Get().(*bufio.Writer)
	buffered.Reset(writer)
	defer func() {
		buffered.Reset(io.Discard)
		msgpackSnapshotWriterPool.Put(buffered)
	}()
	if err := msgpack.NewEncoder(buffered).Encode(snapshot); err != nil {
		return err
	}
	return buffered.Flush()
}

// writeMsgpackSnapshot atomically replaces a snapshot after buffered encoding
// and fsync. An interrupted or failed save leaves the prior snapshot intact.
func writeMsgpackSnapshot(path string, snapshot any) error {
	if err := security.EnsureRootedParent(path, 0o755); err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	file, err := security.CreateRootedFile(tmpPath, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
		_ = security.RemoveRootedPath(tmpPath)
	}()
	if err := encodeMsgpackBuffered(file, snapshot); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return security.RenameRootedFile(tmpPath, path)
}

// writeMsgpackSnapshots writes multiple msgpack snapshots under one directory.
// Filenames are deterministic (sorted keys) to keep persistence behavior stable.
func writeMsgpackSnapshots(dir string, snapshots map[string]any) error {
	if err := security.EnsureRootedParent(filepath.Join(dir, ".snapshot"), 0o755); err != nil {
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
	if err := security.EnsureRootedParent(filepath.Join(parent, ".snapshot"), 0o755); err != nil {
		return err
	}
	tmpDir := filepath.Join(parent, fmt.Sprintf(".tmp-%d", time.Now().UnixNano()))
	backupDir := filepath.Join(parent, fmt.Sprintf(".bak-%d", time.Now().UnixNano()))

	if err := writeMsgpackSnapshots(tmpDir, snapshots); err != nil {
		_ = security.RemoveAllRootedPath(tmpDir)
		return err
	}

	if _, err := security.RootedStat(dir); err == nil {
		if err := security.RenameRootedFile(dir, backupDir); err != nil {
			_ = security.RemoveAllRootedPath(tmpDir)
			return err
		}
	}
	if err := security.RenameRootedFile(tmpDir, dir); err != nil {
		_ = security.RenameRootedFile(backupDir, dir)
		_ = security.RemoveAllRootedPath(tmpDir)
		return err
	}
	if _, err := security.RootedStat(backupDir); err == nil {
		_ = security.RemoveAllRootedPath(backupDir)
	}
	return nil
}
