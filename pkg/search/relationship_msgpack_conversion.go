package search

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/orneryd/nornicdb/internal/gobcodec"
	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/vmihailenco/msgpack/v5"
)

// writeRelationshipSnapshot owns both regular saves and migrated snapshots.
// The exclusive temporary file is in the destination directory, so a stale
// fixed-name symlink cannot redirect writes and a successful rename is atomic.
func writeRelationshipSnapshot(path string, snapshot *relationshipVectorsFile, perm os.FileMode, syncDirectory func(string) error) error {
	canonical, err := security.AbsCanonicalFilePath(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(canonical)
	file, err := os.CreateTemp(dir, ".relationship-vectors-*.tmp")
	if err != nil {
		return err
	}
	temp := file.Name()
	renamed := false
	defer func() {
		if !renamed {
			_ = security.RemoveRootedPath(temp)
		}
	}()
	if err = file.Chmod(perm.Perm()); err == nil {
		writer := bufio.NewWriterSize(file, 64*1024)
		err = encodeRelationshipSnapshot(writer, snapshot)
		if err == nil {
			err = writer.Flush()
		}
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = security.RenameRootedFile(temp, canonical); err != nil {
		return err
	}
	renamed = true
	if err = syncDirectory(dir); err != nil {
		return fmt.Errorf("sync relationship snapshot directory: %w", err)
	}
	return nil
}

// ErrRelationshipSnapshotUpgradeRequired means a persisted relationship
// snapshot still uses the historical gob format.
var ErrRelationshipSnapshotUpgradeRequired = errors.New("relationship snapshot upgrade required; back up the data and restart with --upgrade-storage")

// UpgradeRelationshipSnapshot validates and, when authorized, converts a
// historical relationship-vector snapshot to MessagePack. syncDirectory may
// be nil to use the platform's default directory durability operation. A
// successful rename followed by a failed sync is retried on the next call.
// The caller must stop services writing the file before conversion. A missing
// file is a no-op; an unauthorized gob file returns
// ErrRelationshipSnapshotUpgradeRequired and remains unchanged.
//
//	err := UpgradeRelationshipSnapshot("data/search/nornic/bm25.v2.relationships", true, nil)
func UpgradeRelationshipSnapshot(path string, allowUpgrade bool, syncDirectory func(string) error) error {
	if syncDirectory == nil {
		syncDirectory = storage.SyncDir
	}
	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	stored, already, err := decodeRelationshipSnapshot(file)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if already {
		canonical, err := security.AbsCanonicalFilePath(path)
		if err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(canonical)); err != nil {
			return fmt.Errorf("sync relationship snapshot directory: %w", err)
		}
		return nil
	}
	if !allowUpgrade {
		return fmt.Errorf("%w: %s", ErrRelationshipSnapshotUpgradeRequired, path)
	}
	return writeRelationshipSnapshot(path, &stored, info.Mode(), syncDirectory)
}

func decodeRelationshipSnapshot(file io.ReadSeeker) (relationshipVectorsFile, bool, error) {
	var stored relationshipVectorsFile
	reader := bufio.NewReader(file)
	decoder := msgpack.NewDecoder(reader)
	decoder.DisallowUnknownFields(true)
	if err := decoder.Decode(&stored); err == nil {
		if stored.Format != 1 && stored.Format != relationshipVectorsFormat {
			return relationshipVectorsFile{}, false, fmt.Errorf("unsupported relationship snapshot format %d", stored.Format)
		}
		if _, err := reader.ReadByte(); err == nil {
			return relationshipVectorsFile{}, false, errors.New("trailing data after relationship snapshot")
		} else if !errors.Is(err, io.EOF) {
			return relationshipVectorsFile{}, false, err
		}
		return stored, true, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return relationshipVectorsFile{}, false, err
	}
	stored = relationshipVectorsFile{}
	if err := gobcodec.Decode(file, &stored, gobcodec.RejectTrailing); err != nil {
		return relationshipVectorsFile{}, false, fmt.Errorf("invalid relationship snapshot: %w", err)
	}
	if stored.Format != 1 && stored.Format != relationshipVectorsFormat {
		return relationshipVectorsFile{}, false, fmt.Errorf("unsupported relationship snapshot format %d", stored.Format)
	}
	return stored, false, nil
}

// convertRelationshipSnapshotDir handles the offline converter's direct and
// generation layouts. Search generation selection belongs to the DB owner.
func convertRelationshipSnapshotDir(dbDir string) error {
	entries, err := os.ReadDir(dbDir)
	if err != nil {
		return err
	}
	dirs := []string{dbDir}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "g-") && len(entry.Name()) == 18 {
			dirs = append(dirs, filepath.Join(dbDir, entry.Name()))
		}
	}
	for _, dir := range dirs {
		path := filepath.Join(dir, "bm25.v2"+relationshipVectorsFileSuffix)
		if err := UpgradeRelationshipSnapshot(path, true, nil); err != nil {
			return fmt.Errorf("relationship vectors %s: %w", path, err)
		}
	}
	return nil
}
