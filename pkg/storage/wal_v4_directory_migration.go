package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

const v4WALBackupComplete = ".nornic-v4-wal-backup-complete"
const v4WALBackupSuffix = ".before-v4-v6"

// WALV4UpgradeOptions selects an optional application payload conversion.
// Transform is invoked only for records using an older frame and must return
// the complete replacement payload without modifying its input.
//
// OneFrameRecords keeps the converted journal to one-frame records: an old record
// whose converted form does not fit one frame is refused, in every historical format,
// by validation before anything changes unless it is a property-epoch control or its
// conversion gives other bytes the second time. The replication journal sets it, as
// its transport carries one-frame records only; a store journal carries such a record whole.
type WALV4UpgradeOptions struct {
	Transform       func(version uint8, operation OperationType, payload []byte) ([]byte, error)
	OneFrameRecords bool
	afterPublish    func(string) error
}

// records is the conversion this upgrade applies to every file of the
// journal, in validation and in conversion alike.
func (o WALV4UpgradeOptions) records() *v4UpgradeRecords {
	policy := WALRecordsLarge
	if o.OneFrameRecords {
		policy = WALRecordsStandard
	}
	return &v4UpgradeRecords{policy: policy, transform: o.Transform}
}

// stageV4WALBackup copies source into a durable, immutable staging directory
// beside backup and returns its path. The copy is synced and marked complete,
// but it is not published: UpgradeWALV4 renames it to backup only after every
// file of the journal has converted, so a conversion that fails changes no file
// and leaves no backup. Only its own staging directory is removed here.
func stageV4WALBackup(source, backup string) (stage string, resultErr error) {
	if _, err := os.Lstat(backup); err == nil {
		return "", fmt.Errorf("wal: V4 backup already exists: %s", backup)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	stage, err := os.MkdirTemp(filepath.Dir(backup), ".wal-v4-backup-*")
	if err != nil {
		return "", err
	}
	defer func(allocatedStage string) {
		if resultErr != nil {
			_ = os.RemoveAll(allocatedStage)
		}
	}(stage)
	var directories []string
	err = filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if rel == v4WALBackupComplete {
			return fmt.Errorf("wal: reserved backup marker already exists")
		}
		dest := filepath.Join(stage, rel)
		if entry.IsDir() {
			if rel != "." {
				if err := os.Mkdir(dest, 0700); err != nil {
					return err
				}
			}
			directories = append(directories, dest)
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("wal: cannot back up non-regular file %q", path)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			in.Close()
			return err
		}
		_, copyErr := io.Copy(out, in)
		if copyErr == nil {
			copyErr = out.Sync()
		}
		copyErr = errors.Join(copyErr, in.Close(), out.Close())
		if copyErr != nil {
			return copyErr
		}
		equal, err := v4WALFilesEqual(path, dest)
		if err != nil {
			return err
		}
		if !equal {
			return fmt.Errorf("wal: source changed during V4 backup: %s", path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	marker, err := os.OpenFile(filepath.Join(stage, v4WALBackupComplete), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	if err := errors.Join(marker.Sync(), marker.Close()); err != nil {
		return "", err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := syncDir(directories[i]); err != nil {
			return "", err
		}
	}
	return stage, nil
}

func v4WALFilesEqual(left, right string) (bool, error) {
	first, err := os.Open(left)
	if err != nil {
		return false, err
	}
	defer first.Close()
	second, err := os.Open(right)
	if err != nil {
		return false, err
	}
	defer second.Close()
	firstInfo, err := first.Stat()
	if err != nil {
		return false, err
	}
	secondInfo, err := second.Stat()
	if err != nil {
		return false, err
	}
	if !firstInfo.Mode().IsRegular() || !secondInfo.Mode().IsRegular() {
		return false, fmt.Errorf("wal: V4 comparison requires regular files")
	}
	if firstInfo.Size() != secondInfo.Size() {
		return false, nil
	}
	a, b := sha256.New(), sha256.New()
	if _, err := io.Copy(a, first); err != nil {
		return false, err
	}
	if _, err := io.Copy(b, second); err != nil {
		return false, err
	}
	return string(a.Sum(nil)) == string(b.Sum(nil)), nil
}

// UpgradeWALV4 stages every converted file before publication, and publishes
// the durable pre-upgrade backup only once every file has converted. A crash
// after any replacement is resumed from that backup; an externally changed
// file is rejected rather than overwritten, and a failed conversion leaves
// every source file as it was with no backup published.
func UpgradeWALV4(dir string, options WALV4UpgradeOptions) (bool, error) {
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return false, fmt.Errorf("wal: V4 source %q is not a directory", dir)
	}
	if err := ValidateWALFormat(dir, WALValidateCurrent); err == nil {
		return false, nil
	} else if !errors.Is(err, ErrWALUnsupportedVersion) {
		return false, err
	}
	records := options.records()
	if err := validateWALFormat(dir, WALValidateV4Upgrade, records, walFrameReader{}); err != nil {
		return false, err
	}
	backup := dir + v4WALBackupSuffix
	// Every file is converted from one immutable copy of the journal: the
	// published pre-upgrade backup when an earlier attempt made one, or a
	// staged copy of the current files. The staged copy becomes the backup only
	// after every file has converted, so a conversion that fails changes no
	// file and publishes no backup.
	source, backedUp := backup, true
	if _, err := os.Lstat(backup); os.IsNotExist(err) {
		staged, err := stageV4WALBackup(dir, backup)
		if err != nil {
			return false, fmt.Errorf("wal: V4 backup: %w", err)
		}
		defer os.RemoveAll(staged)
		source, backedUp = staged, false
	} else if err != nil {
		return false, err
	} else if marker, err := os.Lstat(filepath.Join(backup, v4WALBackupComplete)); err != nil || !marker.Mode().IsRegular() {
		return false, fmt.Errorf("wal: existing V4 backup is incomplete")
	}
	manifest, err := accessWALManifest(source, walReadManifest, nil)
	if err != nil {
		return false, err
	}
	current, err := accessWALManifest(dir, walReadManifest, nil)
	if err != nil {
		return false, err
	}
	if len(current.Segments) != len(manifest.Segments) {
		return false, fmt.Errorf("wal: segment inventory changed since V4 backup")
	}
	for i := range manifest.Segments {
		if current.Segments[i].Path != manifest.Segments[i].Path {
			return false, fmt.Errorf("wal: segment inventory changed since V4 backup")
		}
	}
	for _, rel := range []string{filepath.Join("segments", "manifest.json"), "wal.log"} {
		original, prior := filepath.Join(dir, rel), filepath.Join(source, rel)
		_, originalErr := os.Lstat(original)
		_, priorErr := os.Lstat(prior)
		if (originalErr == nil) != (priorErr == nil) || originalErr != nil && !os.IsNotExist(originalErr) || priorErr != nil && !os.IsNotExist(priorErr) {
			return false, fmt.Errorf("wal: file inventory changed since V4 backup: %s", rel)
		}
		if rel != "wal.log" && originalErr == nil {
			equal, err := v4WALFilesEqual(original, prior)
			if err != nil {
				return false, err
			}
			if !equal {
				return false, fmt.Errorf("wal: manifest changed since V4 backup")
			}
		}
	}
	var paths []string
	for _, segment := range manifest.Segments {
		if _, err := resolveWALSegmentPath(source, segment.Path); err != nil {
			return false, err
		}
		paths = append(paths, filepath.Join("segments", segment.Path))
	}
	sort.Strings(paths)
	if _, err := os.Stat(walActivePath(source)); err == nil {
		paths = append(paths, "wal.log")
	} else if !os.IsNotExist(err) {
		return false, err
	}
	stage, err := os.MkdirTemp(filepath.Dir(dir), ".wal-v4-upgrade-*")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(stage)
	for _, rel := range paths {
		dest := filepath.Join(stage, rel)
		if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
			return false, err
		}
		if err := upgradeWALFileV4(filepath.Join(source, rel), dest, rel == "wal.log", records); err != nil {
			return false, fmt.Errorf("wal: convert %s: %w", rel, err)
		}
		original, converted := filepath.Join(dir, rel), dest
		matchesOriginal, err := v4WALFilesEqual(original, filepath.Join(source, rel))
		if err != nil {
			return false, err
		}
		matchesConverted, err := v4WALFilesEqual(original, converted)
		if err != nil {
			return false, err
		}
		if !matchesOriginal && !matchesConverted {
			return false, fmt.Errorf("wal: source changed since V4 backup: %s", rel)
		}
	}
	if !backedUp {
		if _, err := os.Lstat(backup); err == nil {
			return false, fmt.Errorf("wal: V4 backup already exists: %s", backup)
		} else if !os.IsNotExist(err) {
			return false, err
		}
		if err := os.Rename(source, backup); err != nil {
			return false, err
		}
		if err := syncDir(filepath.Dir(backup)); err != nil {
			return false, err
		}
	}
	for _, rel := range paths {
		original, converted := filepath.Join(dir, rel), filepath.Join(stage, rel)
		matchesOriginal, err := v4WALFilesEqual(original, filepath.Join(backup, rel))
		if err != nil {
			return false, err
		}
		matchesConverted, err := v4WALFilesEqual(original, converted)
		if err != nil {
			return false, err
		}
		if !matchesOriginal && !matchesConverted {
			return false, fmt.Errorf("wal: source changed during V4 conversion: %s", rel)
		}
		if !matchesConverted {
			if err := os.Rename(converted, original); err != nil {
				return false, err
			}
			if err := syncDir(filepath.Dir(original)); err != nil {
				return false, err
			}
		}
		if options.afterPublish != nil {
			if err := options.afterPublish(rel); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

// The pending key lives in the existing metadata key family. It is deleted in
// the same Badger transaction that commits the V4 catalog image and version.
var v4WALRecoveryKey = append([]byte{prefixMVCCMeta}, []byte("wal-v4-upgrade-pending")...)
var v4WALFormatRecoveryKey = append([]byte{prefixMVCCMeta}, []byte("wal-v4-msgpack-upgrade-pending")...)

func (b *BadgerEngine) v4WALRecoveryState() (catalogPending, formatPending bool, err error) {
	err = b.db.View(func(tx *badger.Txn) error {
		for _, item := range []struct {
			key     []byte
			present *bool
		}{{v4WALRecoveryKey, &catalogPending}, {v4WALFormatRecoveryKey, &formatPending}} {
			_, getErr := tx.Get(item.key)
			if getErr == badger.ErrKeyNotFound {
				continue
			}
			if getErr != nil {
				return getErr
			}
			*item.present = true
		}
		return nil
	})
	if err == nil && catalogPending && formatPending {
		err = fmt.Errorf("wal: conflicting V4 recovery markers")
	}
	return catalogPending, formatPending, err
}

// walV5OrLaterOnly identifies the format-only upgrade for a store whose V4
// catalog has already been committed. Earlier frames still require the V4
// catalog replay/transform ordering.
func walV5OrLaterOnly(dir string) (bool, error) {
	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	if err != nil {
		return false, err
	}
	seen, older := false, false
	visit := func(entry WALEntry, _ *walPropertyEpochFrame, _, _ uint64) error {
		seen = true
		older = older || entry.framingVersion < 5
		return nil
	}
	for _, segment := range manifest.Segments {
		path, err := resolveWALSegmentPath(dir, segment.Path)
		if err != nil {
			return false, err
		}
		if err := scanV4MigrationWAL(path, false, true, visit); err != nil {
			return false, err
		}
	}
	active := walActivePath(dir)
	if _, err := os.Stat(active); err == nil {
		if err := scanV4MigrationWAL(active, true, true, visit); err != nil {
			return false, err
		}
	} else if !os.IsNotExist(err) {
		return false, err
	}
	return seen && !older, nil
}

// recoverV4WALAtOpen runs only during the unpublished startup phase. The
// converted WAL is replayed before the V4 catalog transaction; normal flush
// markers and the durable migration marker make interruption retries safe.
func (b *BadgerEngine) recoverV4WALAtOpen(allowUpgrade, pending, formatOnly bool) (bool, error) {
	dir := filepath.Join(b.dataDir, "wal")
	if _, err := os.Lstat(dir); os.IsNotExist(err) {
		if pending {
			return false, fmt.Errorf("wal: V4 recovery marker exists but WAL directory is missing")
		}
		return false, nil
	} else if err != nil {
		return false, err
	}
	mode := WALValidateCurrent
	if allowUpgrade || pending {
		mode = WALValidateV4Upgrade
	}
	if err := ValidateWALFormat(dir, mode); err != nil {
		return false, err
	}
	if !pending {
		key := v4WALRecoveryKey
		if formatOnly {
			key = v4WALFormatRecoveryKey
		}
		if err := b.db.Update(func(tx *badger.Txn) error { return tx.Set(key, []byte{1}) }); err != nil {
			return false, err
		}
		if err := b.db.Sync(); err != nil {
			return false, err
		}
	}
	if allowUpgrade || pending {
		if _, err := UpgradeWALV4(dir, WALV4UpgradeOptions{}); err != nil {
			return false, err
		}
	}
	cfg := DefaultWALConfig()
	cfg.SyncMode, cfg.BatchSyncInterval = "none", 0
	wal, err := NewWAL(dir, cfg)
	if err != nil {
		return false, err
	}
	replayed, replayErr := RecoverBufferedCommitsAtOpen(b, wal)
	closeErr := wal.Close()
	if replayErr != nil || closeErr != nil {
		return false, errors.Join(replayErr, closeErr)
	}
	if formatOnly {
		if err := b.db.Update(func(tx *badger.Txn) error { return tx.Delete(v4WALFormatRecoveryKey) }); err != nil {
			return false, err
		}
		if err := b.db.Sync(); err != nil {
			return false, err
		}
	}
	if replayed > 0 && b.log != nil {
		b.log.Info("recovered WAL before V4 migration", "commits", replayed)
	}
	return true, nil
}

// finalizeV4WALBackup keeps the original files for operator rollback after the
// V4 transaction is durable. A still-present pending name is safe to retry.
func finalizeV4WALBackup(db *managedBadgerDB, dir string) error {
	backup := dir + v4WALBackupSuffix
	info, err := os.Lstat(backup)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("wal: V4 backup is not a directory")
	}
	marker, err := os.Lstat(filepath.Join(backup, v4WALBackupComplete))
	if err != nil || !marker.Mode().IsRegular() {
		return fmt.Errorf("wal: V4 backup is incomplete")
	}
	if err := db.Sync(); err != nil {
		return err
	}
	archive := dir + ".pre-v4-" + uuid.NewString()
	if _, err := os.Lstat(archive); err == nil {
		return fmt.Errorf("wal: V4 backup archive already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(backup, archive); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}
