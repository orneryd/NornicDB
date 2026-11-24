package storage

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/security"
)

const backupArchivePrefix = "nornicdb-backup-v1/"
const backupSharedMember = backupArchivePrefix + "root.badger"
const backupArchiveMagic = "NORNICDB-BACKUP\n"
const backupArchiveEncryptedSource = "encrypted-source"

// ErrOfflineDatabaseRestore identifies a restore that requires a new data directory.
var ErrOfflineDatabaseRestore = errors.New("restore this backup into a new data directory with nornicdb-admin database restore")

// WriteDatabaseBackupArchive writes a versioned collection of native Badger
// streams. The empty key identifies the root, which includes the catalog and
// all shared databases. Other keys are the catalog's physical store IDs.
func WriteDatabaseBackupArchive(ctx context.Context, path string, stores map[string]Engine) error {
	if stores[""] == nil {
		return fmt.Errorf("database backup has no root store: %w", ErrInvalidData)
	}
	path, err := security.AbsCanonicalFilePath(path)
	if err != nil {
		return err
	}
	temporary := filepath.Join(filepath.Dir(path), ".nornicdb-backup-"+uuid.NewString())
	file, err := security.OpenRootedFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer security.RemoveRootedPath(temporary)
	if _, err := file.WriteString(backupArchiveMagic); err != nil {
		return errors.Join(err, file.Close())
	}
	archive := zip.NewWriter(file)
	if root, ok := UnwrapEngine(stores[""]).(*BadgerEngine); ok && root.encryptedStorage() {
		if err := archive.SetComment(backupArchiveEncryptedSource); err != nil {
			return errors.Join(err, file.Close())
		}
	}
	for id, engine := range stores {
		if err = ctx.Err(); err != nil {
			break
		}
		name, nameErr := backupMemberName(id)
		if nameErr != nil {
			err = nameErr
			break
		}
		writer, createErr := archive.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
		if createErr != nil {
			err = createErr
			break
		}
		backup, ok := engine.(interface {
			BackupTo(context.Context, io.Writer) error
		})
		if !ok {
			err = fmt.Errorf("backing up %s: %w", name, ErrNotImplemented)
			break
		}
		if backupErr := backup.BackupTo(ctx, writer); backupErr != nil {
			err = fmt.Errorf("backing up %s: %w", name, backupErr)
			break
		}
	}
	if err := errors.Join(err, archive.Close()); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := errors.Join(ctx.Err(), file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := security.RenameRootedFile(temporary, path); err != nil {
		return err
	}
	return SyncDir(filepath.Dir(path))
}

// DatabaseBackupArchive holds native streams keyed by physical store ID.
// The empty ID identifies the root store.
type DatabaseBackupArchive struct {
	Stores             map[string]*zip.File
	RequiresEncryption bool
}

// ReadDatabaseBackupArchive opens the archive. A nil result identifies a legacy file.
func ReadDatabaseBackupArchive(file *os.File) (*DatabaseBackupArchive, error) {
	var header [len(backupArchiveMagic)]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	if string(header[:]) != backupArchiveMagic {
		return nil, nil
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size() - int64(len(header))
	archive, err := zip.NewReader(io.NewSectionReader(file, int64(len(header)), size), size)
	if err != nil {
		return nil, err
	}
	stores := make(map[string]*zip.File, len(archive.File))
	for _, member := range archive.File {
		id := ""
		if member.Name != backupSharedMember {
			id = strings.TrimSuffix(strings.TrimPrefix(member.Name, backupArchivePrefix), ".badger")
		}
		name, err := backupMemberName(id)
		if err != nil || name != member.Name || stores[id] != nil || !member.Mode().IsRegular() {
			return nil, fmt.Errorf("invalid database backup member %q: %w", member.Name, ErrInvalidData)
		}
		stores[id] = member
	}
	if stores[""] == nil {
		return nil, fmt.Errorf("database backup is missing the root store: %w", ErrInvalidData)
	}
	return &DatabaseBackupArchive{Stores: stores, RequiresEncryption: archive.Comment == backupArchiveEncryptedSource}, nil
}

func (b *BadgerEngine) encryptedStorage() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.db != nil && len(b.db.Opts().EncryptionKey) != 0
}

func backupMemberName(id string) (string, error) {
	if id == "" {
		return backupSharedMember, nil
	}
	if _, err := PreparedDatabaseDirectory("", id); err != nil {
		return "", err
	}
	return backupArchivePrefix + id + ".badger", nil
}
