package multidb

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// RestoreDatabaseBackup restores a native single-store file or a full archive
// into a new data directory. All stores are verified before the directory is
// made available. A restore never replaces an existing installation.
func RestoreDatabaseBackup(ctx context.Context, source, destination string) error {
	return RestoreDatabaseBackupWithKey(ctx, source, destination, nil)
}

// RestoreDatabaseBackupWithKey provisions the destination's key inside the
// temporary data root. Every restored native store opens with that key.
func RestoreDatabaseBackupWithKey(ctx context.Context, source, destination string, initializeKey func(string) ([]byte, error)) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	destination, err := security.AbsCanonicalFilePath(destination)
	if err != nil {
		return err
	}
	if err := requireNewDataDirectory(destination); err != nil {
		return err
	}
	file, err := security.OpenRootedFile(source, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	archive, err := storage.ReadDatabaseBackupArchive(file.File)
	if err != nil {
		return err
	}
	parent := filepath.Dir(destination)
	stage, err := os.MkdirTemp(parent, ".nornicdb-restore-")
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, os.RemoveAll(stage)) }()
	var key []byte
	if initializeKey != nil {
		key, err = initializeKey(stage)
		if err != nil {
			return err
		}
	}
	if archive != nil && archive.RequiresEncryption && len(key) == 0 {
		return errors.New("database backup requires encryption settings for the restored data directory")
	}

	var root io.Reader = file
	var members map[string]*zip.File
	if archive != nil {
		members = archive.Stores
		member, err := members[""].Open()
		if err != nil {
			return err
		}
		defer member.Close()
		root = member
	}
	var catalog DatabaseManager
	if err := restoreNativeStore(ctx, stage, key, root, func(engine *storage.BadgerEngine) error {
		catalog.inner, catalog.config = engine, DefaultConfig()
		_, err := catalog.loadMetadata(ctx)
		catalog.inner = nil
		return err
	}); err != nil {
		return fmt.Errorf("restoring root database store: %w", err)
	}
	if err := restoreDedicatedStores(ctx, stage, key, &catalog, members); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := storage.SyncDir(stage); err != nil {
		return err
	}
	if err := requireNewDataDirectory(destination); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := security.RenameRootedFile(stage, destination); err != nil {
		return err
	}
	return storage.SyncDir(parent)
}

func requireNewDataDirectory(path string) error {
	_, err := os.Lstat(path)
	if err == nil {
		return fmt.Errorf("restore data directory already exists: %w", os.ErrExist)
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func restoreDedicatedStores(ctx context.Context, stage string, key []byte, catalog *DatabaseManager, members map[string]*zip.File) error {
	delete(members, "")
	for name, database := range catalog.databases {
		if database == nil || database.IsolatedStoreID == "" || catalog.pendingDrops[name] {
			continue
		}
		id := database.IsolatedStoreID
		member := members[id]
		if member == nil {
			return fmt.Errorf("backup is missing the store for database %q: %w", name, storage.ErrInvalidData)
		}
		if err := restoreDedicatedStore(ctx, stage, name, id, key, member); err != nil {
			return fmt.Errorf("restoring database %q: %w", name, err)
		}
		delete(members, id)
	}
	if len(members) != 0 {
		return fmt.Errorf("backup contains stores outside its catalog: %w", storage.ErrInvalidData)
	}
	return nil
}

func restoreDedicatedStore(ctx context.Context, stage, name, id string, key []byte, member *zip.File) error {
	dir, err := storage.PreparedDatabaseDirectory(stage, id)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	src, err := member.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	graph := storage.PreparedDatabase{Database: name}
	if err := restoreNativeStore(ctx, dir, key, src, func(engine *storage.BadgerEngine) error {
		view := storage.NewNamespacedEngine(engine, name)
		nodes, nodeErr := view.NodeCount(ctx)
		edges, edgeErr := view.EdgeCount(ctx)
		graph.Nodes, graph.Relationships = nodes, edges
		return errors.Join(nodeErr, edgeErr)
	}); err != nil {
		return err
	}
	return storage.SealPreparedDatabase(dir, graph)
}

func restoreNativeStore(ctx context.Context, dir string, key []byte, src io.Reader, inspect func(*storage.BadgerEngine) error) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, HighPerformance: true, EncryptionKey: key})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, engine.Close()) }()
	if err := engine.RestoreFrom(ctx, offlineRestoreReader{ctx: ctx, source: src}); err != nil {
		return err
	}
	if err := inspect(engine); err != nil {
		return err
	}
	return engine.Sync()
}

// The temporary store can be discarded if its load stops partway through.
type offlineRestoreReader struct {
	ctx    context.Context
	source io.Reader
}

func (r offlineRestoreReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(data)
}
