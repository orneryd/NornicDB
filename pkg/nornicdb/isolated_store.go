package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type isolatedWALEngine struct {
	*storage.WALEngine
	indexesHealthy bool
	closeOnce      sync.Once
	closeErr       error
}

func (e *isolatedWALEngine) Close() error {
	e.closeOnce.Do(func() {
		markerErr := markStorageCleanShutdown(context.Background(), e.WALEngine, e.indexesHealthy)
		e.closeErr = errors.Join(markerErr, e.WALEngine.Close())
	})
	return e.closeErr
}

// SetIsolatedStoresClose retires dedicated stores after serving and background work have stopped.
func (db *DB) SetIsolatedStoresClose(closeStores func() error) {
	db.dbConfigResolverMu.Lock()
	db.closeIsolatedStores = closeStores
	db.dbConfigResolverMu.Unlock()
}

// IsolatedStoreConfig supplies a separate Badger and WAL pair to the catalog
// owner on persistent standalone servers. The catalog stays in the root store.
func (db *DB) IsolatedStoreConfig() *multidb.IsolatedStoreConfig {
	if db.config == nil || db.config.Database.DataDir == "" || db.cluster == nil || db.cluster.mode != replication.ModeStandalone {
		return nil
	}
	root := db.config.Database.DataDir
	return &multidb.IsolatedStoreConfig{
		Create: func(ctx context.Context, name string, options map[string]string) (string, storage.Engine, error) {
			id, dir, err := storage.CreatePreparedDatabaseDirectory(root, name)
			if err != nil {
				return "", nil, err
			}
			engine, err := db.openIsolatedStore(ctx, dir, name, options, nil)
			if err != nil {
				return "", nil, errors.Join(err, os.RemoveAll(dir))
			}
			if err := storage.SealPreparedDatabase(dir, storage.PreparedDatabase{Database: name}); err != nil {
				return "", nil, errors.Join(err, engine.Close(), os.RemoveAll(dir))
			}
			return id, engine, nil
		},
		Open: func(ctx context.Context, name, id string, options map[string]string, verify bool) (storage.Engine, error) {
			// CSV staging currently produces plaintext files. A root opened
			// with encryption must not accept those files.
			if verify && db.encryptionEnabled {
				return nil, fmt.Errorf("prepared CSV imports require an unencrypted store: %w", storage.ErrNotImplemented)
			}
			dir, graph, err := storage.ReadPreparedDatabase(root, id, name)
			if err != nil {
				return nil, err
			}
			var expected *storage.PreparedDatabase
			if verify {
				expected = &graph
			}
			return db.openIsolatedStore(ctx, dir, name, options, expected)
		},
		Drop: func(id string) error {
			dir, err := storage.PreparedDatabaseDirectory(root, id)
			if err != nil {
				return err
			}
			parent := filepath.Dir(dir)
			info, err := os.Lstat(parent)
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			if !info.IsDir() {
				return storage.ErrInvalidData
			}
			if err := os.RemoveAll(dir); err != nil {
				return err
			}
			return storage.SyncDir(parent)
		},
	}
}

func (db *DB) openIsolatedStore(ctx context.Context, dir, name string, options map[string]string, expected *storage.PreparedDatabase) (storage.Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	badgerOpts, walConfig := resolveDurabilityOptions(dir, db.config)
	badgerOpts.MemoryPolicy = db.memoryPolicy
	badgerOpts.EncryptionKey = db.isolatedStoreEncryptionKey
	native, err := storage.NewBadgerEngineWithOptions(badgerOpts)
	if err != nil {
		return nil, err
	}
	wal, err := storage.NewWAL(walConfig.Dir, walConfig)
	if err != nil {
		return nil, errors.Join(err, native.Close())
	}
	engine := storage.NewWALEngine(native, wal)
	native.SetChangeCapture(name, storage.ParseChangeCaptureMode(options[multidb.OptionTxLogEnrichment]))
	if _, err := storage.RecoverBufferedCommitsAtOpen(native, wal); err != nil {
		return nil, errors.Join(err, engine.Close())
	}
	if expected != nil {
		scoped := storage.NewNamespacedEngine(engine, name)
		nodes, nodeErr := scoped.NodeCount(ctx)
		edges, edgeErr := scoped.EdgeCount(ctx)
		if err := errors.Join(nodeErr, edgeErr); err != nil {
			return nil, errors.Join(err, engine.Close())
		}
		if nodes != expected.Nodes || edges != expected.Relationships {
			return nil, errors.Join(fmt.Errorf("prepared graph counts differ from the seal: %w", storage.ErrInvalidData), engine.Close())
		}
	}
	indexesHealthy := prepareDerivedIndexes(ctx, engine)
	if db.config.Database.WALAutoCompactionEnabled {
		if err := engine.EnableAutoCompaction(filepath.Join(dir, "snapshots")); err != nil {
			return nil, errors.Join(err, engine.Close())
		}
	}
	return &isolatedWALEngine{WALEngine: engine, indexesHealthy: indexesHealthy}, nil
}
