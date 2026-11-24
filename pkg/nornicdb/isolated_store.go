package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/storage/lifecycle"
)

type isolatedWALEngine struct {
	*storage.WALEngine
	indexesHealthy bool
	lifecycle      *lifecycle.MVCCLifecycleManager
	closeOnce      sync.Once
	closeErr       error
}

func (e *isolatedWALEngine) Close() error {
	e.closeOnce.Do(func() {
		if e.lifecycle != nil {
			e.lifecycle.StopLifecycle()
		}
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
			if err := ctx.Err(); err != nil {
				return "", nil, err
			}
			id, dir, err := storage.CreatePreparedDatabaseDirectory(root, name)
			if err != nil {
				return "", nil, err
			}
			engine, err := db.openIsolatedStore(ctx, dir, name, options, nil, false)
			if err != nil {
				return "", nil, errors.Join(err, os.RemoveAll(dir))
			}
			if err := storage.SealPreparedDatabase(dir, storage.PreparedDatabase{Database: name}); err != nil {
				return "", nil, errors.Join(err, engine.Close(), os.RemoveAll(dir))
			}
			return id, engine, nil
		},
		Open: func(ctx context.Context, name, id string, options map[string]string, verify bool) (storage.Engine, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			readStore := storage.ReadPreparedDatabaseSeal
			if verify {
				readStore = storage.ReadPreparedDatabase
			}
			dir, graph, err := readStore(root, id, name)
			if err != nil {
				return nil, err
			}
			return db.openIsolatedStore(ctx, dir, name, options, &graph, verify)
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

func (db *DB) openIsolatedStore(ctx context.Context, dir, name string, options map[string]string, graph *storage.PreparedDatabase, verify bool) (storage.Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	badgerOpts, walConfig := resolveDurabilityOptions(dir, db.config)
	badgerOpts.MemoryPolicy = db.memoryPolicy
	badgerOpts.EncryptionKey = db.isolatedStoreEncryptionKey
	var native *storage.BadgerEngine
	var err error
	if graph != nil && !verify {
		native, err = openPublishedIsolatedStore(ctx, dir, badgerOpts, graph)
	} else {
		native, err = storage.NewBadgerEngineWithOptions(badgerOpts)
	}
	if err != nil {
		return nil, err
	}
	wal, err := storage.NewWAL(walConfig.Dir, walConfig)
	if err != nil {
		return nil, errors.Join(err, native.Close())
	}
	engine := storage.NewWALEngine(native, wal)
	configureEmbeddingStore(native, db.config)
	native.SetChangeCapture(name, storage.ParseChangeCaptureMode(options[multidb.OptionTxLogEnrichment]))
	if _, err := storage.RecoverBufferedCommitsAtOpen(native, wal); err != nil {
		return nil, errors.Join(err, engine.Close())
	}
	if verify {
		scoped := storage.NewNamespacedEngine(engine, name)
		nodes, nodeErr := scoped.NodeCount(ctx)
		edges, edgeErr := scoped.EdgeCount(ctx)
		if err := errors.Join(nodeErr, edgeErr); err != nil {
			return nil, errors.Join(err, engine.Close())
		}
		if nodes != graph.Nodes || edges != graph.Relationships {
			return nil, errors.Join(fmt.Errorf("prepared graph counts differ from the seal: %w", storage.ErrInvalidData), engine.Close())
		}
	}
	indexesHealthy := prepareDerivedIndexes(ctx, engine)
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, engine.Close())
	}
	if db.config.Database.WALAutoCompactionEnabled {
		if err := engine.EnableAutoCompaction(filepath.Join(dir, "snapshots")); err != nil {
			return nil, errors.Join(err, engine.Close())
		}
	}
	controller := attachStorageLifecycle(db.config, native)
	if controller != nil {
		controller.StartLifecycle(db.LifetimeContext())
	}
	return &isolatedWALEngine{WALEngine: engine, indexesHealthy: indexesHealthy, lifecycle: controller}, nil
}

func openPublishedIsolatedStore(ctx context.Context, dir string, opts storage.BadgerOptions, seal *storage.PreparedDatabase) (*storage.BadgerEngine, error) {
	status, err := readRecoveryManifest(dir)
	if err == nil && status.Phase != RecoveryPhaseComplete {
		return nil, &StoreUnavailableError{Status: status, Cause: errors.New(status.Error)}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	var native *storage.BadgerEngine
	err = storage.VerifyPreparedDatabaseManifest(dir)
	missingManifest := errors.Is(err, os.ErrNotExist)
	if err == nil {
		native, err = storage.NewBadgerEngineWithOptions(opts)
	} else if !missingManifest {
		return nil, err
	}
	if err == nil || !(missingManifest || looksLikeCorruption(err)) || !autoRecoverOnCorruptionEnabled() || !hasRecoverableArtifacts(dir) {
		return native, err
	}
	if len(opts.EncryptionKey) != 0 {
		if keyErr := storage.VerifyBadgerEncryptionKey(dir, opts.EncryptionKey); keyErr != nil {
			return nil, errors.Join(err, keyErr)
		}
	}
	recovered, preserved, recoverErr := recoverBadgerStore(ctx, dir, opts, seal)
	if recoverErr != nil {
		return nil, errors.Join(err, recoverErr)
	}
	log.Printf("Recovered isolated store %q from snapshot and WAL artifacts. Preserved files at %s", seal.Database, preserved)
	return recovered, nil
}

func attachStorageLifecycle(config *Config, native *storage.BadgerEngine) *lifecycle.MVCCLifecycleManager {
	if !config.Database.MVCCLifecycleEnabled {
		return nil
	}
	settings := lifecycle.DefaultLifecycleConfig()
	settings.Enabled = config.Database.MVCCRetentionMaxVersions > 1 || config.Database.MVCCRetentionTTL > 0
	settings.CycleInterval = config.Database.MVCCLifecycleCycleInterval
	settings.MaxVersionsPerKey = config.Database.MVCCRetentionMaxVersions
	settings.TTL = config.Database.MVCCRetentionTTL
	settings.MaxSnapshotLifetime = config.Database.MVCCLifecycleMaxSnapshotAge
	settings.MaxChainHardCap = config.Database.MVCCLifecycleMaxChainCap
	controller := lifecycle.NewMVCCLifecycleManager(settings, native)
	native.SetLifecycleController(controller)
	return controller
}
