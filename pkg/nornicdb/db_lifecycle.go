package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Close closes the database.
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closeDone != nil {
		done := db.closeDone
		db.mu.Unlock()
		<-done
		db.mu.RLock()
		err := db.closeErr
		db.mu.RUnlock()
		return err
	}
	db.closed = true
	db.closeDone = make(chan struct{})
	db.mu.Unlock()

	err := db.closeInternal()
	db.mu.Lock()
	db.closeErr = err
	close(db.closeDone)
	db.mu.Unlock()
	return err
}

// LifetimeContext returns a context that is cancelled when the database
// closes, for background work that must stop with it.
func (db *DB) LifetimeContext() context.Context {
	if db == nil || db.buildCtx == nil {
		return context.Background()
	}
	return db.buildCtx
}

// MemoryPolicy returns the database lifetime's opt-in application admission
// owner. Nil means no application soft limit was configured.
func (db *DB) MemoryPolicy() *memory.Policy {
	if db == nil {
		return nil
	}
	return db.memoryPolicy
}

// HealthCheck reports whether the underlying storage engine is responsive.
func (db *DB) HealthCheck(ctx context.Context) error {
	if db == nil {
		return localizedError(localization.NornicDBCoreNilDatabase(), nil)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if db.storage == nil {
		return localizedError(localization.NornicDBCoreStorageEngineNotInitialized(), nil)
	}
	if _, err := db.storage.NodeCount(); err != nil {
		return localizedError(localization.NornicDBCoreStorageProbeFailed(err), err)
	}
	return nil
}

func (db *DB) retentionPoliciesPath() string {
	if db == nil || db.config == nil {
		return "retention-policies.json"
	}
	if path := strings.TrimSpace(db.config.Retention.PoliciesFile); path != "" {
		return path
	}
	dataDir := strings.TrimSpace(db.config.Database.DataDir)
	if dataDir == "" {
		return "retention-policies.json"
	}
	return filepath.Join(dataDir, "retention-policies.json")
}

// closeInternal performs cleanup without requiring the lock.
// Used during initialization failures and normal close.
func (db *DB) closeInternal() error {
	// SetEmbedder refuses once closed is set, so embedQueue is final here.
	db.mu.Lock()
	db.closed = true
	embedQueue := db.embedQueue
	db.mu.Unlock()

	db.stopClusteringTimer()
	if db.buildCancel != nil {
		db.buildCancel()
	}
	if embedQueue != nil {
		embedQueue.Close()
	}
	db.bgWg.Wait()
	db.foregroundWg.Wait()
	if db.cypherExecutor != nil {
		db.cypherExecutor.StatementCaches().Dispose()
	}

	// Queue workers, initializers and foreground calls are now quiescent. Release all provider
	// resources through their one owner, including cached/replaced providers.
	errs := db.closeOwnedEmbedders()
	// Stop every component that can still mutate graph storage before recording
	// the durable storage boundary. Search snapshots are derived artifacts and
	// may take much longer to save; they must not delay this marker.
	if db.accessFlusher != nil {
		db.accessFlusher.Stop()
	}
	if db.replicator != nil {
		if err := db.replicator.Shutdown(); err != nil {
			errs = append(errs, err)
		}
	}
	if db.replicationTrans != nil {
		if err := db.replicationTrans.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if db.replicationAdapter != nil {
		if err := db.replicationAdapter.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if db.retentionManager != nil && db.config != nil {
		if err := db.retentionManager.SavePolicies(db.retentionPoliciesPath()); err != nil {
			log.Printf("⚠️  Failed to save retention policies: %v", err)
		}
	}
	var markerEngine storage.Engine
	if len(errs) == 0 {
		markerEngine = db.baseStorage
	}
	markErr := persistDerivedAfterStorageBoundary(markerEngine, db.derivedIndexesHealthy, func() {
		if db.config == nil || !db.config.Database.PersistSearchIndexes || db.config.Database.DataDir == "" {
			return
		}
		db.searchServicesMu.RLock()
		defer db.searchServicesMu.RUnlock()
		for _, entry := range db.searchServices {
			if entry != nil && entry.svc != nil {
				entry.svc.PersistIndexesToDisk()
			}
		}
	})
	if markErr != nil {
		errs = append(errs, markErr)
	}

	db.searchServicesMu.RLock()
	for _, entry := range db.searchServices {
		if entry != nil && entry.svc != nil {
			if err := entry.svc.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	db.searchServicesMu.RUnlock()
	db.searchContinuationMu.Lock()
	if db.searchContinuation != nil {
		db.searchContinuation.Close()
		db.searchContinuation = nil
	}
	db.searchContinuationMu.Unlock()

	if db.baseStorage != nil {
		if err := db.baseStorage.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return localizedError(localization.NornicDBCoreCloseFailed(fmt.Sprint(errs)), errors.Join(errs...))
	}
	return nil
}

// persistDerivedAfterStorageBoundary records the durable graph-storage state
// before starting potentially slow derived-index persistence. The marker does
// not claim search snapshots are current; each search snapshot validates and
// rebuilds independently when absent or incompatible.
func persistDerivedAfterStorageBoundary(engine storage.Engine, derivedIndexesHealthy bool, persist func()) error {
	err := markStorageCleanShutdown(engine, derivedIndexesHealthy)
	if persist != nil {
		persist()
	}
	return err
}

// prepareDerivedIndexes consumes the one-use clean-shutdown marker before
// traffic can start. Missing or invalid state conservatively rebuilds.
func prepareDerivedIndexes(ctx context.Context, engine storage.Engine) bool {
	if state, ok := engine.(storage.StartupMaintenanceStateEngine); ok {
		clean, err := state.ConsumeCleanShutdownMarker(ctx)
		if err != nil && !errors.Is(err, storage.ErrNotImplemented) {
			log.Printf("⚠️  Clean-shutdown state check failed; rebuilding derived indexes: %v", err)
		} else if clean {
			log.Printf("✅ Clean shutdown detected; derived storage indexes are current")
			return true
		}
	}

	healthy := true
	if maint, ok := engine.(storage.TemporalMaintenanceEngine); ok {
		log.Printf("🕰️ Rebuilding temporal indexes before serving traffic...")
		if err := maint.RebuildTemporalIndexes(ctx); err != nil && !errors.Is(err, storage.ErrNotImplemented) {
			log.Printf("⚠️  Temporal index rebuild failed: %v", err)
			healthy = false
		}
	}
	if maint, ok := engine.(storage.MVCCMaintenanceEngine); ok {
		log.Printf("🧾 Rebuilding MVCC heads before serving traffic...")
		if err := maint.RebuildMVCCHeads(ctx); err != nil && !errors.Is(err, storage.ErrNotImplemented) {
			log.Printf("⚠️  MVCC head rebuild failed: %v", err)
			healthy = false
		}
	}
	return healthy
}

// markStorageCleanShutdown flushes acknowledged asynchronous writes before
// recording the durable marker.
func markStorageCleanShutdown(engine storage.Engine, derivedIndexesHealthy bool) error {
	if !derivedIndexesHealthy {
		return nil
	}
	state, ok := engine.(storage.StartupMaintenanceStateEngine)
	if !ok {
		return nil
	}
	if flusher, ok := engine.(interface{ Flush() error }); ok {
		if err := flusher.Flush(); err != nil {
			return fmt.Errorf("flush storage before clean shutdown: %w", err)
		}
	}
	if err := state.MarkCleanShutdown(context.Background()); err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		return fmt.Errorf("mark clean storage shutdown: %w", err)
	}
	return nil
}
