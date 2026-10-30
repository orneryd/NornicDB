package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Close closes the database. It is the DB owner's one stop: the first caller
// signals every producer while holding a ticket, so retirement cannot overtake
// the signal; every caller, first or repeated, joins the same retirement and
// receives the same outcome.
func (db *DB) Close() error {
	if db.work.Enter() {
		if db.work.CloseAdmission() {
			db.signalStop()
		}
		db.work.Leave()
	}
	return db.work.Shutdown(context.Background(), db.retire)
}

// LifetimeContext returns a context that is cancelled when the database
// closes, for background work that must stop with it: buildCtx once Open set
// it, otherwise the DB owner's own context (buildCtx's parent). A nil DB has
// no lifetime; it is the caller's boundary default.
func (db *DB) LifetimeContext() context.Context {
	if db == nil {
		return context.Background()
	}
	if db.buildCtx == nil {
		return db.work.Context()
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

// signalStop stops the producers so accepted work can return. Admission is
// sealed: SetEmbedder refuses, so embedQueue is final here.
func (db *DB) signalStop() {
	db.mu.Lock()
	embedQueue := db.embedQueue
	db.mu.Unlock()

	if db.buildCancel != nil {
		db.buildCancel()
	}
	// A retained DB consumer can be waiting on the native owner's admission
	// or leader state. Start that owner's stop before joining those consumers;
	// its existing Shutdown still owns native signal, join and physical close.
	if replicator := db.replicator; replicator != nil {
		db.replicationStop.Go(replicator.Shutdown)
	}
	db.searchServicesMu.Lock()
	servicesToStop := make([]*search.Service, 0, len(db.searchServices))
	for _, entry := range db.searchServices {
		if entry != nil && entry.svc != nil {
			servicesToStop = append(servicesToStop, entry.svc)
		}
	}
	db.searchServicesMu.Unlock()
	for _, svc := range servicesToStop {
		svc.BeginShutdown()
	}
	db.stopClusteringTimer()
	if embedQueue != nil {
		embedQueue.Close()
	}
}

// retire is the owner's dispose: it runs once, after queue workers,
// initializers, constructors and foreground provider users actually returned.
func (db *DB) retire() error {
	if db.cypherExecutor != nil {
		db.cypherExecutor.StatementCaches().Dispose()
	}

	// Queue workers, initializers and foreground calls are now quiescent. Release all provider
	// resources through their one owner, including cached/replaced providers.
	errs := db.closeOwnedProviders()
	// Stop every component that can still mutate graph storage before recording
	// the durable storage boundary. Search snapshots are derived artifacts and
	// may take much longer to save; they must not delay this marker.
	if db.accessFlusher != nil {
		if err := db.accessFlusher.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	if db.replicator != nil {
		<-db.replicationStop.Done()
		if err := db.replicationStop.Err(); err != nil {
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
			errs = append(errs, fmt.Errorf("save retention policies: %w", err))
		}
	}
	var markerEngine storage.Engine
	if len(errs) == 0 && db.opened {
		markerEngine = db.baseStorage
	}
	db.searchServicesMu.RLock()
	services := make([]*search.Service, 0, len(db.searchServices))
	for _, entry := range db.searchServices {
		if entry != nil && entry.svc != nil {
			services = append(services, entry.svc)
		}
	}
	db.searchServicesMu.RUnlock()
	// The marker is part of the DB stop after cancellation: it runs under the
	// owner's values without its cancellation, so the stop does not abort it.
	markErr := persistDerivedAfterStorageBoundary(context.WithoutCancel(db.work.Context()), markerEngine, db.derivedIndexesHealthy, func() {
		if db.config == nil || !db.config.Database.PersistSearchIndexes || db.config.Database.DataDir == "" {
			return
		}
		for _, svc := range services {
			svc.PersistIndexesToDisk()
		}
	})
	if markErr != nil {
		errs = append(errs, markErr)
	}
	for _, svc := range services {
		if err := svc.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	db.searchContinuationMu.Lock()
	continuation := db.searchContinuation
	db.searchContinuation = nil
	db.searchContinuationMu.Unlock()
	if continuation != nil {
		continuation.Close()
	}

	if db.baseStorage != nil {
		if err := db.baseStorage.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return localizedError(localization.NornicDBCoreCloseFailed(fmt.Sprint(errs)), compose.Outcomes(nil, errs...))
	}
	return nil
}

// persistDerivedAfterStorageBoundary records the durable graph-storage state
// before starting potentially slow derived-index persistence. The marker does
// not claim search snapshots are current; each search snapshot validates and
// rebuilds independently when absent or incompatible.
func persistDerivedAfterStorageBoundary(ctx context.Context, engine storage.Engine, derivedIndexesHealthy bool, persist func()) error {
	err := markStorageCleanShutdown(ctx, engine, derivedIndexesHealthy)
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
func markStorageCleanShutdown(ctx context.Context, engine storage.Engine, derivedIndexesHealthy bool) error {
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
	if err := state.MarkCleanShutdown(ctx); err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			return nil
		}
		return fmt.Errorf("mark clean storage shutdown: %w", err)
	}
	return nil
}
