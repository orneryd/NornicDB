package nornicdb

import (
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/inference"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/temporal"
)

type dbInferenceService struct {
	dbName string
	engine *inference.Engine
}

// getOrCreateInferenceService returns (or creates) the per-database inference engine.
// storageEngine should be the namespaced storage for the database; if nil, it is created
// by wrapping baseStorage with the db name.
func (db *DB) getOrCreateInferenceService(dbName string, storageEngine storage.Engine) (*inference.Engine, error) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}

	if db.config == nil || !db.config.Memory.AutoLinksEnabled {
		// Inference is disabled by configuration.
		return nil, nil
	}

	db.inferenceServicesMu.RLock()
	if svc, ok := db.inferenceServices[dbName]; ok {
		db.inferenceServicesMu.RUnlock()
		return svc, nil
	}
	db.inferenceServicesMu.RUnlock()

	// Build storage for this database if not provided.
	if storageEngine == nil {
		var err error
		storageEngine, err = db.resolveDatabaseStorage(dbName)
		if err != nil {
			return nil, err
		}
	}

	// Create inference engine using current config flags.
	inferConfig := &inference.Config{
		SimilarityThreshold: db.config.Memory.AutoLinksSimilarityThreshold,
		SimilarityTopK:      10,
		CoAccessEnabled:     true,
		CoAccessWindow:      30 * time.Second,
		CoAccessMinCount:    3,
		TransitiveEnabled:   true,
		TransitiveMinConf:   0.5,
	}
	engine := inference.New(inferConfig)

	// Auto-TLP topology integration (per database).
	if featureflags.IsAutoTLPEnabled() {
		topoConfig := inference.DefaultTopologyConfig()
		topoConfig.Enabled = true
		topoConfig.Algorithm = "adamic_adar"
		topoConfig.Weight = 0.4
		topoConfig.MinScore = 0.3
		topoConfig.GraphRefreshInterval = 100

		topo := inference.NewTopologyIntegration(storageEngine, topoConfig)
		engine.SetTopologyIntegration(topo)
	}

	// Kalman filtering (per database).
	if featureflags.IsKalmanEnabled() {
		kalmanConfig := inference.DefaultKalmanAdapterConfig()
		kalmanAdapter := inference.NewKalmanAdapter(engine, kalmanConfig)
		trackerConfig := temporal.DefaultConfig()
		tracker := temporal.NewTracker(trackerConfig)
		kalmanAdapter.SetTracker(tracker)
		engine.SetKalmanAdapter(kalmanAdapter)
	}

	db.inferenceServicesMu.Lock()
	// Double-check after acquiring write lock.
	if svc, ok := db.inferenceServices[dbName]; ok {
		db.inferenceServicesMu.Unlock()
		return svc, nil
	}
	db.inferenceServices[dbName] = engine
	db.inferenceServicesMu.Unlock()

	return engine, nil
}

// SetDatabaseStorageResolver routes background writes through the database
// manager's decorated storage chain when one is available.
func (db *DB) SetDatabaseStorageResolver(resolver func(string) (storage.Engine, error)) {
	db.dbConfigResolverMu.Lock()
	db.databaseStorageResolver = resolver
	db.dbConfigResolverMu.Unlock()
}

func (db *DB) resolveDatabaseStorage(dbName string) (storage.Engine, error) {
	db.dbConfigResolverMu.RLock()
	resolver := db.databaseStorageResolver
	db.dbConfigResolverMu.RUnlock()
	if resolver != nil {
		return resolver(dbName)
	}
	if db.baseStorage == nil {
		return nil, localizedError(localization.NornicDBCoreInferenceBaseStorageUnavailable(), nil)
	}
	return storage.NewNamespacedEngine(db.baseStorage, dbName), nil
}

// GetOrCreateInferenceService is the public wrapper that checks closed state.
func (db *DB) GetOrCreateInferenceService(dbName string, storageEngine storage.Engine) (*inference.Engine, error) {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	return db.getOrCreateInferenceService(dbName, storageEngine)
}

// ResetInferenceService removes the cached inference engine for a database (used on DROP).
func (db *DB) ResetInferenceService(dbName string) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	db.inferenceServicesMu.Lock()
	delete(db.inferenceServices, dbName)
	db.inferenceServicesMu.Unlock()
}
