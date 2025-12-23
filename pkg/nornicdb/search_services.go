package nornicdb

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type dbSearchService struct {
	dbName string
	engine storage.Engine
	svc    *search.Service

	buildOnce sync.Once
	buildErr  error

	clusterMu              sync.Mutex
	lastClusteredEmbedCount int
}

func splitQualifiedID(id string) (dbName string, local string, ok bool) {
	dbName, local, ok = strings.Cut(id, ":")
	if !ok || dbName == "" || local == "" {
		return "", "", false
	}
	return dbName, local, true
}

func (db *DB) defaultDatabaseName() string {
	if namespaced, ok := db.storage.(*storage.NamespacedEngine); ok {
		return namespaced.Namespace()
	}
	// DB storage must always be namespaced; anything else is a programmer error.
	panic("nornicdb: DB storage is not namespaced")
}

func (db *DB) getOrCreateSearchService(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}

	db.searchServicesMu.RLock()
	if entry, ok := db.searchServices[dbName]; ok {
		db.searchServicesMu.RUnlock()
		return entry.svc, nil
	}
	db.searchServicesMu.RUnlock()

	if storageEngine == nil {
		if db.baseStorage == nil {
			return nil, fmt.Errorf("search service unavailable: base storage is nil")
		}
		storageEngine = storage.NewNamespacedEngine(db.baseStorage, dbName)
	}

	dims := db.embeddingDims
	if dims <= 0 {
		dims = 1024
	}
	svc := search.NewServiceWithDimensions(storageEngine, dims)
	svc.SetDefaultMinSimilarity(db.searchMinSimilarity)

	// Enable per-database clustering if the feature flag is enabled.
	// Each Service maintains its own cluster index and must cluster independently.
	if featureflags.IsGPUClusteringEnabled() {
		// GPU acceleration (if any) is applied via db.SetGPUManager().
		svc.EnableClustering(nil, 100)
	}

	entry := &dbSearchService{
		dbName: dbName,
		engine: storageEngine,
		svc:    svc,
	}

	db.searchServicesMu.Lock()
	// Double-check in case someone else created it.
	if existing, ok := db.searchServices[dbName]; ok {
		db.searchServicesMu.Unlock()
		return existing.svc, nil
	}
	db.searchServices[dbName] = entry
	db.searchServicesMu.Unlock()

	return svc, nil
}

// GetOrCreateSearchService returns the per-database search service for dbName.
//
// storageEngine should be a *storage.NamespacedEngine for dbName (typically
// obtained via multidb.DatabaseManager). If nil, db.baseStorage is wrapped with
// a NamespacedEngine for dbName.
func (db *DB) GetOrCreateSearchService(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	return db.getOrCreateSearchService(dbName, storageEngine)
}

// ResetSearchService drops the cached search service for a database.
// The next call to GetOrCreateSearchService will create a fresh, empty service.
func (db *DB) ResetSearchService(dbName string) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	db.searchServicesMu.Lock()
	delete(db.searchServices, dbName)
	db.searchServicesMu.Unlock()
}

func (db *DB) ensureSearchIndexesBuilt(ctx context.Context, dbName string) error {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}

	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if !ok || entry == nil {
		return fmt.Errorf("search service not initialized for database %q", dbName)
	}

	entry.buildOnce.Do(func() {
		entry.buildErr = entry.svc.BuildIndexes(ctx)
	})
	return entry.buildErr
}

// EnsureSearchIndexesBuilt ensures the per-database search indexes are built exactly once.
// If the service doesn’t exist yet, it is created (using storageEngine if provided).
func (db *DB) EnsureSearchIndexesBuilt(ctx context.Context, dbName string, storageEngine storage.Engine) (*search.Service, error) {
	svc, err := db.getOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		return nil, err
	}
	if err := db.ensureSearchIndexesBuilt(ctx, dbName); err != nil {
		return svc, err
	}
	return svc, nil
}

func (db *DB) indexNodeFromEvent(node *storage.Node) {
	if node == nil {
		return
	}

	dbName, local, ok := splitQualifiedID(string(node.ID))
	if !ok {
		// Unprefixed IDs are not supported. This indicates a bug in the storage event pipeline.
		log.Printf("⚠️ storage event had unprefixed node ID: %q", node.ID)
		return
	}

	svc, err := db.getOrCreateSearchService(dbName, nil)
	if err != nil || svc == nil {
		return
	}

	userNode := storage.CopyNode(node)
	userNode.ID = storage.NodeID(local)
	if err := svc.IndexNode(userNode); err != nil {
		log.Printf("⚠️ Failed to index node %s in db %s: %v", node.ID, dbName, err)
	}
}

func (db *DB) removeNodeFromEvent(nodeID storage.NodeID) {
	dbName, local, ok := splitQualifiedID(string(nodeID))
	if !ok {
		return
	}

	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if !ok || entry == nil {
		// Service not in cache yet; nothing to remove.
		return
	}

	if err := entry.svc.RemoveNode(storage.NodeID(local)); err != nil {
		log.Printf("⚠️ Failed to remove node %s from search indexes in db %s: %v", nodeID, dbName, err)
	}
}

func (db *DB) runClusteringOnceAllDatabases() {
	db.searchServicesMu.RLock()
	entries := make([]*dbSearchService, 0, len(db.searchServices))
	for _, entry := range db.searchServices {
		entries = append(entries, entry)
	}
	db.searchServicesMu.RUnlock()

	for _, entry := range entries {
		if entry == nil || entry.svc == nil || !entry.svc.IsClusteringEnabled() {
			continue
		}

		currentCount := entry.svc.EmbeddingCount()

		entry.clusterMu.Lock()
		if currentCount == entry.lastClusteredEmbedCount && entry.lastClusteredEmbedCount > 0 {
			entry.clusterMu.Unlock()
			continue
		}
		entry.clusterMu.Unlock()

		if err := entry.svc.TriggerClustering(); err != nil {
			log.Printf("⚠️  K-means clustering skipped for db %s: %v", entry.dbName, err)
			continue
		}

		entry.clusterMu.Lock()
		entry.lastClusteredEmbedCount = currentCount
		entry.clusterMu.Unlock()
		log.Printf("🔬 K-means clustering completed for db %s (%d embeddings)", entry.dbName, currentCount)
	}
}
