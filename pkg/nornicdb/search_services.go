package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type dbSearchService struct {
	dbName string
	engine storage.Engine
	svc    *search.Service

	buildOnce  sync.Once
	buildDone  chan struct{}
	buildErr   error
	buildErrMu sync.RWMutex

	clusterMu               sync.Mutex
	lastClusteredEmbedCount int

	pendingMu           sync.Mutex
	pendingOps          map[string]pendingSearchMutation
	pendingFlushMu      sync.Mutex
	pendingFlushTimer   *time.Timer
	pendingFlushRunning bool
	pendingFlushDelay   time.Duration
}

func (e *dbSearchService) closeBuildDone() {
	if e == nil || e.buildDone == nil {
		return
	}
	defer func() {
		if recover() != nil {
			// Tests and shutdown races may observe a pre-closed buildDone channel.
			// Treat repeated close attempts as idempotent completion.
		}
	}()
	close(e.buildDone)
}

const searchMutationDebounceDelay = 250 * time.Millisecond

type pendingSearchMutation struct {
	node   *storage.Node
	remove bool
}

func (db *DB) shouldIgnoreSearchIndexingError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, storage.ErrStorageClosed) || errors.Is(err, ErrClosed) {
		return true
	}
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	return closed
}

func (e *dbSearchService) queueIndex(node *storage.Node) {
	if e == nil || node == nil {
		return
	}
	e.pendingMu.Lock()
	if e.pendingOps == nil {
		e.pendingOps = make(map[string]pendingSearchMutation)
	}
	e.pendingOps[string(node.ID)] = pendingSearchMutation{node: storage.CopyNode(node), remove: false}
	e.pendingMu.Unlock()
}

func (e *dbSearchService) queueRemove(localID string) {
	if e == nil || localID == "" {
		return
	}
	e.pendingMu.Lock()
	if e.pendingOps == nil {
		e.pendingOps = make(map[string]pendingSearchMutation)
	}
	e.pendingOps[localID] = pendingSearchMutation{remove: true}
	e.pendingMu.Unlock()
}

func (e *dbSearchService) drainPending() map[string]pendingSearchMutation {
	if e == nil {
		return nil
	}
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	if len(e.pendingOps) == 0 {
		return nil
	}
	out := e.pendingOps
	e.pendingOps = make(map[string]pendingSearchMutation)
	return out
}

func (e *dbSearchService) hasPending() bool {
	if e == nil {
		return false
	}
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	return len(e.pendingOps) > 0
}

func (e *dbSearchService) flushDelay() time.Duration {
	if e == nil || e.pendingFlushDelay <= 0 {
		return searchMutationDebounceDelay
	}
	return e.pendingFlushDelay
}

// DatabaseSearchStatus exposes per-database search service readiness/progress.
type DatabaseSearchStatus struct {
	Ready           bool    `json:"ready"`
	Building        bool    `json:"building"`
	Initialized     bool    `json:"initialized"`
	Strategy        string  `json:"strategy,omitempty"`
	Phase           string  `json:"phase,omitempty"`
	ProcessedNodes  int64   `json:"processed_nodes,omitempty"`
	TotalNodes      int64   `json:"total_nodes,omitempty"`
	RateNodesPerSec float64 `json:"rate_nodes_per_sec,omitempty"`
	ETASeconds      int64   `json:"eta_seconds,omitempty"`
	// BM25Enabled / VectorEnabled mirror the resolved per-DB master switches.
	// Search handler uses these to decide between 200-with-empty-half,
	// 503 search_disabled_for_database (both off), and the lazy-trigger 503.
	BM25Enabled   bool `json:"bm25_enabled"`
	VectorEnabled bool `json:"vector_enabled"`
	// LazyTriggerNeeded is true when at least one resolved-as-enabled index
	// is configured warming=lazy and has not yet started building.
	LazyTriggerNeeded bool `json:"lazy_trigger_needed,omitempty"`
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

// kmeansNumClusters returns the configured number of k-means clusters (0 = auto from dataset size).
// Used when enabling clustering so EnableClustering receives the configured value.
func (db *DB) kmeansNumClusters() int {
	if db.config != nil {
		return db.config.Memory.KmeansNumClusters
	}
	return 0
}

func (db *DB) getOrCreateSearchService(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	if dbName == "system" {
		return nil, fmt.Errorf("search service not available for system database")
	}

	dims := db.embeddingDims
	minSim := db.searchMinSimilarity
	bm25Engine := search.DefaultBM25Engine()
	db.dbConfigResolverMu.RLock()
	resolver := db.dbConfigResolver
	db.dbConfigResolverMu.RUnlock()
	if resolver != nil {
		rd, rs, re := resolver(dbName)
		if rd > 0 {
			dims = rd
		}
		minSim = rs
		if re != "" {
			bm25Engine = re
		}
	}

	var gpuMgr *gpu.Manager
	db.gpuManagerMu.RLock()
	if m, ok := db.gpuManager.(*gpu.Manager); ok {
		gpuMgr = m
	}
	db.gpuManagerMu.RUnlock()

	db.searchServicesMu.RLock()
	if entry, ok := db.searchServices[dbName]; ok {
		svc := entry.svc
		reranker := db.searchReranker
		rr := db.rerankerResolver
		db.searchServicesMu.RUnlock()
		if rr != nil {
			reranker = rr(dbName)
		}

		// If clustering is enabled globally, ensure cached services have clustering enabled too.
		// Services may be created before the feature flag is turned on (e.g., early HTTP calls),
		// in which case they need to be upgraded in place. Use configured cluster count.
		if svc != nil && featureflags.IsGPUClusteringEnabled() && !svc.IsClusteringEnabled() {
			var mgr *gpu.Manager
			if gpuMgr != nil && gpuMgr.IsEnabled() {
				mgr = gpuMgr
			}
			svc.EnableClustering(mgr, db.kmeansNumClusters())
		}
		// If a Stage-2 reranker is configured (e.g. Heimdall LLM), ensure it is applied.
		// This keeps behavior consistent even if the reranker is configured after the
		// service was created (e.g. Heimdall initializes later in server startup).
		if svc != nil {
			svc.SetReranker(reranker)
		}
		return svc, nil
	}
	db.searchServicesMu.RUnlock()

	// Serialize creation so only one goroutine is in the create+insert path at a time,
	// avoiding RWMutex deadlock when background build, flush indexNodeFromEvent, and CreateNode contend.
	db.searchServiceCreationMu.Lock()
	defer db.searchServiceCreationMu.Unlock()

	// Re-check after acquiring creation lock; another goroutine may have created it.
	db.searchServicesMu.RLock()
	if entry, ok := db.searchServices[dbName]; ok {
		svc := entry.svc
		db.searchServicesMu.RUnlock()
		return svc, nil
	}
	db.searchServicesMu.RUnlock()

	if storageEngine == nil {
		if db.baseStorage == nil {
			return nil, fmt.Errorf("search service unavailable: base storage is nil")
		}
		storageEngine = storage.NewNamespacedEngine(db.baseStorage, dbName)
	}

	if dims <= 0 {
		dims = 1024
	}
	svc := search.NewServiceWithDimensionsAndBM25Engine(storageEngine, dims, bm25Engine)
	svc.SetDefaultMinSimilarity(minSim)
	// Per-DB master switches: pull from the resolver and seed the service.
	// Defaults (true, true) when no resolver is wired reproduce today's behaviour.
	bm25On, vectorOn, _, _ := db.resolveSearchFlags(dbName)
	svc.SetIndexFlags(bm25On, vectorOn)
	// When both indexes are disabled, mark ready immediately so any
	// concurrent indexNodeFromEvent / pendingFlush goroutine that races
	// the boot orchestrator does NOT call startSearchIndexBuild — its
	// GetBuildProgress.Ready check will see true and skip the build.
	// Without this, a write event firing between getOrCreateSearchService
	// and EnsureSearchIndexesBuilt's MarkReadyDisabled call would trigger
	// a real BuildIndexes run before the gate could land.
	if !bm25On && !vectorOn {
		svc.MarkReadyDisabled()
	}
	persistSearchIndexesEnabled := db.config != nil && db.config.Database.DataDir != "" && db.config.Database.PersistSearchIndexes
	svc.SetPersistenceEnabled(persistSearchIndexesEnabled)

	// EXPERIMENTAL: when PersistSearchIndexes is true, set paths so BuildIndexes saves indexes after a
	// build and loads them on startup (skipping the full iteration when both are present).
	// HNSW is also persisted so the approximate nearest-neighbor index does not need rebuilding.
	// If a rebuild is required (e.g., missing/incompatible artifacts), IVF-HNSW rebuild at startup
	// can be long on large datasets (~30 minutes for ~1M embeddings on observed hardware).
	if persistSearchIndexesEnabled {
		base := filepath.Join(db.config.Database.DataDir, "search", dbName)
		fulltextFilename := "bm25"
		if strings.EqualFold(strings.TrimSpace(bm25Engine), search.BM25EngineV2) {
			fulltextFilename = "bm25.v2"
		}
		svc.SetFulltextIndexPath(filepath.Join(base, fulltextFilename))
		svc.SetVectorIndexPath(filepath.Join(base, "vectors"))
		svc.SetHNSWIndexPath(filepath.Join(base, "hnsw"))
	}

	// Enable GPU brute-force search if a GPU manager is configured.
	if gpuMgr != nil {
		svc.SetGPUManager(gpuMgr)
	}

	// Enable per-database clustering if the feature flag is enabled.
	// Each Service maintains its own cluster index and must cluster independently.
	// Cluster count comes from db.config.Memory.KmeansNumClusters (0 = auto from dataset size).
	if featureflags.IsGPUClusteringEnabled() {
		var mgr *gpu.Manager
		if gpuMgr != nil && gpuMgr.IsEnabled() {
			mgr = gpuMgr
		}
		numClusters := db.kmeansNumClusters()
		svc.EnableClustering(mgr, numClusters)
	}

	// Apply configured Stage-2 reranker (if any). Use per-DB resolver when set.
	db.searchServicesMu.RLock()
	reranker := db.searchReranker
	rr := db.rerankerResolver
	db.searchServicesMu.RUnlock()
	if rr != nil {
		reranker = rr(dbName)
	}
	svc.SetReranker(reranker)

	entry := &dbSearchService{
		dbName:    dbName,
		engine:    storageEngine,
		svc:       svc,
		buildDone: make(chan struct{}),
	}

	db.searchServicesMu.Lock()
	// Double-check in case another goroutine created it while we were building.
	if existing, ok := db.searchServices[dbName]; ok {
		db.searchServicesMu.Unlock()
		return existing.svc, nil
	}
	db.searchServices[dbName] = entry
	db.searchServicesMu.Unlock()

	return svc, nil
}

// SetSearchReranker configures the Stage-2 reranker for all per-database search services.
//
// This is typically set by the server when Heimdall is enabled and the
// vector rerank feature flag is turned on.
func (db *DB) SetSearchReranker(r search.Reranker) {
	db.searchServicesMu.Lock()
	db.searchReranker = r
	entries := make([]*dbSearchService, 0, len(db.searchServices))
	for _, entry := range db.searchServices {
		entries = append(entries, entry)
	}
	db.searchServicesMu.Unlock()

	for _, entry := range entries {
		if entry == nil || entry.svc == nil {
			continue
		}
		entry.svc.SetReranker(r)
	}
}

// SetRerankerResolver sets an optional function that returns the reranker for a given database.
// When set, getOrCreateSearchService uses it instead of the single global searchReranker (enables per-DB rerankers).
func (db *DB) SetRerankerResolver(fn func(dbName string) search.Reranker) {
	db.searchServicesMu.Lock()
	db.rerankerResolver = fn
	db.searchServicesMu.Unlock()
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
	if entry, ok := db.searchServices[dbName]; ok && entry != nil && entry.svc != nil {
		// Stop pending persist timers and close file-backed vector store
		// so removed databases cannot later re-persist stale index state.
		entry.svc.SetPersistenceEnabled(false)
	}
	delete(db.searchServices, dbName)
	db.searchServicesMu.Unlock()
}

// DropSearchServiceState removes cached search service state and best-effort purges
// on-disk persisted index artifacts for the database.
func (db *DB) DropSearchServiceState(dbName string) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	db.ResetSearchService(dbName)
	if db.config == nil || db.config.Database.DataDir == "" {
		return
	}
	_ = os.RemoveAll(filepath.Join(db.config.Database.DataDir, "search", dbName))
}

// GetDatabaseSearchStatus returns readiness and progress for the database search service.
// It does not create a new service; it reports status for existing cached services only.
func (db *DB) GetDatabaseSearchStatus(dbName string) DatabaseSearchStatus {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	bm25On, vectorOn, bm25Warming, vectorWarming := db.resolveSearchFlags(dbName)
	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if !ok || entry == nil || entry.svc == nil {
		// Service not yet created. If both indexes are enabled+startup, the
		// boot loop will have already created the entry, so a missing entry
		// here typically means deferred (lazy) or disabled. Compute the
		// LazyTriggerNeeded signal so the search handler can fire the trigger.
		lazyNeeded := false
		bothOff := !bm25On && !vectorOn
		if !bothOff {
			lazyNeeded = (bm25On && bm25Warming == "lazy") || (vectorOn && vectorWarming == "lazy")
		}
		return DatabaseSearchStatus{
			Ready:             false,
			Building:          false,
			Initialized:       false,
			Strategy:          "unknown",
			Phase:             "not_initialized",
			ETASeconds:        -1,
			BM25Enabled:       bm25On,
			VectorEnabled:     vectorOn,
			LazyTriggerNeeded: lazyNeeded,
		}
	}
	p := entry.svc.GetBuildProgress()
	// The lazy-trigger signal flips on once the service is created but the
	// build hasn't started. After ForceSearchIndexBuild fires, building=true
	// and lazyTrigger flips off until restart.
	lazyNeeded := !p.Ready && !p.Building &&
		((bm25On && bm25Warming == "lazy") || (vectorOn && vectorWarming == "lazy")) &&
		!(bm25On == false && vectorOn == false)
	return DatabaseSearchStatus{
		Ready:             p.Ready,
		Building:          p.Building,
		Initialized:       true,
		Strategy:          entry.svc.CurrentStrategy(),
		Phase:             p.Phase,
		ProcessedNodes:    p.ProcessedNodes,
		TotalNodes:        p.TotalNodes,
		RateNodesPerSec:   p.RateNodesPerSec,
		ETASeconds:        p.ETASeconds,
		BM25Enabled:       bm25On,
		VectorEnabled:     vectorOn,
		LazyTriggerNeeded: lazyNeeded,
	}
}

func (db *DB) startSearchIndexBuild(entry *dbSearchService, ctx context.Context) {
	if entry == nil || entry.svc == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	entry.buildOnce.Do(func() {
		if !db.startBackgroundTask(func() {
			err := entry.svc.BuildIndexes(ctx)
			entry.buildErrMu.Lock()
			entry.buildErr = err
			if err == nil {
				// Mark clustering as current so runClusteringOnceAllDatabases skips this db
				// (we either restored IVF-HNSW from disk or ran k-means in warmup).
				entry.clusterMu.Lock()
				entry.lastClusteredEmbedCount = entry.svc.EmbeddingCount()
				entry.clusterMu.Unlock()
			}
			entry.buildErrMu.Unlock()
			entry.closeBuildDone()
		}) {
			entry.buildErrMu.Lock()
			entry.buildErr = ErrClosed
			entry.buildErrMu.Unlock()
			entry.closeBuildDone()
		}
	})
}

func (db *DB) ensurePendingFlush(entry *dbSearchService) {
	if entry == nil || entry.svc == nil {
		return
	}
	entry.pendingFlushMu.Lock()
	if entry.pendingFlushTimer != nil {
		entry.pendingFlushTimer.Stop()
	}
	delay := entry.flushDelay()
	entry.pendingFlushTimer = time.AfterFunc(delay, func() {
		entry.pendingFlushMu.Lock()
		entry.pendingFlushTimer = nil
		if entry.pendingFlushRunning {
			entry.pendingFlushMu.Unlock()
			return
		}
		entry.pendingFlushRunning = true
		entry.pendingFlushMu.Unlock()

		started := db.startBackgroundTask(func() {
			defer func() {
				entry.pendingFlushMu.Lock()
				entry.pendingFlushRunning = false
				entry.pendingFlushMu.Unlock()
				if entry.hasPending() {
					db.ensurePendingFlush(entry)
				}
			}()

			progress := entry.svc.GetBuildProgress()
			if progress.Building || !progress.Ready {
				ctx := db.buildCtx
				if ctx == nil {
					ctx = context.Background()
				}
				db.startSearchIndexBuild(entry, ctx)
				<-entry.buildDone
			}

			for {
				ops := entry.drainPending()
				if len(ops) == 0 {
					return
				}
				ids := make([]string, 0, len(ops))
				for id := range ops {
					ids = append(ids, id)
				}
				sort.Strings(ids)
				for _, id := range ids {
					op := ops[id]
					if op.remove {
						if err := entry.svc.RemoveNode(storage.NodeID(id)); err != nil {
							if db.shouldIgnoreSearchIndexingError(err) {
								continue
							}
							log.Printf("⚠️ Failed to remove node %s from deferred search mutation in db %s: %v", id, entry.dbName, err)
						}
						continue
					}
					if op.node == nil {
						continue
					}
					if err := entry.svc.IndexNode(op.node); err != nil {
						if db.shouldIgnoreSearchIndexingError(err) {
							continue
						}
						log.Printf("⚠️ Failed to index node %s from deferred search mutation in db %s: %v", id, entry.dbName, err)
					}
				}
			}
		})
		if !started {
			entry.pendingFlushMu.Lock()
			entry.pendingFlushRunning = false
			entry.pendingFlushMu.Unlock()
		}
	})
	entry.pendingFlushMu.Unlock()
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
	db.startSearchIndexBuild(entry, ctx)
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-entry.buildDone:
		entry.buildErrMu.RLock()
		err := entry.buildErr
		entry.buildErrMu.RUnlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// EnsureSearchIndexesBuilt ensures the per-database search indexes are built exactly once.
// If the service doesn’t exist yet, it is created (using storageEngine if provided).
// Honours the per-DB master switches: when both are off, the service is marked
// ready (no-op stub for BM25, vector flag guard) and no build runs. When both
// are warming=lazy, the build is deferred to the first inbound search query.
func (db *DB) EnsureSearchIndexesBuilt(ctx context.Context, dbName string, storageEngine storage.Engine) (*search.Service, error) {
	svc, err := db.getOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		return nil, err
	}
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	bm25On, vectorOn, bm25Warming, vectorWarming := db.resolveSearchFlags(dbName)
	if !bm25On && !vectorOn {
		// Both indexes off — mark ready (handler returns 503
		// search_disabled_for_database upstream) and skip the build.
		svc.MarkReadyDisabled()
		return svc, nil
	}
	if (!bm25On || bm25Warming == "lazy") && (!vectorOn || vectorWarming == "lazy") {
		// All enabled indexes are configured warming=lazy — defer the
		// build to the first inbound search query (handler will call
		// ForceSearchIndexBuild).
		return svc, nil
	}
	if err := db.ensureSearchIndexesBuilt(ctx, dbName); err != nil {
		return svc, err
	}
	return svc, nil
}

func (db *DB) removeNodeFromSearchIndexes(ctx context.Context, dbName string, storageEngine storage.Engine, id storage.NodeID) error {
	if id == "" {
		return nil
	}
	svc, err := db.EnsureSearchIndexesBuilt(ctx, dbName, storageEngine)
	if err != nil {
		return err
	}
	if svc == nil {
		return nil
	}
	return svc.RemoveNode(id)
}

// EnsureSearchIndexesBuildStarted starts per-database search indexing if not already started
// and returns immediately without waiting for completion. Honours the per-DB
// warming triggers: when both BM25 and vector are configured warming=lazy, the
// build is deferred until ForceSearchIndexBuild is called (e.g., from the
// search handler on first inbound query).
func (db *DB) EnsureSearchIndexesBuildStarted(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	svc, err := db.getOrCreateSearchService(dbName, storageEngine)
	if err != nil {
		return nil, err
	}
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry == nil {
		return svc, nil
	}
	// Lazy-warming check: if both indexes are configured warming=lazy, defer
	// the boot-time build. The search handler triggers the build on the first
	// inbound query via ForceSearchIndexBuild. If at least one index is
	// startup-warmed, start the build now (the disabled / lazy index simply
	// doesn't materialize during BuildIndexes).
	bm25On, vectorOn, bm25Warming, vectorWarming := db.resolveSearchFlags(dbName)
	bothOff := !bm25On && !vectorOn
	bothLazy := (!bm25On || bm25Warming == "lazy") && (!vectorOn || vectorWarming == "lazy")
	if bothOff {
		// Mark service "ready" (handler short-circuits to 503 anyway) so
		// status probes don't report perpetual building.
		svc.MarkReadyDisabled()
		return svc, nil
	}
	if bothLazy {
		// Defer the build to first-query trigger.
		return svc, nil
	}
	ctx := db.buildCtx
	if ctx == nil {
		ctx = context.Background()
	}
	db.startSearchIndexBuild(entry, ctx)
	return svc, nil
}

// ForceSearchIndexBuild triggers a build for the given database irrespective
// of the warming setting. Used by the search handler on the first inbound
// query against a database whose indexes are configured warming=lazy.
// Idempotent: if a build is already running or finished, this is a no-op.
func (db *DB) ForceSearchIndexBuild(dbName string, storageEngine storage.Engine) error {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	if _, err := db.getOrCreateSearchService(dbName, storageEngine); err != nil {
		return err
	}
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry == nil {
		return nil
	}
	ctx := db.buildCtx
	if ctx == nil {
		ctx = context.Background()
	}
	db.startSearchIndexBuild(entry, ctx)
	return nil
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
	// Qdrant gRPC points are stored under a reserved sub-namespace and are indexed
	// by the Qdrant vector index cache, not the hybrid search service.
	if strings.HasPrefix(local, "qdrant:") {
		return
	}

	svc, err := db.getOrCreateSearchService(dbName, nil)
	if err != nil || svc == nil {
		return
	}

	userNode := storage.CopyNode(node)
	userNode.ID = storage.NodeID(local)

	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry == nil {
		return
	}

	entry.queueIndex(userNode)
	progress := svc.GetBuildProgress()
	if progress.Building || !progress.Ready {
		ctx := db.buildCtx
		if ctx == nil {
			ctx = context.Background()
		}
		db.startSearchIndexBuild(entry, ctx)
	}
	db.ensurePendingFlush(entry)
}

func (db *DB) removeNodeFromEvent(nodeID storage.NodeID) {
	dbName, local, ok := splitQualifiedID(string(nodeID))
	if !ok {
		// Unprefixed ID (e.g. single-db or callback from engine that doesn't prefix).
		// Use default database and the ID as-is so embeddings are still removed.
		dbName = db.defaultDatabaseName()
		local = string(nodeID)
	}

	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if !ok || entry == nil {
		// Service not in cache yet; nothing to remove.
		return
	}

	entry.queueRemove(local)
	progress := entry.svc.GetBuildProgress()
	if progress.Building || !progress.Ready {
		ctx := db.buildCtx
		if ctx == nil {
			ctx = context.Background()
		}
		db.startSearchIndexBuild(entry, ctx)
	}
	db.ensurePendingFlush(entry)
}

// GetDatabaseManagedEmbeddingStats returns managed embedding usage for a database.
// managedBytes is computed as embedding_count * vector_dimensions * 4 (float32 bytes).
func (db *DB) GetDatabaseManagedEmbeddingStats(dbName string) (embeddingCount int, vectorDimensions int, managedBytes int64) {
	if strings.TrimSpace(dbName) == "" {
		dbName = db.defaultDatabaseName()
	}
	if dbName == "system" {
		return 0, 0, 0
	}

	svc, err := db.getOrCreateSearchService(dbName, nil)
	if err != nil || svc == nil {
		return 0, 0, 0
	}

	embeddingCount = svc.EmbeddingCount()
	vectorDimensions = svc.VectorIndexDimensions()
	if embeddingCount <= 0 || vectorDimensions <= 0 {
		return embeddingCount, vectorDimensions, 0
	}
	managedBytes = int64(embeddingCount) * int64(vectorDimensions) * 4
	return embeddingCount, vectorDimensions, managedBytes
}

// runClusteringOnceAllDatabases runs k-means for each database. Stops when ctx is cancelled (e.g. shutdown).
func (db *DB) runClusteringOnceAllDatabases(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Ensure the default database service exists so an immediate clustering run
	// produces deterministic behavior even before the first search request.
	if _, err := db.getOrCreateSearchService(db.defaultDatabaseName(), db.storage); err != nil {
		log.Printf("⚠️  K-means clustering: failed to initialize default search service: %v", err)
	}

	// If storage can enumerate namespaces, initialize per-database services so
	// clustering can run across all known databases (excluding system).
	if lister, ok := db.baseStorage.(storage.NamespaceLister); ok {
		for _, ns := range lister.ListNamespaces() {
			if ns == "" || ns == "system" {
				continue
			}
			if _, err := db.getOrCreateSearchService(ns, nil); err != nil {
				log.Printf("⚠️  K-means clustering: failed to initialize search service for db %s: %v", ns, err)
			}
		}
	}

	db.searchServicesMu.RLock()
	entries := make([]*dbSearchService, 0, len(db.searchServices))
	for _, entry := range db.searchServices {
		entries = append(entries, entry)
	}
	db.searchServicesMu.RUnlock()

	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry == nil || entry.dbName == "system" {
			continue
		}
		if entry.svc == nil || !entry.svc.IsClusteringEnabled() {
			continue
		}
		progress := entry.svc.GetBuildProgress()
		// Do not run timer/manual clustering while initial search build is still in progress.
		// BuildIndexes warmup already runs BM25-seeded k-means + IVF-HNSW when enabled.
		if progress.Building {
			log.Printf("🔬 K-means clustering deferred for db %s: search build in progress (phase=%s)", entry.dbName, progress.Phase)
			continue
		}
		// Also skip until initial build reaches ready; otherwise we'd cluster partial indexes.
		if !progress.Ready {
			log.Printf("🔬 K-means clustering deferred for db %s: search not ready yet (phase=%s)", entry.dbName, progress.Phase)
			continue
		}
		if entry.svc.ClusteringInProgress() {
			continue
		}

		// Serialize clustering per database to avoid duplicate work when multiple
		// triggers fire concurrently (startup hooks, manual triggers, timer ticks).
		// Hold clusterMu only for the check and the count update; do not hold it across
		// TriggerClustering (k-means + rebuildClusterHNSW can take a long time and would
		// block other code that needs clusterMu or that triggers IndexNode while rebuild
		// holds vector index read lock).
		entry.clusterMu.Lock()
		currentCount := entry.svc.EmbeddingCount()
		shouldRun := currentCount != entry.lastClusteredEmbedCount || entry.lastClusteredEmbedCount == 0
		entry.clusterMu.Unlock()
		if !shouldRun {
			continue
		}

		if err := entry.svc.TriggerClustering(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("⚠️  K-means clustering skipped for db %s: %v", entry.dbName, err)
			continue
		}

		entry.clusterMu.Lock()
		entry.lastClusteredEmbedCount = entry.svc.EmbeddingCount()
		doneCount := entry.lastClusteredEmbedCount
		entry.clusterMu.Unlock()
		log.Printf("🔬 K-means clustering completed for db %s (%d embeddings)", entry.dbName, doneCount)
	}
}
