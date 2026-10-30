package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/internal/collection"
	featureflags "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/search/stemmer"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type dbSearchService struct {
	dbName string
	engine storage.Engine
	svc    *search.Service

	buildOnce  lifecycle.OnceCompletion
	buildDone  chan struct{}
	buildErr   error
	buildErrMu sync.RWMutex

	clusterMu               sync.Mutex
	lastClusteredEmbedCount int
	// growthObservedCount is the vector count the last growth check saw
	// (clusteringGrowthDue).
	growthObservedCount int

	// pendingOps and pendingEdgeOps hold the refreshes reserved for queued
	// storage events, by local ID. The drain reads storage, so no payload is kept.
	pendingMu           sync.Mutex
	pendingOps          map[string]search.NodeMutation
	pendingEdgeOps      map[string]search.EdgeMutation
	pendingFlushMu      sync.Mutex
	pendingFlushTimer   *lifecycle.OwnedTimer
	pendingFlushRunning bool
	pendingFlushDelay   time.Duration
	// Delayed retry ownership and entry retirement are protected by pendingMu.
	refreshRetries map[string]*pendingSearchRetry
	retired        bool
	// generation is the database's search-state generation this entry is.
	generation uint64
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

func (e *dbSearchService) drainPending() (map[string]search.NodeMutation, map[string]search.EdgeMutation) {
	if e == nil {
		return nil, nil
	}
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	var nodes map[string]search.NodeMutation
	var edges map[string]search.EdgeMutation
	if len(e.pendingOps) != 0 {
		nodes, e.pendingOps = e.pendingOps, nil
	}
	if len(e.pendingEdgeOps) != 0 {
		edges, e.pendingEdgeOps = e.pendingEdgeOps, nil
	}
	return nodes, edges
}

// reusePending clears a completed batch and reuses its maps when producers
// have not already created the next batch. A newer batch, including a retry
// queued while this one ran, always keeps its own reservations.
func (e *dbSearchService) reusePending(nodes map[string]search.NodeMutation, edges map[string]search.EdgeMutation) {
	clear(nodes)
	clear(edges)
	e.pendingMu.Lock()
	if e.pendingOps == nil {
		e.pendingOps = nodes
	}
	if e.pendingEdgeOps == nil {
		e.pendingEdgeOps = edges
	}
	e.pendingMu.Unlock()
}

func (e *dbSearchService) hasPending() bool {
	if e == nil {
		return false
	}
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	return len(e.pendingOps)+len(e.pendingEdgeOps) > 0
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
	Error           string  `json:"error,omitempty"`
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

func searchPersistenceBasePath(dataDir, dbName string) (string, error) {
	if dbName == "" || dbName == "." || dbName == ".." || strings.ContainsAny(dbName, "/\\\x00") || filepath.IsAbs(dbName) {
		return "", localizedError(localization.NornicDBCoreSearchPersistenceDatabaseInvalid(dbName), nil)
	}
	return filepath.Join(dataDir, "search", dbName), nil
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

// ensureSearchClustering upgrades an existing service when clustering is
// enabled after creation. Both lookup and indexing events share this owner.
func (db *DB) ensureSearchClustering(svc *search.Service) {
	if svc == nil || !featureflags.IsGPUClusteringEnabled() || svc.IsClusteringEnabled() {
		return
	}
	db.gpuManagerMu.RLock()
	mgr, _ := db.gpuManager.(*gpu.Manager)
	db.gpuManagerMu.RUnlock()
	if mgr != nil && !mgr.IsEnabled() {
		mgr = nil
	}
	svc.EnableClustering(mgr, db.kmeansNumClusters())
}

// GetOrCreateSearchService checks the DB lifetime before entering its existing search registry.
func (db *DB) GetOrCreateSearchService(dbName string, storageEngine storage.Engine) (*search.Service, error) {
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	return db.getOrCreateSearchService(dbName, storageEngine)
}

func (db *DB) getOrCreateSearchService(dbName string, storageEngine storage.Engine) (service *search.Service, resultErr error) {
	// This path is also entered by storage callbacks while db.mu is held.
	// Enroll under the existing service registry admission, before any resolver
	// or construction. Close seals this gate before waiting on the same bgWg.
	db.searchServicesMu.Lock()
	if db.searchClosing || db.searchRetiring {
		db.searchServicesMu.Unlock()
		return nil, ErrClosed
	}
	if db.searchConstructing == 0 {
		db.searchConstructionDone = make(chan struct{})
	}
	db.searchConstructing++
	db.bgWg.Add(1)
	db.searchServicesMu.Unlock()
	defer db.bgWg.Done()
	defer db.finishSearchConstruction()
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	if dbName == "system" {
		return nil, localizedError(localization.NornicDBCoreSearchSystemDatabaseUnsupported(), nil)
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

		db.ensureSearchClustering(svc)
		// If a Stage-2 reranker is configured (e.g. Heimdall LLM), ensure it is applied.
		// This keeps behavior consistent even if the reranker is configured after the
		// service was created (e.g. Heimdall initializes later in server startup).
		if svc != nil {
			svc.SetReranker(reranker)
		}
		return svc, nil
	}
	db.searchServicesMu.RUnlock()

	// Construct outside host locks. Publication below selects one cache owner;
	// a losing candidate is disposed before its DB retention is released.
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
			return nil, localizedError(localization.NornicDBCoreSearchBaseStorageUnavailable(), nil)
		}
		storageEngine = storage.NewNamespacedEngine(db.baseStorage, dbName)
	}

	// Construction settings are not consumed by an existing cached service.
	dims := db.embeddingDims
	minSim := db.searchMinSimilarity
	db.dbConfigResolverMu.RLock()
	resolver := db.dbConfigResolver
	optionsResolver := db.dbSearchOptionsResolver
	db.dbConfigResolverMu.RUnlock()
	if resolver != nil {
		rd, rs := resolver(dbName)
		if rd > 0 {
			dims = rd
		}
		minSim = rs
	}

	if dims <= 0 {
		dims = 1024
	}
	// Resolve index flags before the analyzer so a missing optional BM25 plugin
	// cannot prevent vector search service creation.
	bm25On, vectorOn, bm25Warming, vectorWarming := db.resolveSearchFlags(dbName)
	resolvedOptions := search.DefaultServiceOptions(dbName)
	if optionsResolver != nil {
		resolvedOptions = optionsResolver(dbName)
	} else if db.config != nil && stemmer.NormalizeSelection(db.config.Memory.SearchBM25Stemmer) != stemmer.NoneID {
		resolvedOptions.BM25StemmerID = db.config.Memory.SearchBM25Stemmer
	}
	// Database identity is not an optional cache setting. Continuation qids are
	// shared across protocol adapters and must always bind to the canonical
	// database managed by this service, including services created before the
	// server installs its per-database options resolver.
	resolvedOptions.DatabaseID = dbName
	if db.config != nil {
		resolvedOptions.SearchCursorMaxPageSize = db.config.Memory.SearchCursorMaxPageSize
	}
	selectedStemmer := stemmer.NormalizeSelection(resolvedOptions.BM25StemmerID)
	if selectedStemmer != stemmer.NoneID {
		reg, ok := stemmer.Lookup(selectedStemmer)
		if !ok {
			log.Printf("⚠️  Disabling BM25 for database %s: configured stemmer %q is not registered", dbName, selectedStemmer)
			bm25On = false
			resolvedOptions.BM25StemmerID = stemmer.NoneID
		} else {
			resolvedOptions.BM25Stemmer = &reg
		}
	}
	svc := search.NewService(storageEngine, dims, &resolvedOptions)
	adopted := false
	defer func() {
		if adopted {
			return
		}
		if err := svc.Close(); err != nil {
			cleanupErr := fmt.Errorf("search service %s construction cleanup: %w", dbName, err)
			db.recordBackgroundError(cleanupErr)
			resultErr = compose.Outcomes(resultErr, cleanupErr)
		}
	}()
	if db.config != nil && db.config.Memory.EmbeddingEnabled {
		// Embeddings have the configured dimension: the index keeps it.
		svc.FixVectorDimensions()
	}
	if configuredEmbedder, embedErr := db.getOrCreateEmbedderForDB(dbName); embedErr != nil {
		log.Printf("⚠️  Could not resolve embedding space for database %s: %v", dbName, embedErr)
	} else if provider, ok := configuredEmbedder.(embed.EmbeddingSpaceProvider); ok {
		if space := provider.EmbeddingSpace(); space != "" {
			svc.SetEmbeddingSpace(space)
		}
	}
	continuations, err := db.getOrCreateSearchContinuationRegistry()
	if err != nil {
		return nil, err
	}
	svc.SetContinuationRegistry(continuations)
	svc.SetDefaultMinSimilarity(minSim)
	svc.SetEmbedTextOptions(embeddingutil.EmbedTextOptionsFromConfig(db.config))
	if db.config != nil {
		svc.SetFulltextProperties(db.config.Memory.SearchBM25Properties)
		svc.SetRelationshipFulltextProperties(db.config.Memory.SearchRelationshipBM25Properties)
		svc.SetFulltextLabels(db.config.Memory.SearchBM25Labels)
	}
	// Per-DB master switches: pull from the resolver and seed the service.
	// Defaults (true, true) when no resolver is wired reproduce today's behaviour.
	svc.SetIndexFlags(bm25On, vectorOn)
	// When both indexes are disabled, mark ready immediately so any
	// concurrent indexNodeFromEvent / pendingFlush goroutine that races
	// the boot orchestrator does NOT call startSearchIndexBuild — its
	// GetBuildProgress.Ready check will see true and skip the build.
	// Without this, a write event firing between GetOrCreateSearchService
	// and EnsureSearchIndexes's MarkReadyDisabled call would trigger
	// a real BuildIndexes run before the gate could land.
	if !bm25On && !vectorOn {
		svc.MarkReadyDisabled()
	} else if (!bm25On || bm25Warming == "lazy") && (!vectorOn || vectorWarming == "lazy") {
		// All enabled indexes are warming=lazy: install the WarmFunc so any
		// inbound read path (Service.Search, VectorSearchCandidates,
		// db.index.vector.queryNodes, Bolt search, GraphQL search resolver,
		// gRPC) triggers the build synchronously on first read, regardless
		// of which entry point the caller used. The trigger itself runs in
		// db.buildCtx (long-lived) so a caller whose request ctx times out
		// during the wait does NOT abort the build — the next reader will
		// find the service warm.
		warmDBName := dbName
		svc.SetLazyWarming(true, search.WarmFunc(func() {
			ctx := db.buildCtx
			if ctx == nil {
				ctx = context.Background()
			}
			log.Printf("🔍 Search lazy-warm trigger fired for db %s", warmDBName)
			// Use the lowercase variant which does NOT re-check the lazy
			// gate (EnsureSearchIndexes would defer right back). The
			// service entry is already in db.searchServices because the
			// outer GetOrCreateSearchService call inserted it before
			// SetLazyWarming ran.
			if err := db.ensureSearchIndexesBuilt(ctx, warmDBName); err != nil && !db.shouldIgnoreSearchIndexingError(err) {
				log.Printf("⚠️  Search lazy-warm build failed for db %s: %v", warmDBName, err)
			}
		}))
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
	if db.config != nil && db.config.Memory.SearchRelationshipsEnabled {
		svc.EnableRelationshipSearch(db.relationshipEmbedFunc(dbName))
		svc.SetRelationshipEmbedPolicy(relationshipEmbedPolicy(db.config))
	}

	db.searchServiceCreationMu.Lock()
	defer db.searchServiceCreationMu.Unlock()
	db.searchServicesMu.RLock()
	// This construction was admitted before a retiring phase. That phase
	// joins admitted preparation before advancing the native generation, so
	// its original publication must complete and then be retired by Reset.
	unavailable := db.searchClosing
	db.searchServicesMu.RUnlock()
	if unavailable {
		return nil, ErrClosed
	}
	persistSearchIndexesEnabled := db.config != nil && db.config.Database.DataDir != "" && db.config.Database.PersistSearchIndexes
	svc.SetPersistenceEnabled(persistSearchIndexesEnabled)

	// EXPERIMENTAL: when PersistSearchIndexes is true, set paths so BuildIndexes saves indexes after a
	// build and loads them on startup (skipping the full iteration when both are present).
	// HNSW is also persisted so the approximate nearest-neighbor index does not need rebuilding.
	// If a rebuild is required (e.g., missing/incompatible artifacts), IVF-HNSW rebuild at startup
	// can be long on large datasets (~30 minutes for ~1M embeddings on observed hardware).
	if persistSearchIndexesEnabled {
		base, err := db.activeSearchStateDir(dbName)
		if err != nil {
			return nil, err
		}
		svc.SetFulltextIndexPath(filepath.Join(base, "bm25.v2"))
		svc.SetVectorIndexPath(filepath.Join(base, "vectors"))
		svc.SetHNSWIndexPath(filepath.Join(base, "hnsw"))
	}

	entry := &dbSearchService{
		dbName:    dbName,
		engine:    storageEngine,
		svc:       svc,
		buildDone: make(chan struct{}),
	}
	svc.SetRefreshRetryOwner(db.searchRefreshRetry(entry))

	db.searchServicesMu.Lock()
	if db.searchClosing {
		db.searchServicesMu.Unlock()
		return nil, ErrClosed
	}
	// Double-check in case another goroutine created it while we were building.
	if existing, ok := db.searchServices[dbName]; ok {
		db.searchServicesMu.Unlock()
		return existing.svc, nil
	}
	entry.generation = db.searchGenerations[dbName]
	if db.defaultSearchMetrics != nil && dbName == db.defaultDatabaseName() {
		svc.AttachMetrics(db.defaultSearchMetrics)
	}
	db.searchServices[dbName] = entry
	adopted = true
	db.searchServicesMu.Unlock()
	svc.StartRelationshipIndex()

	return svc, nil
}

// finishSearchConstruction discharges the existing registry's preparation
// admission only after publication or actual failed-candidate disposal. Reset
// joins this population outside the native generation fence; it owns no second
// resource/task registry and does not decide an origin or generation.
func (db *DB) finishSearchConstruction() {
	db.searchServicesMu.Lock()
	db.searchConstructing--
	if db.searchConstructing == 0 {
		close(db.searchConstructionDone)
	}
	db.searchServicesMu.Unlock()
}

func (db *DB) getOrCreateSearchContinuationRegistry() (*resultstream.Registry, error) {
	db.searchContinuationMu.Lock()
	defer db.searchContinuationMu.Unlock()
	if db.searchContinuation != nil {
		return db.searchContinuation, nil
	}
	maxStreams := int64(0)
	maxPageSize := 0
	ttl := 5 * time.Minute
	if db.config != nil {
		maxStreams = db.config.Memory.SearchCursorMax
		maxPageSize = db.config.Memory.SearchCursorMaxPageSize
		if db.config.Memory.SearchCursorTTL > 0 {
			ttl = db.config.Memory.SearchCursorTTL
		}
	}
	registry, err := resultstream.NewRegistry(resultstream.Config{
		Disabled: maxStreams == 0, MaxStreams: maxStreams, TTL: ttl,
		MaxPageSize: maxPageSize,
	})
	if err != nil {
		return nil, err
	}
	db.searchContinuation = registry
	return registry, nil
}

// ResolveSearchContinuationDatabase validates a qid's owner and optional
// requested database before returning its canonical database binding.
func (db *DB) ResolveSearchContinuationDatabase(owner, qid, requested string) (string, error) {
	registry, err := db.getOrCreateSearchContinuationRegistry()
	if err != nil {
		return "", err
	}
	return registry.ResolveDatabase(owner, qid, requested)
}

// DiscardSearchContinuation releases a signed cursor, including after its
// database has been dropped or its final page consumed.
func (db *DB) DiscardSearchContinuation(owner, qid, database string) error {
	registry, err := db.getOrCreateSearchContinuationRegistry()
	if err != nil {
		return err
	}
	return registry.Discard(resultstream.Scope{Owner: owner, Database: database}, qid)
}

// SetSearchResultCachePolicy applies a dynamic cache policy to an existing database service.
func (db *DB) SetSearchResultCachePolicy(dbName string, maxEntries int, ttl time.Duration) {
	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if ok && entry.svc != nil {
		entry.svc.SetSearchResultCachePolicy(maxEntries, ttl)
	}
}

// SetSearchReranker configures the Stage-2 reranker for all per-database search services.
//
// This is typically set by the server when Heimdall is enabled and the
// vector rerank feature flag is turned on.
func (db *DB) SetSearchReranker(r search.Reranker) {
	if local, ok := r.(*search.LocalReranker); ok {
		if _, accepted := db.OwnProvider("local reranker model", local.UnderlyingScorer()); !accepted {
			return
		}
	}
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
// When set, GetOrCreateSearchService uses it instead of the single global searchReranker (enables per-DB rerankers).
func (db *DB) SetRerankerResolver(fn func(dbName string) search.Reranker) {
	db.searchServicesMu.Lock()
	db.rerankerResolver = fn
	db.searchServicesMu.Unlock()
}

// ResetSearchService ends the database's current search state: its service,
// pending refreshes and persisted files. The next call to
// GetOrCreateSearchService creates a fresh service, built from storage.
func (db *DB) ResetSearchService(dbName string) {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}
	db.invalidateSearchState(func(name string) bool { return name == dbName }, true)
}

// DropSearchServiceState is ResetSearchService, for a database being removed.
func (db *DB) DropSearchServiceState(dbName string) {
	db.ResetSearchService(dbName)
}

// dropSearchForPrefix ends the search state of every database a
// DeleteByPrefix of prefix removed entities from: a dropped database ("<db>:"),
// several, or part of one, whether its service is loaded or only persisted. A
// database recreated under the same name then starts from its stored state,
// not the dropped one's index.
func (db *DB) dropSearchForPrefix(prefix string) {
	db.invalidateSearchState(func(name string) bool {
		namespace := name + ":"
		return strings.HasPrefix(namespace, prefix) || strings.HasPrefix(prefix, namespace)
	}, true)
}

func (db *DB) searchServiceNames() []string {
	db.searchServicesMu.RLock()
	defer db.searchServicesMu.RUnlock()
	names := make([]string, 0, len(db.searchServices))
	for name := range db.searchServices {
		names = append(names, name)
	}
	return names
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
	phase := p.Phase
	buildError := ""
	if !p.Ready && !p.Building {
		entry.buildErrMu.RLock()
		if entry.buildErr != nil {
			phase = "failed"
			buildError = entry.buildErr.Error()
		}
		entry.buildErrMu.RUnlock()
	}
	// The lazy-trigger signal flips on once the service is created but the
	// build hasn't started. After Service.EnsureWarm fires the WarmFunc on
	// the first inbound read, building=true and lazyTrigger flips off
	// until restart.
	lazyNeeded := !p.Ready && !p.Building &&
		((bm25On && bm25Warming == "lazy") || (vectorOn && vectorWarming == "lazy")) &&
		!(bm25On == false && vectorOn == false)
	return DatabaseSearchStatus{
		Ready:             p.Ready,
		Building:          p.Building,
		Initialized:       true,
		Strategy:          entry.svc.CurrentStrategy(),
		Phase:             phase,
		Error:             buildError,
		ProcessedNodes:    p.ProcessedNodes,
		TotalNodes:        p.TotalNodes,
		RateNodesPerSec:   p.RateNodesPerSec,
		ETASeconds:        p.ETASeconds,
		BM25Enabled:       bm25On,
		VectorEnabled:     vectorOn,
		LazyTriggerNeeded: lazyNeeded,
	}
}

func (db *DB) shouldStartSearchBuildForWriteEvent(dbName string, progress search.BuildProgress) bool {
	if progress.Building {
		return true
	}
	if progress.Ready {
		return false
	}
	bm25On, vectorOn, bm25Warming, vectorWarming := db.resolveSearchFlags(dbName)
	return (bm25On && bm25Warming == "startup") || (vectorOn && vectorWarming == "startup")
}

func (db *DB) ensurePendingFlushAfterSuccessfulBuild(entry *dbSearchService) {
	if entry == nil || !entry.hasPending() {
		return
	}
	entry.pendingFlushMu.Lock()
	running := entry.pendingFlushRunning
	entry.pendingFlushMu.Unlock()
	if running {
		return
	}
	db.ensurePendingFlush(entry)
}

func (db *DB) startSearchIndexBuild(entry *dbSearchService, ctx context.Context) {
	if entry == nil || entry.svc == nil || entry.isRetired() {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Run the build on its own goroutine: the Once body takes db.mu.RLock
	// through startBackgroundTask, and callers may hold db.mu.Lock while a
	// storage write event fires (e.g. RecordConsent). Running the Once
	// synchronously here would deadlock those callers against the in-flight
	// build (db.mu → once.mu → db.mu).
	rejected := func() {
		entry.buildErrMu.Lock()
		entry.buildErr = ErrClosed
		entry.buildErrMu.Unlock()
		entry.closeBuildDone()
	}
	if !entry.svc.RunBackgroundTask(func() {
		entry.buildOnce.Run(func() {
			if !db.startBackgroundTask(func() { _ = db.runInitialSearchIndexBuild(entry, ctx) }) {
				rejected()
			}
		})
	}) {
		return
	}
}

// runInitialSearchIndexBuild runs a database's initial search-index build and
// records its outcome: the build error, clustering state, the buildDone
// signal, and the flush of writes queued while it ran. It must run inside
// entry.buildOnce, in the background (startSearchIndexBuild) or synchronously
// (DB.BuildSearchIndexes).
func (db *DB) runInitialSearchIndexBuild(entry *dbSearchService, ctx context.Context) error {
	err := entry.svc.BuildIndexes(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("search index build failed for database %s: %v", entry.dbName, err)
	}
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
	if err == nil {
		db.ensurePendingFlushAfterSuccessfulBuild(entry)
	}
	return err
}

func (db *DB) ensurePendingFlush(entry *dbSearchService) {
	if entry == nil || entry.svc == nil || entry.isRetired() {
		return
	}
	entry.pendingFlushMu.Lock()
	if entry.pendingFlushTimer != nil {
		entry.pendingFlushTimer.Stop()
	}
	delay := entry.flushDelay()
	entry.pendingFlushTimer = entry.svc.AfterFunc(delay, func() {
		entry.pendingFlushMu.Lock()
		entry.pendingFlushTimer = nil
		if entry.pendingFlushRunning {
			entry.pendingFlushMu.Unlock()
			return
		}
		entry.pendingFlushRunning = true
		entry.pendingFlushMu.Unlock()

		started := db.startBackgroundTask(func() {
			reschedulePending := true
			defer func() {
				entry.pendingFlushMu.Lock()
				entry.pendingFlushRunning = false
				entry.pendingFlushMu.Unlock()
				if reschedulePending && entry.hasPending() {
					db.ensurePendingFlush(entry)
				}
			}()

			progress := entry.svc.GetBuildProgress()
			if db.shouldStartSearchBuildForWriteEvent(entry.dbName, progress) {
				ctx := db.buildCtx
				if ctx == nil {
					ctx = context.Background()
				}
				db.startSearchIndexBuild(entry, ctx)
				<-entry.buildDone
			} else if !progress.Ready {
				reschedulePending = false
				return
			}

			for {
				nodes, edges := entry.drainPending()
				if len(nodes)+len(edges) == 0 {
					return
				}
				// A refresh whose read fails is retried (searchRefreshRetry),
				// not taken for a deletion.
				for _, id := range collection.SortedKeys(nodes) {
					err := entry.svc.RefreshNodeWithMutation(nodes[id])
					db.logSearchRefreshError("node", id, entry, err)
					if err == nil {
						db.finishSearchRefresh(entry, search.RefreshRetry{Node: nodes[id]})
					}
				}
				for _, id := range collection.SortedKeys(edges) {
					err := entry.svc.RefreshEdgeWithMutation(edges[id])
					db.logSearchRefreshError("relationship", id, entry, err)
					if err == nil {
						db.finishSearchRefresh(entry, search.RefreshRetry{Edge: edges[id]})
					}
				}
				entry.reusePending(nodes, edges)
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

func (db *DB) logSearchRefreshError(kind, id string, entry *dbSearchService, err error) {
	if err != nil && !db.shouldIgnoreSearchIndexingError(err) {
		log.Printf("⚠️ Failed to apply deferred search refresh for %s %s in db %s: %v", kind, id, entry.dbName, err)
	}
}

func (db *DB) ensureSearchIndexesBuilt(ctx context.Context, dbName string) error {
	if dbName == "" {
		dbName = db.defaultDatabaseName()
	}

	db.searchServicesMu.RLock()
	entry, ok := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if !ok || entry == nil {
		return localizedError(localization.NornicDBCoreSearchDatabaseNotInitialized(dbName), nil)
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

// SearchIndexBuildMode selects what EnsureSearchIndexes does once it has
// resolved the database's search service and its per-DB warming decision.
type SearchIndexBuildMode int

const (
	// SearchIndexBuildWait waits for the initial build to finish and reports
	// its outcome, or ctx's error if ctx ends first. When this call starts
	// the build, the build uses ctx; an already-started build keeps its context.
	SearchIndexBuildWait SearchIndexBuildMode = iota
	// SearchIndexBuildStart starts the initial build on the database
	// lifetime's context if it has not started, and returns without waiting
	// for it to finish.
	SearchIndexBuildStart
)

// EnsureSearchIndexes resolves the per-database search service and ensures its
// indexes are built exactly once. If the service does not exist yet it is
// created, using storageEngine if provided. It honours the per-DB master
// switches: when both are off, the service is marked ready (no-op stub for
// BM25, vector flag guard) and no build runs. When every enabled index is
// warming=lazy, the build is deferred to the first inbound search query, which
// fires the WarmFunc registered in GetOrCreateSearchService.
//
// Otherwise the build is initiated according to mode: SearchIndexBuildWait
// waits for the build's outcome, SearchIndexBuildStart only starts it.
func (db *DB) EnsureSearchIndexes(ctx context.Context, dbName string, storageEngine storage.Engine, mode SearchIndexBuildMode) (*search.Service, error) {
	if mode != SearchIndexBuildStart && mode != SearchIndexBuildWait {
		return nil, localizedError(localization.NornicDBCoreSearchIndexBuildModeInvalid(int(mode)), nil)
	}
	svc, err := db.GetOrCreateSearchService(dbName, storageEngine)
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
		// build. The first inbound read path goes through Service.Search
		// (or VectorSearchCandidates) which calls Service.EnsureWarm,
		// which fires the WarmFunc registered in GetOrCreateSearchService
		// to drive the synchronous build.
		return svc, nil
	}
	if mode == SearchIndexBuildStart {
		db.startSearchIndexBuildForDatabase(dbName)
		return svc, nil
	}
	return svc, db.ensureSearchIndexesBuilt(ctx, dbName)
}

// startSearchIndexBuildForDatabase starts the database's initial search build
// on the database lifetime's context, if its service is loaded.
func (db *DB) startSearchIndexBuildForDatabase(dbName string) {
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry == nil {
		return
	}
	ctx := db.buildCtx
	if ctx == nil {
		ctx = context.Background()
	}
	db.startSearchIndexBuild(entry, ctx)
}

// indexNodeFromEvent queues a refresh for a node a storage event reports as
// written. Only its ID is used: the refresh reads the node's stored state.
func (db *DB) indexNodeFromEvent(node *storage.Node) {
	if node == nil {
		return
	}
	// Accepted embedding outcomes publish before storage delivers the worker
	// completion callback. Body events and outcomes awaiting a build use the
	// same deferred refresh owner below.
	if node.EmbeddingAttempt() != nil && db.publishEmbeddedNode(node) {
		return
	}
	db.queueSearchRefreshEvent(searchNodeWrite, string(node.ID))
}

type searchRefreshEventKind uint8

const (
	searchNodeWrite searchRefreshEventKind = iota
	searchNodeDelete
	searchRelationshipChange
)

// queueSearchRefreshEvent resolves a storage event to its database, reserves
// the newest node or relationship refresh under pendingMu, and schedules the
// deferred flush. Node writes can create a service; deletes and relationship
// events only use an existing one.
func (db *DB) queueSearchRefreshEvent(kind searchRefreshEventKind, id string) {
	if kind > searchRelationshipChange {
		return
	}
	dbName, local, ok := splitQualifiedID(id)
	if !ok {
		if kind == searchNodeDelete {
			// Node deletes from an unprefixed engine belong to the default DB.
			dbName, local = db.defaultDatabaseName(), id
		} else {
			if kind == searchNodeWrite {
				log.Printf("⚠️ storage event had unprefixed node ID: %q", id)
			}
			return
		}
	}
	// Qdrant gRPC points are stored under a reserved sub-namespace and are indexed
	// by the Qdrant vector index cache, not the hybrid search service.
	if kind == searchNodeWrite && strings.HasPrefix(local, "qdrant:") {
		return
	}

	// An indexing event needs the existing service, not query-time reranker
	// resolution. Query-facing lookups continue to resolve the current reranker.
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if kind == searchNodeWrite && (entry == nil || entry.svc == nil) {
		svc, err := db.GetOrCreateSearchService(dbName, nil)
		if err != nil || svc == nil {
			return
		}
		db.searchServicesMu.RLock()
		entry = db.searchServices[dbName]
		db.searchServicesMu.RUnlock()
	}
	if entry == nil || entry.svc == nil {
		return
	}
	switch kind {
	case searchNodeWrite:
		if !entry.svc.BM25Enabled() && !entry.svc.VectorEnabled() {
			return
		}
		db.ensureSearchClustering(entry.svc)
	case searchRelationshipChange:
		if !entry.svc.VectorEnabled() && !entry.svc.RelationshipSearchEnabled() {
			return
		}
	}

	// Reserve and replace work while holding the queue lock: a later event
	// must supersede older work, including a refresh already being drained.
	entry.pendingMu.Lock()
	var work search.RefreshRetry
	if !entry.retired {
		if kind == searchRelationshipChange {
			work.Edge = entry.svc.ReserveEdgeMutation(storage.EdgeID(local))
		} else {
			work.Node = entry.svc.ReserveNodeMutation(storage.NodeID(local))
		}
		entry.queueReservedRefreshLocked(local, work)
	}
	entry.pendingMu.Unlock()
	db.scheduleQueuedRefreshes(entry)
}

// publishEmbeddedNode indexes a node the embed worker has just persisted into
// its database's search service at once, when that service is ready and not
// building, and reports whether it did. Otherwise (no service yet, a build
// running or not yet run, or indexing failed) the caller queues the node like
// any write event, and the build or the deferred flush publishes it.
//
// Only the node's ID is used: the refresh reads its stored state, so a newer
// write or delete wins over the worker's copy, and supersedes older queued work.
func (db *DB) publishEmbeddedNode(node *storage.Node) bool {
	if db == nil || node == nil {
		return false
	}
	dbName, local, ok := splitQualifiedID(string(node.ID))
	if !ok || strings.HasPrefix(local, "qdrant:") {
		return false
	}
	db.searchServicesMu.RLock()
	entry := db.searchServices[dbName]
	db.searchServicesMu.RUnlock()
	if entry == nil || entry.svc == nil || entry.isRetired() {
		return false
	}
	if progress := entry.svc.GetBuildProgress(); !progress.Ready || progress.Building {
		return false
	}
	if err := entry.svc.RefreshNode(storage.NodeID(local)); err != nil {
		if db.shouldIgnoreSearchIndexingError(err) {
			return true
		}
		// The caller queues a retry.
		log.Printf("⚠️ Failed to index embedded node %s in db %s: %v", local, dbName, err)
		return false
	}
	return true
}

// scheduleQueuedRefreshes has queued refreshes applied: by the deferred flush
// once the service is ready, after starting the build a write may trigger.
func (db *DB) scheduleQueuedRefreshes(entry *dbSearchService) {
	progress := entry.svc.GetBuildProgress()
	if db.shouldStartSearchBuildForWriteEvent(entry.dbName, progress) {
		ctx := db.buildCtx
		if ctx == nil {
			ctx = context.Background()
		}
		db.startSearchIndexBuild(entry, ctx)
		db.ensurePendingFlush(entry)
		return
	}
	if progress.Ready {
		db.ensurePendingFlush(entry)
	}
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

	svc, err := db.GetOrCreateSearchService(dbName, nil)
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
	if _, err := db.GetOrCreateSearchService(db.defaultDatabaseName(), db.storage); err != nil {
		log.Printf("⚠️  K-means clustering: failed to initialize default search service: %v", err)
	}

	// If storage can enumerate namespaces, initialize per-database services so
	// clustering can run across all known databases (excluding system).
	if lister, ok := db.baseStorage.(storage.NamespaceLister); ok {
		for _, ns := range lister.ListNamespaces() {
			if ns == "" || ns == "system" {
				continue
			}
			if _, err := db.GetOrCreateSearchService(ns, nil); err != nil {
				log.Printf("⚠️  K-means clustering: failed to initialize search service for db %s: %v", ns, err)
			}
		}
	}

	db.runClusteringAllDatabases(ctx, clusteringStale, true)
}

// clusteringStale is the timer's and manual trigger's rule: a database is
// clustered when its vector count changed since its last clustering.
func clusteringStale(entry *dbSearchService, count int) bool {
	return count != entry.lastClusteredEmbedCount || entry.lastClusteredEmbedCount == 0
}

// clusteringGrowthDue is the growth check's rule: a database is clustered once
// its vector count has grown by at least ratio since its last clustering and
// then held still for one check, so a bulk load is clustered when it finishes
// instead of at the next timer tick. It records the count it observed.
func clusteringGrowthDue(ratio float64) func(entry *dbSearchService, count int) bool {
	return func(entry *dbSearchService, count int) bool {
		previous := entry.growthObservedCount
		entry.growthObservedCount = count
		grown := float64(count-entry.lastClusteredEmbedCount) >= ratio*float64(max(entry.lastClusteredEmbedCount, 1))
		return grown && count > entry.lastClusteredEmbedCount && count == previous
	}
}

// runClusteringAllDatabases runs k-means for each open database for which due
// (called under the entry's clusterMu with its vector count) reports true.
// logDeferred logs a due database skipped because its search build is not
// ready.
func (db *DB) runClusteringAllDatabases(ctx context.Context, due func(entry *dbSearchService, count int) bool, logDeferred bool) {
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
		// Do not run timer/manual clustering while a search build is in progress
		// (BuildIndexes warmup already runs BM25-seeded k-means + IVF-HNSW when
		// enabled) or before the initial build is ready, which would cluster
		// partial indexes.
		// A clustering run already in progress is skipped quietly.
		clustering := entry.svc.ClusteringInProgress()
		if progress.Building || !progress.Ready || clustering {
			if !clustering && logDeferred {
				log.Printf("🔬 K-means clustering deferred for db %s: search build in progress or not ready (building=%t, phase=%s)", entry.dbName, progress.Building, progress.Phase)
			}
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
		shouldRun := due(entry, currentCount)
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
