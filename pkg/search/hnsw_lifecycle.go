package search

import (
	"context"
	"os"
	"time"

	"github.com/orneryd/nornicdb/pkg/envutil"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// hnswBuildJob belongs to the service, never the requesting search. Its result
// is immutable after done closes. hnswMu protects the job pointer and pauses.
type hnswBuildJob struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	index  *HNSWIndex
	err    error
}

// startHNSWBuild is the single-flight owner for initial construction and runtime
// transitions. Searches use the exact pipeline while done is still open.
func (s *Service) startHNSWBuild(dimensions int) (*HNSWIndex, *hnswBuildJob, error) {
	// Validate before launching a worker: an absent source cannot serve the
	// exact fallback either. Snapshot without reversing mu -> hnswMu ordering.
	s.mu.RLock()
	sourceErr := validateHNSWVectorSource(s.vectorIndex, s.vectorFileStore)
	s.mu.RUnlock()
	s.hnswMu.Lock()
	defer s.hnswMu.Unlock()
	ctx := s.lifecycleCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if s.hnswBuildPaused > 0 || s.hnswVectorsChanging.Load() {
		return nil, nil, nil
	}
	if s.hnswIndex != nil {
		return s.hnswIndex, nil, nil
	}
	if job := s.hnswBuild; job != nil {
		select {
		case <-job.done:
			// A drop cancelled this generation. A later generation can build.
			job.cancel() // Release a completed failed job before replacing it.
			s.hnswBuild = nil
		default:
			return nil, job, nil
		}
	}
	if sourceErr != nil {
		return nil, nil, sourceErr
	}
	ctx, cancel := context.WithCancel(ctx)
	job := &hnswBuildJob{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	s.hnswBuild = job
	go func() {
		job.index, job.err = s.buildInitialHNSWIndex(ctx, dimensions)
		if job.err != nil && ctx.Err() == nil {
			s.logPrintf("[HNSW] background build failed: %v", job.err)
		}
		close(job.done)
	}()
	return nil, job, nil
}

// validateHNSWVectorSource is shared by scheduling and snapshot construction so
// invalid sources preserve the synchronous, localized initialization error.
func validateHNSWVectorSource(vi *VectorIndex, vfs *VectorFileStore) error {
	if vi != nil || (vfs != nil && vfs.Count() > 0) {
		return nil
	}
	return localizedError(localization.SearchVectorIndexNil(), nil)
}

// invalidateHNSWLocked prevents an obsolete generation from publishing after a
// dimension change, strategy switch or index drop. Caller holds hnswMu.
func (s *Service) invalidateHNSWLocked() {
	s.retireVectorSnapshotLocked()
	s.installHNSWLocked(nil)
}

// retireVectorSnapshotLocked retires captured sources without discarding a
// ready graph whose vectors have only moved between equivalent stores.
// Caller holds hnswMu. A caller already holding indexMu must never join here.
func (s *Service) retireVectorSnapshotLocked() {
	s.advanceSnapshotGeneration()
	if s.hnswBuild != nil {
		s.hnswBuild.cancel()
	}
}

// getOrCreateHNSWIndex waits only for internal callers that need a ready graph.
// Cancelling the wait cannot cancel construction. Request strategy resolution
// calls startHNSWBuild directly and answers with exact search in the meantime.
func (s *Service) getOrCreateHNSWIndex(ctx context.Context, dimensions int) (*HNSWIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	idx, job, err := s.startHNSWBuild(dimensions)
	if err != nil || idx != nil {
		return idx, err
	}
	if job == nil {
		return nil, ErrSearchIndexBuilding
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-job.done:
		return job.index, job.err
	}
}

// pauseHNSWBuild cancels and joins construction before vector storage is reset
// or closed. Call before taking indexMu: installation and mutation replay use it.
// The returned function permits construction from the replacement vector store.
func (s *Service) pauseHNSWBuild() func() {
	s.hnswMu.Lock()
	s.hnswBuildPaused++
	s.retireVectorSnapshotLocked()
	job := s.hnswBuild
	s.hnswMu.Unlock()
	if job != nil {
		<-job.done
	}
	return func() {
		s.hnswMu.Lock()
		if s.hnswBuild == job {
			s.hnswBuild = nil
		}
		s.hnswBuildPaused--
		s.hnswMu.Unlock()
	}
}

// vectorSnapshotToken records admission before reading any source pointers.
// A snapshot begun while sources are changing stays unavailable even when the
// same generation becomes ready: warmup cannot make its old pointers current.
type vectorSnapshotToken struct {
	generation uint64
	available  bool
	// sourceEpoch is read before source pointers, like generation.
	sourceEpoch uint64
}

func (s *Service) captureVectorSnapshot() vectorSnapshotToken {
	return vectorSnapshotToken{
		generation:  s.hnswGeneration.Load(),
		available:   !s.hnswVectorsChanging.Load(),
		sourceEpoch: s.vectorSourceEpoch.Load(),
	}
}

// vectorSnapshotCurrent is the shared admission check for pipeline factories,
// asynchronous transitions and direct brute switches. Retirement advances the
// generation before clearing the cache under pipelineMu; transitions serialize
// their final swap with retirement under indexMu. Callers retain their own
// publication boundary rather than reversing pipelineMu -> indexMu ordering.
func (s *Service) vectorSnapshotCurrent(snapshot vectorSnapshotToken) bool {
	return snapshot.available && s.lifecycleCtx.Err() == nil && s.hnswGeneration.Load() == snapshot.generation && !s.hnswVectorsChanging.Load()
}

// reserveHNSWRebuild gives maintenance the same cancellation/join ownership as
// initial construction. The caller runs the job and closes it on every exit.
func (s *Service) reserveHNSWRebuild(ctx context.Context, old *HNSWIndex) (context.Context, func(), bool) {
	s.hnswMu.Lock()
	defer s.hnswMu.Unlock()
	if s.hnswBuildPaused > 0 || s.hnswVectorsChanging.Load() || s.hnswIndex != old || ctx.Err() != nil {
		return ctx, nil, false
	}
	if job := s.hnswBuild; job != nil {
		select {
		case <-job.done:
		default:
			return ctx, nil, false
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	job := &hnswBuildJob{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	s.hnswBuild = job
	return ctx, func() { cancel(); close(job.done) }, true
}

// buildHNSWSnapshot is the sole snapshot/lease owner for initial builds,
// maintenance rebuilds and strategy transitions. File views never outlive their
// read lease; the published graph always returns to the owning runtime lookup.
func (s *Service) buildHNSWSnapshot(ctx context.Context, dimensions int, config HNSWConfig, vi *VectorIndex, vfs *VectorFileStore, hints map[string]LexicalSeedHint) (*HNSWIndex, hnswBuildStats, error) {
	if err := ctx.Err(); err != nil {
		return nil, hnswBuildStats{}, err
	}
	if err := validateHNSWVectorSource(vi, vfs); err != nil {
		return nil, hnswBuildStats{}, err
	}
	if vfs != nil && (vi == nil || vfs.Count() > 0) {
		total := vfs.Count()
		s.logPrintf("[HNSW] Building from file store: %d vectors", total)
		lookup := s.getVectorLookup()
		iterator := hnswVectorFileStoreIterator(vfs, hints)
		lease, err := vfs.beginReadLease()
		if err == nil {
			defer lease.Close()
			lookup, iterator = lease.Lookup, hnswVectorReadLeaseIterator(lease, hints)
		}
		built, stats, err := buildHNSWWithOptionalGPU(ctx, dimensions, config, lookup, total, iterator, nil)
		if built != nil {
			built.SetVectorLookup(s.getVectorLookup())
		}
		return built, stats, err
	}
	pairs := hnswOrderedVectorIndexPairs(vi, hints)
	s.logPrintf("[HNSW] Building from in-memory index: %d vectors", len(pairs))
	return buildHNSWWithOptionalGPU(ctx, dimensions, config, vi.getVectorRef, len(pairs), hnswPairSliceIterator(pairs), nil)
}

// hnswBeforeInstallHook, set by tests only, runs after an initial HNSW build
// and before the graph is installed: where a vector stored during the build
// used to be lost.
var hnswBeforeInstallHook func(context.Context)

// buildInitialHNSWIndex constructs and publishes one service-owned generation.
func (s *Service) buildInitialHNSWIndex(ctx context.Context, dimensions int) (*HNSWIndex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.hnswMu.RLock()
	if idx := s.hnswIndex; idx != nil {
		s.hnswMu.RUnlock()
		return idx, nil
	}
	s.hnswMu.RUnlock()

	config := HNSWConfigFromEnv()

	s.mu.RLock()
	vfs := s.vectorFileStore
	vi := s.vectorIndex
	ft := s.fulltext()
	s.mu.RUnlock()
	seedHints := s.hnswLexicalSeedHints(ft, config.M)
	if len(seedHints) > 0 {
		s.logPrintf("[HNSW] 🧭 Lexical build metadata: %d ranked node IDs", len(seedHints))
	}

	var built *HNSWIndex
	var buildStats hnswBuildStats

	// Record mutations before taking the vector snapshot. No request owns this
	// build; the job serializes all initial construction for this generation.
	s.indexMu.Lock()
	if err := ctx.Err(); err != nil {
		s.indexMu.Unlock()
		return nil, err
	}
	s.hnswRebuildOverlay.Reset()
	s.hnswRebuildInFlight.Store(true)
	s.indexMu.Unlock()
	defer func() {
		s.indexMu.Lock()
		s.hnswRebuildOverlay.Reset()
		s.hnswRebuildInFlight.Store(false)
		s.indexMu.Unlock()
	}()
	var err error
	built, buildStats, err = s.buildHNSWSnapshot(ctx, dimensions, config, vi, vfs, seedHints)
	if err != nil {
		return nil, err
	}
	// Construction may use immutable memory views. Published graphs resolve
	// through the current owning store, including later memory-to-disk moves.
	built.SetVectorLookup(s.getVectorLookup())
	if hnswBeforeInstallHook != nil {
		hnswBeforeInstallHook(ctx)
	}
	// Replay before publication: the first ANN reader must see every mutation.
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := applyHNSWMutationSnapshot(built, s.hnswRebuildOverlay.snapshot()); err != nil {
		return nil, err
	}
	// Install unless a persisted index was installed while construction ran.
	s.hnswMu.Lock()
	if err := ctx.Err(); err != nil {
		s.hnswMu.Unlock()
		return nil, err
	}
	if s.hnswIndex != nil {
		existing := s.hnswIndex
		s.hnswMu.Unlock()
		return existing, nil
	}

	// Log configuration and stats for observability
	quality := os.Getenv("NORNICDB_VECTOR_ANN_QUALITY")
	if quality == "" {
		quality = "fast"
	}
	s.logPrintf("🔍 HNSW index created: quality=%s M=%d efConstruction=%d efSearch=%d vectors=%d tombstone_ratio=%.2f build_strategy=%s build_backend=%s build_batches=%d build_kernel_errors=%d build_fallback=%t build_duration=%s",
		quality, config.M, config.EfConstruction, config.EfSearch, built.Size(), built.TombstoneRatio(),
		buildStats.Strategy, buildStats.Backend, buildStats.Batches, buildStats.KernelErrors, buildStats.Fallback, buildStats.Duration)

	s.installHNSWLocked(built)
	s.hnswMu.Unlock()
	s.invalidateVectorPipeline()

	s.ensureHNSWMaintenance()
	s.schedulePersist()
	return built, nil
}

func (s *Service) ensureHNSWMaintenance() {
	// One loop at a time, registered with the lifecycle owner: Close cancels
	// and joins it, and a loop that was cancelled can be started again.
	if !s.hnswMaintRunning.CompareAndSwap(false, true) {
		return
	}
	ctx, done, ok := s.runWorker(context.Background(), workerMaintenance)
	if !ok {
		s.hnswMaintRunning.Store(false)
		return
	}
	interval := envDurationMs("NORNICDB_HNSW_MAINT_INTERVAL_MS", 30_000)
	minRebuildInterval := envDurationSec("NORNICDB_HNSW_MIN_REBUILD_INTERVAL_SEC", 60)
	rebuildRatio := envFloat("NORNICDB_HNSW_TOMBSTONE_REBUILD_RATIO", 0.50)
	maxOverhead := envFloat("NORNICDB_HNSW_MAX_TOMBSTONE_OVERHEAD_FACTOR", 2.0)
	enabled := envutil.GetBoolStrict("NORNICDB_HNSW_REBUILD_ENABLED", true)
	go func() {
		defer done()
		defer s.hnswMaintRunning.Store(false)
		s.runHNSWMaintenance(ctx, interval, minRebuildInterval, rebuildRatio, maxOverhead, enabled)
	}()
}

// runHNSWMaintenance is the maintenance loop; its only exit is ctx, which the
// lifecycle owner cancels on stop and Close.
func (s *Service) runHNSWMaintenance(ctx context.Context, interval, minRebuildInterval time.Duration, rebuildRatio, maxOverhead float64, enabled bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !enabled || s.buildInProgress.Load() {
				continue
			}
			_ = s.maybeRebuildHNSW(ctx, rebuildRatio, maxOverhead, minRebuildInterval)
		case <-ctx.Done():
			return
		}
	}
}

// stopHNSWMaintenance cancels and joins the maintenance loop.
func (s *Service) stopHNSWMaintenance() {
	if s == nil {
		return
	}
	s.pauseWorkers(workerMaintenance)()
}

func (s *Service) maybeRebuildHNSW(ctx context.Context, tombstoneRatioThreshold, maxOverheadFactor float64, minInterval time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if minInterval <= 0 {
		minInterval = 60 * time.Second
	}

	last := time.Unix(s.hnswLastRebuildUnix.Load(), 0)
	if !last.IsZero() && time.Since(last) < minInterval {
		return nil
	}
	s.hnswMu.RLock()
	old := s.hnswIndex
	s.hnswMu.RUnlock()
	if old == nil {
		return nil
	}

	// Derive rebuild condition from a single read lock on the index.
	old.mu.RLock()
	total := len(old.nodeLevel)
	live := old.liveCount
	old.mu.RUnlock()
	if total == 0 || live <= 0 {
		return nil
	}

	deleted := total - live
	ratio := float64(deleted) / float64(total)
	overhead := float64(total) / float64(live)
	if ratio <= tombstoneRatioThreshold && overhead <= maxOverheadFactor {
		return nil
	}

	jobCtx, finish, ok := s.reserveHNSWRebuild(ctx, old)
	if !ok {
		return nil
	}
	defer finish()
	ctx = jobCtx
	if !s.hnswRebuildInFlight.CompareAndSwap(false, true) {
		return nil
	}
	s.indexMu.Lock()
	s.hnswRebuildOverlay.Reset()
	s.indexMu.Unlock()
	defer func() {
		s.indexMu.Lock()
		s.hnswRebuildInFlight.Store(false)
		s.hnswRebuildOverlay.Reset()
		s.indexMu.Unlock()
	}()

	s.mu.RLock()
	vfs := s.vectorFileStore
	vi := s.vectorIndex
	fulltext := s.fulltext()
	s.mu.RUnlock()

	rebuilt, _, err := s.buildHNSWSnapshot(ctx, old.dimensions, old.config, vi, vfs, s.hnswLexicalSeedHints(fulltext, old.config.M))
	if err != nil {
		return err
	}
	rebuilt.SetVectorLookup(s.getVectorLookup())
	if err := ctx.Err(); err != nil {
		return err
	}

	// Close the rebuild race under the mutation lock. Replaying the complete
	// overlay is idempotent for vectors already observed by the store snapshot,
	// and captures mutations that arrived while the graph was being rebuilt.
	s.indexMu.Lock()
	s.hnswMu.RLock()
	currentIsOld := s.hnswIndex == old && ctx.Err() == nil
	s.hnswMu.RUnlock()
	if !currentIsOld {
		s.indexMu.Unlock()
		return nil
	}
	if s.hnswRebuildOverlay != nil {
		pending := s.hnswRebuildOverlay.snapshot()
		if err := applyHNSWMutationSnapshot(rebuilt, pending); err != nil {
			s.indexMu.Unlock()
			return err
		}
	}

	// IMPORTANT: do NOT take pipelineMu while holding hnswMu.
	// Search pipeline creation uses lock order pipelineMu -> hnswMu, so taking
	// hnswMu -> pipelineMu here can deadlock.
	swapped := false
	s.hnswMu.Lock()
	if s.hnswIndex == old && ctx.Err() == nil {
		s.installHNSWLocked(rebuilt)
		s.hnswLastRebuildUnix.Store(time.Now().Unix())
		swapped = true
	}
	s.hnswMu.Unlock()
	s.indexMu.Unlock()

	if swapped {
		// Invalidate pipeline after releasing hnswMu so lock order remains consistent.
		s.invalidateVectorPipeline()
	}

	return nil
}
