package search

import (
	"context"
	"sync"
	"sync/atomic"
)

// vectorLifecycle is the single owner of the search service's vector
// lifecycle (family F-I86). Every other file reads lifecycle state through it
// and changes it only through its transition methods:
//
//   - state: Unloaded -> Populating -> Ready, back to Populating on rebuild,
//     to Unloaded when a population that replaced the sources fails, and
//     Closed for good. Readiness is state == Ready.
//   - generations, one per artifact scope (see advanceSnapshotGeneration,
//     advanceSourceEpoch, advanceClusterGeneration). A transition advances
//     the scopes it retires; an artifact built for an older generation of
//     its scope can neither publish nor be persisted. The scopes stay apart
//     so a strategy switch does not discard clustering or compressed work.
//   - the transition lock is Service.indexMu. Transitions hold it; builders
//     never hold it while building, only for the O(1) publication check.
//   - workers: every background job (population admission, HNSW maintenance,
//     clustering, compressed-index builds) registers here, so retirement and
//     Close can cancel and join it whatever context it was started from.
type vectorLifecycle struct {
	state atomic.Int32

	// stepSeq is odd while a source-replacement step runs: the recorded live
	// writes are withdrawn, or a source cleared, until the replay puts them
	// back. Searches do not take indexMu; they wait for an even value and
	// repeat when it changed, so none observes a step half-done. stepDone is
	// closed when the running step ends.
	stepSeq  atomic.Uint64
	stepDone atomic.Pointer[chan struct{}]

	workersMu sync.Mutex
	workers   map[workerKind]*workerSet
	closed    bool
}

type lifecycleState int32

const (
	lifecycleUnloaded lifecycleState = iota
	lifecyclePopulating
	lifecycleReady
	lifecycleClosed
)

func (st lifecycleState) String() string {
	switch st {
	case lifecyclePopulating:
		return "populating"
	case lifecycleReady:
		return "ready"
	case lifecycleClosed:
		return "closed"
	default:
		return "unloaded"
	}
}

// workerKind groups background jobs that teardown pauses together.
type workerKind int

const (
	workerAdmission workerKind = iota
	workerMaintenance
	workerClustering
	workerCompressedBuild
)

// workerSet tracks the running jobs of one kind. A paused set admits no new
// job; its scope cancels every running one.
type workerSet struct {
	paused      int
	scope       context.Context
	scopeCancel context.CancelFunc
	running     sync.WaitGroup
}

func (l *vectorLifecycle) set(kind workerKind) *workerSet {
	if l.workers == nil {
		l.workers = make(map[workerKind]*workerSet)
	}
	w := l.workers[kind]
	if w == nil {
		w = &workerSet{}
		l.workers[kind] = w
	}
	return w
}

// lifecycleState returns the owner's current state.
func (s *Service) lifecycleState() lifecycleState {
	return lifecycleState(s.life.state.Load())
}

// setLifecycleState records a state transition. Closed is terminal.
func (s *Service) setLifecycleState(next lifecycleState) {
	for {
		cur := s.life.state.Load()
		if lifecycleState(cur) == lifecycleClosed {
			return
		}
		if s.life.state.CompareAndSwap(cur, int32(next)) {
			return
		}
	}
}

// advanceSnapshotGeneration retires every captured source snapshot: native
// graph construction and cached pipelines built before it cannot publish.
func (s *Service) advanceSnapshotGeneration() {
	s.hnswGeneration.Add(1)
}

// advanceSourceEpoch retires artifacts derived from the vector content (the
// compressed index, its overlay and saved bundle). Equivalent moves between
// stores keep the epoch.
func (s *Service) advanceSourceEpoch() {
	s.vectorSourceEpoch.Add(1)
}

// advanceClusterGeneration retires the cluster object's derived state: its
// clustering result, per-cluster graphs and lexical profiles.
func (s *Service) advanceClusterGeneration() {
	s.clusterGeneration.Add(1)
}

// runWorker registers a background job of kind. The returned context is
// cancelled by ctx, by pausing or cancelling kind, and by Close; done must be
// called when the job returns. ok is false while kind is paused or after
// Close, and the caller must skip the job.
func (s *Service) runWorker(ctx context.Context, kind workerKind) (context.Context, func(), bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	l := &s.life
	l.workersMu.Lock()
	w := l.set(kind)
	if l.closed || w.paused > 0 || s.lifecycleErr() != nil {
		l.workersMu.Unlock()
		return nil, nil, false
	}
	if w.scope == nil {
		base := s.lifecycleCtx
		if base == nil {
			base = context.Background()
		}
		w.scope, w.scopeCancel = context.WithCancel(base)
	}
	scope := w.scope
	w.running.Add(1)
	l.workersMu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(scope, cancel)
	return runCtx, func() {
		stop()
		cancel()
		w.running.Done()
	}, true
}

// cancelWorkers cancels running jobs of the given kinds without waiting.
// New jobs start in a fresh scope.
func (s *Service) cancelWorkers(kinds ...workerKind) {
	l := &s.life
	l.workersMu.Lock()
	for _, kind := range kinds {
		w := l.set(kind)
		if w.scopeCancel != nil {
			w.scopeCancel()
		}
		w.scope, w.scopeCancel = nil, nil
	}
	l.workersMu.Unlock()
}

// pauseWorkers cancels and joins the given kinds and admits none of them
// until resume runs. Call without holding indexMu or mu: jobs may need them
// to return.
func (s *Service) pauseWorkers(kinds ...workerKind) (resume func()) {
	l := &s.life
	l.workersMu.Lock()
	sets := make([]*workerSet, 0, len(kinds))
	for _, kind := range kinds {
		w := l.set(kind)
		w.paused++
		if w.scopeCancel != nil {
			w.scopeCancel()
		}
		w.scope, w.scopeCancel = nil, nil
		sets = append(sets, w)
	}
	l.workersMu.Unlock()
	// A paused set admits no Add, so these Waits cannot race a new job.
	for _, w := range sets {
		w.running.Wait()
	}
	return func() {
		l.workersMu.Lock()
		for _, w := range sets {
			w.paused--
		}
		l.workersMu.Unlock()
	}
}

// closeWorkers permanently refuses new jobs, then cancels and joins every
// registered one. Close calls it after cancelling the lifecycle context.
func (s *Service) closeWorkers() {
	l := &s.life
	l.workersMu.Lock()
	l.closed = true
	sets := make([]*workerSet, 0, len(l.workers))
	for _, w := range l.workers {
		if w.scopeCancel != nil {
			w.scopeCancel()
		}
		w.scope, w.scopeCancel = nil, nil
		sets = append(sets, w)
	}
	l.workersMu.Unlock()
	for _, w := range sets {
		w.running.Wait()
	}
	s.setLifecycleState(lifecycleClosed)
}

func (s *Service) lifecycleErr() error {
	if s.lifecycleCtx == nil {
		return nil
	}
	return s.lifecycleCtx.Err()
}

// setVectorsChanging marks whether a base population is replacing the vector
// source (F-I86 B4). While set, no ANN construction snapshots the source and
// no pipeline is cached. The population sets it when it starts replacing
// sources and its warmup clears it once every base vector is loaded; the
// population's exit clears it on any other path. This owner method is its
// only writer.
func (s *Service) setVectorsChanging(changing bool) {
	s.hnswVectorsChanging.Store(changing)
}

// changeNodeSourcesLocked is the sole completion/authority writer. The
// replacing operation supplies its actual meaning; no caller infers it from
// counts, a dirty backend, a whole-service ready flag or pointer identity.
func (s *Service) changeNodeSourcesLocked(change nodeSourceChange) {
	switch change {
	case nodeTextRestored:
		s.nodeSources.textComplete = true
	case nodeVectorsRestored:
		s.nodeSources.vectorsComplete = true
	case nodePopulationAdmitted:
		s.nodeSources.textComplete, s.nodeSources.vectorsComplete = true, true
		if s.vectorFileStore == nil {
			s.nodeSources.memoryAuthoritative = true
		}
	case nodePopulationFailed:
		s.nodeSources.textComplete, s.nodeSources.vectorsComplete = false, false
	case nodeVectorMemoryMoved:
		s.nodeSources.memoryAuthoritative = true
	case nodeVectorCleared:
		s.nodeSources.vectorsComplete, s.nodeSources.memoryAuthoritative = true, true
	case nodeVectorReset:
		s.nodeSources.vectorsComplete, s.nodeSources.memoryAuthoritative = false, true
	case nodeVectorFileMoved:
		s.nodeSources.memoryAuthoritative = false
	case nodeFulltextReset:
		s.nodeSources.textComplete = false
	}
}

func (s *Service) setNodeFulltextCheckpointLocked(path string, count int) {
	s.nodeSources.fulltextCheckpoint = path
	s.nodeSources.fulltextCheckpointCount = count
}

// beginSourceStepLocked marks a source-replacement step as running. Caller
// holds indexMu (steps never overlap) and calls the returned end once the
// recorded writes are re-applied.
func (s *Service) beginSourceStepLocked() (end func()) {
	done := make(chan struct{})
	s.life.stepDone.Store(&done)
	s.life.stepSeq.Add(1)
	return func() {
		s.life.stepSeq.Add(1)
		close(done)
	}
}

// waitSourceStepIdle returns the step sequence once no step is running.
func (s *Service) waitSourceStepIdle(ctx context.Context) (uint64, error) {
	for {
		seq := s.life.stepSeq.Load()
		if seq%2 == 0 {
			return seq, nil
		}
		done := s.life.stepDone.Load()
		if done == nil {
			continue
		}
		select {
		case <-*done:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

type sourceStepReadKey struct{}

// readAcrossSourceSteps runs read so that it observes no source-replacement
// step half-done: it waits for any running step, and repeats read when a step
// ran meanwhile. Steps are short (the saved image is staged without indexMu)
// and rare. Nested reads in the same request run once, inside the outer one.
func readAcrossSourceSteps[T any](s *Service, ctx context.Context, read func(context.Context) (T, error)) (T, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Value(sourceStepReadKey{}) != nil {
		return read(ctx)
	}
	// A cancelled request reports its cancellation, whatever the build state:
	// readiness or a running step must not mask the caller's own context.
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	inner := context.WithValue(ctx, sourceStepReadKey{}, true)
	for {
		seq, err := s.waitSourceStepIdle(ctx)
		if err != nil {
			var zero T
			return zero, err
		}
		result, err := read(inner)
		if s.life.stepSeq.Load() == seq {
			return result, err
		}
	}
}

// inSourceStepRead reports whether ctx already belongs to a read wrapped by
// readAcrossSourceSteps.
func inSourceStepRead(ctx context.Context) bool {
	return ctx != nil && ctx.Value(sourceStepReadKey{}) != nil
}
