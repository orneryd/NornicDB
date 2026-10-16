package search

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/gpu"
)

// clusterState reads the cluster object and its generation together, so a
// reader can later tell whether state derived from it is still current.
func (s *Service) clusterState() (*gpu.ClusterIndex, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clusterIndex, s.clusterGeneration.Load()
}

// retireClusterGenerationLocked retires the current cluster object's derived
// state. Caller holds mu, so readers of clusterState see the pointer and the
// generation change together.
func (s *Service) retireClusterGenerationLocked() {
	s.clusterGeneration.Add(1)
}

// ownClusterRun registers clustering work with the service so Clear and Close
// can cancel and join it whatever context the caller supplied. It returns
// false when teardown is in progress or the service is closed; the caller
// skips the work.
func (s *Service) ownClusterRun(ctx context.Context) (context.Context, func(), bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.clusterRunMu.Lock()
	if s.clusterRunPaused > 0 || (s.lifecycleCtx != nil && s.lifecycleCtx.Err() != nil) {
		s.clusterRunMu.Unlock()
		return nil, nil, false
	}
	if s.clusterRunScope == nil {
		base := s.lifecycleCtx
		if base == nil {
			base = context.Background()
		}
		s.clusterRunScope, s.clusterRunScopeCancel = context.WithCancel(base)
	}
	scope := s.clusterRunScope
	s.clusterRuns.Add(1)
	s.clusterRunMu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(scope, cancel)
	return runCtx, func() {
		stop()
		cancel()
		s.clusterRuns.Done()
	}, true
}

// cancelClusterRunsLocked cancels running clustering without waiting. Caller
// holds clusterRunMu. The next run starts a fresh scope.
func (s *Service) cancelClusterRunsLocked() {
	if s.clusterRunScopeCancel != nil {
		s.clusterRunScopeCancel()
	}
	s.clusterRunScope, s.clusterRunScopeCancel = nil, nil
}

// cancelClusterRuns stops work on a cluster object that is being replaced.
func (s *Service) cancelClusterRuns() {
	s.clusterRunMu.Lock()
	s.cancelClusterRunsLocked()
	s.clusterRunMu.Unlock()
}

// pauseClusterRuns cancels and joins clustering work and refuses new work
// until the returned function runs. Call before taking indexMu or mu.
func (s *Service) pauseClusterRuns() func() {
	s.clusterRunMu.Lock()
	s.clusterRunPaused++
	s.cancelClusterRunsLocked()
	s.clusterRunMu.Unlock()
	s.clusterRuns.Wait()
	return func() {
		s.clusterRunMu.Lock()
		s.clusterRunPaused--
		s.clusterRunMu.Unlock()
	}
}
