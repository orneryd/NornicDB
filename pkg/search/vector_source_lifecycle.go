package search

import (
	"context"
)

// vectorMemorySnapshot preserves the factory's captured source through every
// descriptor/fallback. Direct internal callers can take the same owned snapshot.
func (s *Service) vectorMemorySnapshot(source []*VectorIndex) *VectorIndex {
	if len(source) > 0 {
		return source[0]
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.vectorIndex
}

// pauseBaseBuild excludes new base loading and joins the current caller before
// source retirement. Clear/Close also cancel it; a live storage toggle lets it
// finish. Never call while holding indexMu or another lock the builder needs.
func (s *Service) pauseBaseBuild(cancelBuild bool) func() {
	s.baseBuildControlMu.Lock()
	s.baseBuildPaused++
	if cancelBuild && s.baseBuildCancel != nil {
		s.baseBuildCancel()
	}
	s.baseBuildControlMu.Unlock()
	s.buildMu.Lock()
	return func() {
		s.baseBuildControlMu.Lock()
		s.baseBuildPaused--
		s.baseBuildControlMu.Unlock()
		s.buildMu.Unlock()
	}
}

// ownBaseBuildContext registers the one build admitted under buildMu and also
// cancels it on service shutdown. Caller cancellation remains valid for this
// explicit base-loading operation; the shared HNSW job has separate ownership.
func (s *Service) ownBaseBuildContext(ctx context.Context) (context.Context, func(), error) {
	lifetime := s.lifecycleCtx
	if lifetime == nil {
		lifetime = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifetime, cancel)
	s.baseBuildControlMu.Lock()
	if err := lifetime.Err(); err != nil {
		s.baseBuildControlMu.Unlock()
		stop()
		cancel()
		return nil, nil, err
	}
	if s.baseBuildPaused > 0 {
		s.baseBuildControlMu.Unlock()
		stop()
		cancel()
		return nil, nil, ErrSearchIndexBuilding
	}
	s.baseBuildCancel = cancel
	s.baseBuildControlMu.Unlock()
	return ctx, func() {
		stop()
		cancel()
		s.baseBuildControlMu.Lock()
		s.baseBuildCancel = nil
		s.baseBuildControlMu.Unlock()
	}, nil
}

// retireVectorSourceLocked is the final publication barrier for source owners.
// Caller owns indexMu and has released source locks. Equivalent storage moves
// can keep a ready HNSW graph because it uses the service lookup; destruction
// retires that graph too. Every case cancels captured construction and retires
// wrappers/scorers before crossing the final pipeline publication boundary.
func (s *Service) retireVectorSourceLocked(keepGraph bool) {
	if !keepGraph {
		// Destruction also retires derived compressed indexes. They are not
		// cleared here: a builder may hold ivfpqMu for a long time, so reuse
		// and publication check this epoch instead.
		s.vectorSourceEpoch.Add(1)
	}
	s.hnswMu.Lock()
	if keepGraph {
		s.retireVectorSnapshotLocked()
	} else {
		s.invalidateHNSWLocked()
	}
	s.hnswMu.Unlock()
	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()
}

// vectorMemoryFromFileStore transfers canonical normalized coordinates in
// small batches. Only the replacement source retains vector payloads; no full
// vector snapshot or second normalized/raw copy is constructed.
func vectorMemoryFromFileStore(store *VectorFileStore) (*VectorIndex, error) {
	memory := NewVectorIndex(store.GetDimensions())
	batchSize := max(1, min(256, (1<<20)/max(1, memory.dimensions*4)))
	err := store.IterateChunked(batchSize, func(ids []string, values [][]float32) error {
		for i, id := range ids {
			// IterateChunked creates an independent slice for every vector.
			// It is already normalized: preserve its bits and ownership.
			memory.vectors[id] = values[i]
		}
		return nil
	})
	return memory, err
}
