package search

import "github.com/orneryd/nornicdb/pkg/gpu"

// Publication through the lifecycle owner (F-I86 B3). These are the only
// writers of the derived vector artifacts: the cached query pipeline, the
// native HNSW graph, the per-cluster graphs, the compressed index and the GPU
// brute index. Builders work without owner locks and publish here; each
// method documents the lock its caller holds or takes it itself, in the
// canonical order indexMu -> mu -> (hnswMu | clusterHNSWMu | ivfpqMu), and
// pipelineMu never under mu or hnswMu.

// invalidateVectorPipeline drops the cached query pipeline so the next query
// rebuilds it from the current artifacts. Call without mu or hnswMu.
func (s *Service) invalidateVectorPipeline() {
	s.pipelineMu.Lock()
	s.vectorPipeline = nil
	s.pipelineMu.Unlock()
}

// storeVectorPipelineLocked caches p for later queries. Caller holds
// pipelineMu and has checked that p was built from current artifacts.
func (s *Service) storeVectorPipelineLocked(p *VectorSearchPipeline) {
	s.vectorPipeline = p
}

// publishVectorPipeline caches p, built by a completed transition.
func (s *Service) publishVectorPipeline(p *VectorSearchPipeline) {
	s.pipelineMu.Lock()
	s.storeVectorPipelineLocked(p)
	s.pipelineMu.Unlock()
}

// installHNSWLocked makes idx the native graph (nil retires it). Caller holds
// hnswMu and has checked idx against the current snapshot generation; it
// invalidates the pipeline after releasing hnswMu.
func (s *Service) installHNSWLocked(idx *HNSWIndex) {
	s.hnswIndex = idx
}

// publishClusterGraphs installs per-cluster graphs built for generation.
// It reports false, and installs nothing, when the cluster state has been
// retired meanwhile: retirement advances the generation before clearing
// under the same lock, so this check either precedes the clear or sees it.
func (s *Service) publishClusterGraphs(graphs map[int]*HNSWIndex, generation uint64) bool {
	s.clusterHNSWMu.Lock()
	if s.clusterGeneration.Load() != generation {
		s.clusterHNSWMu.Unlock()
		return false
	}
	s.clusterHNSW = graphs
	s.clusterHNSWMu.Unlock()
	s.invalidateVectorPipeline()
	return true
}

// installIVFPQLocked makes idx the compressed index of sourceEpoch. Caller
// holds ivfpqMu and has checked that sourceEpoch is still current.
func (s *Service) installIVFPQLocked(idx *IVFPQIndex, sourceEpoch uint64) {
	s.ivfpqIndex = idx
	s.ivfpqEpoch = sourceEpoch
}

// setGPUEmbeddingIndexLocked installs (or, with nil, detaches) the GPU brute
// index. Caller holds mu.
func (s *Service) setGPUEmbeddingIndexLocked(idx *gpu.EmbeddingIndex) {
	s.gpuEmbeddingIndex = idx
}

// retireDerivedANNLocked retires the cluster-derived graphs and, with
// dropCompressed, the compressed index and its mutation overlay. The cluster
// generation advances first, so a builder finishing later cannot publish into
// the replacement. dropLexical also clears the cluster lexical profiles.
// Caller holds indexMu and not mu.
func (s *Service) retireDerivedANNLocked(dropCompressed, dropLexical bool) {
	s.advanceClusterGeneration()
	s.clusterHNSWMu.Lock()
	s.clusterHNSW = nil
	s.clusterHNSWMu.Unlock()
	if dropCompressed {
		s.ivfpqMu.Lock()
		s.installIVFPQLocked(nil, s.ivfpqEpoch)
		s.ivfpqOverlay.Reset()
		s.ivfpqMu.Unlock()
	}
	if dropLexical {
		s.clearClusterLexicalProfiles()
	}
}

// publishPersistedArtifact runs commit, the final durable publication of an
// artifact saved for sourceEpoch, inside the retirement barrier: source
// retirement deletes saved artifacts and advances the epoch under indexMu, so
// an artifact whose source was retired while it was being written is not
// published (false) and its staging is discarded by the caller.
func (s *Service) publishPersistedArtifact(sourceEpoch uint64, commit func() error) (bool, error) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if s.vectorSourceEpoch.Load() != sourceEpoch {
		return false, nil
	}
	return true, commit()
}

// setVectorIndexLocked and setVectorFileStoreLocked replace the live vector
// source. Caller holds indexMu and mu, and retires the replaced source
// (retireVectorSourceLocked) after releasing mu.
func (s *Service) setVectorIndexLocked(vi *VectorIndex) {
	s.vectorIndex = vi
}

func (s *Service) setVectorFileStoreLocked(vfs *VectorFileStore) {
	s.vectorFileStore = vfs
}
