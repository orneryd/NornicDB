package search

import "github.com/orneryd/nornicdb/pkg/gpu"

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
