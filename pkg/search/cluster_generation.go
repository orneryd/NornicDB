package search

import "github.com/orneryd/nornicdb/pkg/gpu"

// clusterStagingOp is one cluster-index admission made while a generation was
// being built: an embedding added (or updated) or removed.
type clusterStagingOp struct {
	id    string
	added bool
}

// admitClusterVectorLocked adds or updates id in the cluster index. Callers
// hold indexMu, the admission boundary from which a generation being built
// catches up (installClusterGeneration).
func (s *Service) admitClusterVectorLocked(id string, vec []float32) {
	if s.clusterIndex == nil {
		return
	}
	_ = s.clusterIndex.Add(id, vec) // Best effort
	if s.clusterStaging != nil {
		*s.clusterStaging = append(*s.clusterStaging, clusterStagingOp{id: id, added: true})
	}
}

// retireClusterVectorLocked removes id from the cluster index; callers hold
// indexMu (see admitClusterVectorLocked).
func (s *Service) retireClusterVectorLocked(id string) {
	if s.clusterIndex == nil {
		return
	}
	s.clusterIndex.Remove(id)
	if s.clusterStaging != nil {
		*s.clusterStaging = append(*s.clusterStaging, clusterStagingOp{id: id})
	}
}

// openClusterStaging starts recording admissions for a generation about to
// be computed. The snapshot is taken afterwards, so replaying the log is
// idempotent: an add of a captured id or a remove of an absent id is a no-op
// in effect.
func (s *Service) openClusterStaging() {
	s.indexMu.Lock()
	s.clusterStaging = &[]clusterStagingOp{}
	s.indexMu.Unlock()
}

// closeClusterStaging stops recording when no generation will be installed.
func (s *Service) closeClusterStaging() {
	s.indexMu.Lock()
	s.clusterStaging = nil
	s.indexMu.Unlock()
}

// reconcileClusterStagingLocked replays the admissions made since the
// snapshot into the unpublished generation's membership and graphs. Callers
// hold indexMu (no admission runs) and clusterStateMu, and close the log.
func (s *Service) reconcileClusterStagingLocked(clusterIndex *gpu.ClusterIndex, clustering *gpu.Clustering, graphs map[int]*HNSWIndex) {
	if s.clusterStaging == nil {
		return
	}
	var lookup VectorLookup
	if graphs != nil {
		lookup = s.getVectorLookup()
	}
	for _, op := range *s.clusterStaging {
		previous, wasMember := clustering.Remove(op.id)
		if wasMember && graphs[previous] != nil {
			graphs[previous].Remove(op.id)
		}
		if !op.added {
			continue
		}
		cluster, ok := clusterIndex.AssignToClustering(clustering, op.id)
		if !ok || graphs[cluster] == nil || lookup == nil {
			continue
		}
		if vec, ok := lookup(op.id); ok && len(vec) > 0 {
			_ = graphs[cluster].Add(op.id, vec)
		}
	}
}
