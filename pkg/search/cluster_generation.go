package search

import (
	"sort"

	"github.com/orneryd/nornicdb/pkg/gpu"
)

// clusterStagingKind is what a staged admission changed.
type clusterStagingKind uint8

const (
	clusterVectorAdded   clusterStagingKind = iota // embedding added or updated
	clusterVectorRemoved                           // embedding removed
	clusterNodeTouched                             // node (re)indexed or removed: its text may have changed
)

// clusterStagingOp is one admission made while a generation was being built.
type clusterStagingOp struct {
	id   string
	kind clusterStagingKind
}

func (s *Service) stageClusterOpLocked(id string, kind clusterStagingKind) {
	if s.clusterStaging != nil {
		*s.clusterStaging = append(*s.clusterStaging, clusterStagingOp{id: id, kind: kind})
	}
}

// admitClusterVectorLocked adds or updates id in the cluster index. Callers
// hold indexMu, the admission boundary from which a generation being built
// catches up (installClusterGeneration).
func (s *Service) admitClusterVectorLocked(id string, vec []float32) {
	if s.clusterIndex == nil {
		return
	}
	_ = s.clusterIndex.Add(id, vec) // Best effort
	s.stageClusterOpLocked(id, clusterVectorAdded)
}

// retireClusterVectorLocked removes id from the cluster index; callers hold
// indexMu (see admitClusterVectorLocked).
func (s *Service) retireClusterVectorLocked(id string) {
	if s.clusterIndex == nil {
		return
	}
	s.clusterIndex.Remove(id)
	s.stageClusterOpLocked(id, clusterVectorRemoved)
}

// stageClusterNodeTouchedLocked records that a node was indexed or removed,
// including text-only updates that admit no vector; callers hold indexMu.
func (s *Service) stageClusterNodeTouchedLocked(nodeID string) {
	s.stageClusterOpLocked(nodeID, clusterNodeTouched)
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

// reconcileClusterStagingLocked brings an unpublished generation up to the
// population admitted since its snapshot. It is the one place that derives
// what the generation publishes from that final population: membership,
// graphs (Add/Remove) and the lexical profile of every cluster an admission
// changed, by membership or by a member's text. Callers hold indexMu (no
// admission runs) and clusterStateMu, and close the log.
func (s *Service) reconcileClusterStagingLocked(clusterIndex *gpu.ClusterIndex, clustering *gpu.Clustering, graphs map[int]*HNSWIndex, profiles map[int]map[string]float64) {
	if s.clusterStaging == nil || len(*s.clusterStaging) == 0 {
		return
	}
	var lookup VectorLookup
	if graphs != nil {
		lookup = s.getVectorLookup()
	}
	affected := make(map[int]struct{})
	var touched []string
	for _, op := range *s.clusterStaging {
		if op.kind == clusterNodeTouched {
			touched = append(touched, op.id)
			continue
		}
		previous, wasMember := clustering.Remove(op.id)
		if wasMember {
			affected[previous] = struct{}{}
			if graphs[previous] != nil {
				graphs[previous].Remove(op.id)
			}
		}
		if op.kind != clusterVectorAdded {
			continue
		}
		cluster, ok := clusterIndex.AssignToClustering(clustering, op.id)
		if !ok {
			continue
		}
		affected[cluster] = struct{}{}
		if graphs[cluster] == nil || lookup == nil {
			continue
		}
		if vec, ok := lookup(op.id); ok && len(vec) > 0 {
			_ = graphs[cluster].Add(op.id, vec)
		}
	}
	// A touched node's text reaches the profiles of the clusters holding any of
	// its vectors (the node itself, chunks, named and property vectors).
	s.mu.RLock()
	for _, nodeID := range touched {
		ids := []string{nodeID}
		ids = append(ids, s.nodeChunkVectors[nodeID]...)
		for _, id := range s.nodeNamedVector[nodeID] {
			ids = append(ids, id)
		}
		for _, id := range s.nodePropVector[nodeID] {
			ids = append(ids, id)
		}
		for _, id := range ids {
			if cluster, ok := clustering.ClusterOf(id); ok {
				affected[cluster] = struct{}{}
			}
		}
	}
	s.mu.RUnlock()
	clusters := make([]int, 0, len(affected))
	for cluster := range affected {
		clusters = append(clusters, cluster)
	}
	sort.Ints(clusters)
	s.updateClusterLexicalProfiles(profiles, clustering, clusters)
}
