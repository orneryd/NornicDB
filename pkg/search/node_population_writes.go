package search

import (
	"maps"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A population owns the node sources (BM25, the vector source, the node query
// metadata and the relationship vectors) from its admission until it
// publishes NODE. Live writes during that window never wait for it and never
// touch the sources: the write is queued on the population job and returns.
// The population is the only writer of its sources, so its snapshot reads,
// batches and base persistence never race a live write. It applies the queue
// between its own steps, last write per ID winning, and an ID a queued write
// set is never overwritten by the population's older storage copy. Publishing
// NODE (publishNodeAdmission) applies the rest of the queue under the same
// locks that admit later writes directly, so the published sources hold
// exactly what a service without a build would hold.
//
// A service with an index memory budget keeps waiting admission instead: its
// write returns the budget's real answer, which only the populated sources can
// give.

// deferredIndexWrite is one live write queued on a population.
type deferredIndexWrite struct {
	node   *storage.Node // IndexNode
	nodeID string        // RemoveNode
	edge   *storage.Edge // IndexEdge
	edgeID string        // RemoveEdge
}

// populationDefersWrites reports whether live writes are queued during a
// population (no index memory budget is configured).
func (s *Service) populationDefersWrites() bool {
	return s.bm25MemoryMaxBytes <= 0 && s.vectorMemoryMaxBytes <= 0 && s.metadataMemoryMaxBytes <= 0
}

// queueOnPopulation queues write on the running population and reports
// whether it did. It takes only baseBuildControlMu, never indexMu, so a write
// never waits for anything the population holds indexMu for. Outside a
// population it costs one atomic load.
func (s *Service) queueOnPopulation(write deferredIndexWrite) (bool, error) {
	if !s.populationRunning.Load() || !s.populationDefersWrites() {
		return false, nil
	}
	if s.lifecycleCtx != nil {
		if err := s.lifecycleCtx.Err(); err != nil {
			return false, err
		}
	}
	s.baseBuildControlMu.Lock()
	defer s.baseBuildControlMu.Unlock()
	job := s.nodeAdmission
	if job == nil || nodeAdmissionClosed(job) || s.baseBuildPaused > 0 {
		return false, nil
	}
	write.node = copyQueuedNode(write.node)
	write.edge = copyQueuedEdge(write.edge)
	job.deferred = append(job.deferred, write)
	job.queued.Store(true)
	return true, nil
}

// takeDeferredLocked removes and returns the queued writes. Caller holds
// baseBuildControlMu.
func (job *nodePopulationAdmission) takeDeferredLocked() []deferredIndexWrite {
	writes := job.deferred
	job.deferred = nil
	job.queued.Store(false)
	return writes
}

// copyQueuedNode keeps a queued node independent of a caller that changes its
// labels or properties after IndexNode returns. Embedding vectors are shared:
// callers replace them, never modify them in place.
func copyQueuedNode(node *storage.Node) *storage.Node {
	if node == nil {
		return nil
	}
	copied := *node
	copied.Labels = slices.Clone(node.Labels)
	copied.Properties = maps.Clone(node.Properties)
	copied.NamedEmbeddings = maps.Clone(node.NamedEmbeddings)
	copied.ChunkEmbeddings = slices.Clone(node.ChunkEmbeddings)
	copied.EmbedMeta = maps.Clone(node.EmbedMeta)
	return &copied
}

func copyQueuedEdge(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	copied := *edge
	copied.Properties = maps.Clone(edge.Properties)
	return &copied
}

// applyDeferredIndexWrites applies the queued writes. Only the population
// calls it, between its steps, holding no index lock. A first vector that
// fixes the index dimensions is admitted outside indexMu first, as the live
// boundary does.
func (s *Service) applyDeferredIndexWrites(job *nodePopulationAdmission) {
	if job == nil || !job.queued.Load() {
		return
	}
	s.baseBuildControlMu.Lock()
	writes := job.takeDeferredLocked()
	s.baseBuildControlMu.Unlock()
	for _, write := range writes {
		if write.node != nil {
			s.maybeAutoSetVectorDimensions(firstVectorDimensions(write.node))
		} else if write.edge != nil {
			s.maybeAutoSetVectorDimensions(firstEdgeVectorDimensions(write.edge))
		}
	}
	s.indexMu.Lock()
	s.applyDeferredIndexWritesLocked(job, writes)
	s.indexMu.Unlock()
}

// applyDeferredIndexWritesLocked applies queued writes in arrival order and
// records each ID as written. Caller holds indexMu and is the population.
func (s *Service) applyDeferredIndexWritesLocked(job *nodePopulationAdmission, writes []deferredIndexWrite) {
	bm25On := s.bm25Enabled.Load()
	for _, write := range writes {
		var err error
		switch {
		case write.node != nil:
			job.markNodeWrittenLocked(string(write.node.ID))
			if err = s.admitNodeWriteLocked(true); err == nil {
				err = s.indexNodeLocked(write.node, !bm25On)
			}
		case write.nodeID != "":
			job.markNodeWrittenLocked(write.nodeID)
			if err = s.admitNodeWriteLocked(true); err == nil {
				s.removeNodeLocked(write.nodeID)
			}
		case write.edge != nil:
			job.markEdgeWrittenLocked(string(write.edge.ID))
			if err = s.admitNodeWriteLocked(false); err == nil {
				err = s.indexEdgeLocked(write.edge)
			}
		case write.edgeID != "":
			job.markEdgeWrittenLocked(write.edgeID)
			if err = s.admitNodeWriteLocked(false); err == nil {
				s.removeEdgeLocked(write.edgeID)
			}
		}
		if err != nil {
			s.logPrintf("⚠️ Search: queued index write failed: %v", err)
		}
	}
}

func (job *nodePopulationAdmission) markNodeWrittenLocked(id string) {
	if job.writtenNodes == nil {
		job.writtenNodes = make(map[string]struct{})
	}
	job.writtenNodes[id] = struct{}{}
}

func (job *nodePopulationAdmission) markEdgeWrittenLocked(id string) {
	if job.writtenEdges == nil {
		job.writtenEdges = make(map[string]struct{})
	}
	job.writtenEdges[id] = struct{}{}
}

// nodeWrittenLocked reports whether a queued write set this node, so the
// population's storage copy, read before it or not, must not replace it.
// Caller holds indexMu.
func (job *nodePopulationAdmission) nodeWrittenLocked(id string) bool {
	if job == nil {
		return false
	}
	_, ok := job.writtenNodes[id]
	return ok
}

func (job *nodePopulationAdmission) edgeWrittenLocked(id string) bool {
	if job == nil {
		return false
	}
	_, ok := job.writtenEdges[id]
	return ok
}
