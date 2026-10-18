package search

import (
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
// population it costs one atomic load. A write is acknowledged only after the
// checks the live path would report (the temporal-currency read): an error is
// returned and nothing is queued. The queued copy owns everything the
// population later reads.
func (s *Service) queueOnPopulation(write deferredIndexWrite) (bool, error) {
	if !s.populationRunning.Load() || !s.populationDefersWrites() {
		return false, nil
	}
	if s.lifecycleCtx != nil {
		if err := s.lifecycleCtx.Err(); err != nil {
			return false, err
		}
	}
	if write.node != nil {
		if _, err := s.shouldIndexNode(write.node); err != nil {
			return false, err
		}
	}
	write.node = copyQueuedNode(write.node)
	write.edge = copyQueuedEdge(write.edge)
	s.baseBuildControlMu.Lock()
	defer s.baseBuildControlMu.Unlock()
	job := s.nodeAdmission
	if job == nil || nodeAdmissionClosed(job) || s.baseBuildPaused > 0 {
		return false, nil
	}
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

// copyQueuedNode makes a queued node independent of its caller, who may reuse
// any buffer once IndexNode returns: the population reads the queued copy
// later, as the live path read the caller's node before returning. Every value
// the index reads by reference is copied: labels, embedding vectors, and the
// slices and maps inside properties (property vectors) and embedding metadata.
// The struct copy keeps every other field.
func copyQueuedNode(node *storage.Node) *storage.Node {
	if node == nil {
		return nil
	}
	copied := *node
	copied.Labels = slices.Clone(node.Labels)
	copied.Properties = cloneQueuedMap(node.Properties)
	copied.EmbedMeta = cloneQueuedMap(node.EmbedMeta)
	if node.NamedEmbeddings != nil {
		copied.NamedEmbeddings = make(map[string][]float32, len(node.NamedEmbeddings))
		for name, embedding := range node.NamedEmbeddings {
			copied.NamedEmbeddings[name] = slices.Clone(embedding)
		}
	}
	if node.ChunkEmbeddings != nil {
		copied.ChunkEmbeddings = make([][]float32, len(node.ChunkEmbeddings))
		for i, embedding := range node.ChunkEmbeddings {
			copied.ChunkEmbeddings[i] = slices.Clone(embedding)
		}
	}
	return &copied
}

func copyQueuedEdge(edge *storage.Edge) *storage.Edge {
	if edge == nil {
		return nil
	}
	copied := *edge
	copied.Properties = cloneQueuedMap(edge.Properties)
	return &copied
}

func cloneQueuedMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	copied := make(map[string]any, len(source))
	for key, value := range source {
		copied[key] = cloneQueuedValue(value)
	}
	return copied
}

// cloneQueuedValue copies the mutable containers a property value can hold;
// scalars and strings are immutable and shared.
func cloneQueuedValue(value any) any {
	switch v := value.(type) {
	case []float32:
		return slices.Clone(v)
	case []float64:
		return slices.Clone(v)
	case []int:
		return slices.Clone(v)
	case []int64:
		return slices.Clone(v)
	case []int32:
		return slices.Clone(v)
	case []string:
		return slices.Clone(v)
	case []bool:
		return slices.Clone(v)
	case []byte:
		return slices.Clone(v)
	case []any:
		copied := make([]any, len(v))
		for i, item := range v {
			copied[i] = cloneQueuedValue(item)
		}
		return copied
	case map[string]any:
		return cloneQueuedMap(v)
	default:
		return value
	}
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

// applyDeferredIndexWritesLocked applies queued writes in arrival order. An ID
// is recorded as written only once its write applied: a write that failed
// leaves the population's storage copy in force, as a failed write that
// waited would have. Caller holds indexMu and is the population.
func (s *Service) applyDeferredIndexWritesLocked(job *nodePopulationAdmission, writes []deferredIndexWrite) {
	bm25On := s.bm25Enabled.Load()
	for _, write := range writes {
		var err error
		switch {
		case write.node != nil:
			if err = s.admitNodeWriteLocked(true); err == nil {
				err = s.indexNodeLocked(write.node, !bm25On)
			}
			if err == nil {
				job.markNodeWrittenLocked(string(write.node.ID))
			}
		case write.nodeID != "":
			if err = s.admitNodeWriteLocked(true); err == nil {
				s.removeNodeLocked(write.nodeID)
				job.markNodeWrittenLocked(write.nodeID)
			}
		case write.edge != nil:
			if err = s.admitNodeWriteLocked(false); err == nil {
				err = s.indexEdgeLocked(write.edge)
			}
			if err == nil {
				job.markEdgeWrittenLocked(string(write.edge.ID))
			}
		case write.edgeID != "":
			if err = s.admitNodeWriteLocked(false); err == nil {
				s.removeEdgeLocked(write.edgeID)
				job.markEdgeWrittenLocked(write.edgeID)
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
