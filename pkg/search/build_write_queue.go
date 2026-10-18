package search

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A live write never waits for an index build. BuildIndexes owns buildMu for
// the whole build, and its population and checkpoint restoration must not be
// overtaken by a live write (buildMu's contract). While a build owns buildMu,
// or waits to own it, a live write therefore hands itself to the owner instead
// of waiting for buildMu: it is queued here and the write returns. The owner
// applies the queue where it already serializes its own work: before each
// node batch, between streamed edges, after the warmup reloads BM25, and
// finally before it releases buildMu. A node or edge a queued write set is
// never replaced by the build's older storage copy. The last write per ID
// wins, as it would after a build that writes waited for.
//
// A service with an index memory budget keeps waiting for buildMu, so a write
// still returns the budget's real answer.

// deferredIndexWrite is one live write queued on the build that owns buildMu.
type deferredIndexWrite struct {
	node   *storage.Node // IndexNode
	nodeID string        // RemoveNode
	edge   *storage.Edge // IndexEdge
	edgeID string        // RemoveEdge
}

type buildWriteQueue struct {
	open atomic.Bool // fast path: a build owns or awaits buildMu
	mu   sync.Mutex
	// owners counts builds that own or await buildMu. The queue stays open
	// until the last of them has applied it.
	owners       int
	writes       []deferredIndexWrite
	writtenNodes map[string]struct{}
	writtenEdges map[string]struct{}
}

// beginBuildWrites opens the queue for a build about to take buildMu.
func (s *Service) beginBuildWrites() {
	q := &s.buildWrites
	q.mu.Lock()
	if q.owners == 0 {
		q.writtenNodes, q.writtenEdges = nil, nil
	}
	q.owners++
	q.open.Store(true)
	q.mu.Unlock()
}

// endBuildWrites applies the rest of the queue and, for the last build,
// closes it. Caller owns buildMu and releases it next, so a write that finds
// the queue closed waits only for that release.
func (s *Service) endBuildWrites() {
	q := &s.buildWrites
	q.mu.Lock()
	q.owners--
	if q.owners == 0 {
		q.open.Store(false)
	}
	writes := q.writes
	q.writes = nil
	q.mu.Unlock()
	if s.applyBuildWrites(writes) && !s.buildInProgress.Load() {
		s.schedulePersist()
	}
}

// queueBuildWrite hands write to the build that owns or awaits buildMu and
// reports whether it did. Outside a build it costs one atomic load. A write is
// acknowledged only after the checks the live path would report: an error
// from them is returned and nothing is queued, as a write that waited would
// have failed. The queued copy owns everything the build later reads.
func (s *Service) queueBuildWrite(write deferredIndexWrite) (bool, error) {
	q := &s.buildWrites
	if !q.open.Load() || !s.buildDefersWrites() {
		return false, nil
	}
	if write.node != nil {
		if err := s.admitQueuedNode(write.node); err != nil {
			return false, err
		}
	}
	write.node = copyQueuedNode(write.node)
	write.edge = copyQueuedEdge(write.edge)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.owners == 0 {
		return false, nil
	}
	q.writes = append(q.writes, write)
	return true, nil
}

// admitQueuedNode applies the outcome checks of indexNodeOwned that do not
// depend on the build's progress: the temporal-currency read and the
// dimension rule. While the index has no vectors and its dimensions are not
// fixed, the first vector still selects them, so the dimension rule waits for
// the deferred write.
func (s *Service) admitQueuedNode(node *storage.Node) error {
	if !s.bm25Enabled.Load() && !s.vectorEnabled.Load() {
		return nil
	}
	shouldIndex, err := s.shouldIndexNode(node)
	if err != nil || !shouldIndex {
		return err
	}
	if !s.vectorDimensionsFixed.Load() && s.EmbeddingCount() == 0 {
		return nil
	}
	return s.checkEmbeddingDimensions(node)
}

// readLockOrQueue admits a live write: it queues write on the build that owns
// or awaits buildMu and returns true, or takes buildMu's read lock and returns
// false (the caller releases it). A failed TryRLock with the queue closed
// means a short holder (ClearVectorIndex, or a build releasing buildMu), so
// waiting for it is short.
func (s *Service) readLockOrQueue(write deferredIndexWrite) (queued bool, err error) {
	for {
		if queued, err := s.queueBuildWrite(write); queued || err != nil {
			return queued, err
		}
		if s.buildMu.TryRLock() {
			return false, nil
		}
		if !s.buildWrites.open.Load() || !s.buildDefersWrites() {
			s.buildMu.RLock()
			return false, nil
		}
	}
}

// buildDefersWrites reports whether live writes are queued during a build (no
// index memory budget is configured).
func (s *Service) buildDefersWrites() bool {
	return s.bm25MemoryMaxBytes <= 0 && s.vectorMemoryMaxBytes <= 0 && s.metadataMemoryMaxBytes <= 0
}

// drainBuildWrites applies the writes queued so far. Caller owns buildMu
// (the build) and holds no index lock.
func (s *Service) drainBuildWrites() {
	q := &s.buildWrites
	if !q.open.Load() {
		return
	}
	q.mu.Lock()
	writes := q.writes
	q.writes = nil
	q.mu.Unlock()
	s.applyBuildWrites(writes)
}

// applyBuildWrites applies writes in arrival order through the same bodies the
// live entry points run. An ID is recorded as written only once its write
// applied: a write that failed leaves the build's storage copy in force, as a
// failed write that waited would have. Caller owns buildMu.
func (s *Service) applyBuildWrites(writes []deferredIndexWrite) bool {
	q := &s.buildWrites
	for _, write := range writes {
		var err error
		switch {
		case write.node != nil:
			if err = s.indexNodeOwned(write.node); err == nil {
				q.markWritten(&q.writtenNodes, string(write.node.ID))
			}
		case write.nodeID != "":
			s.removeNodeOwned(storage.NodeID(write.nodeID))
			q.markWritten(&q.writtenNodes, write.nodeID)
		case write.edge != nil:
			if err = s.indexEdgeOwned(write.edge); err == nil {
				q.markWritten(&q.writtenEdges, string(write.edge.ID))
			}
		case write.edgeID != "":
			s.removeEdgeOwned(storage.EdgeID(write.edgeID))
			q.markWritten(&q.writtenEdges, write.edgeID)
		}
		if err != nil {
			s.logPrintf("⚠️ Search: queued index write failed: %v", err)
		}
	}
	return len(writes) > 0
}

func (q *buildWriteQueue) markWritten(set *map[string]struct{}, id string) {
	q.mu.Lock()
	if *set == nil {
		*set = make(map[string]struct{})
	}
	(*set)[id] = struct{}{}
	q.mu.Unlock()
}

// nodeWritten reports whether a queued write set this node during the current
// build, so the build's storage copy, read before it or not, must not replace
// it.
func (s *Service) nodeWritten(id string) bool {
	q := &s.buildWrites
	if !q.open.Load() {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.writtenNodes[id]
	return ok
}

func (s *Service) edgeWritten(id string) bool {
	q := &s.buildWrites
	if !q.open.Load() {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.writtenEdges[id]
	return ok
}

// copyQueuedNode makes a queued node independent of its caller, who may reuse
// any buffer once IndexNode returns: the build reads the queued copy later,
// as the live path read the caller's node before returning. Every value the
// index reads by reference is copied: labels, embedding vectors, and the
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
