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
	apply  func()        // an administrative operation (orderWithBuild)
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
	// dimsAdopted is set once the build has chosen the index dimensions from
	// a stored vector (adoptBuildVectorDimensions).
	dimsAdopted bool
	// pendingOps counts queued administrative operations not applied yet.
	pendingOps int
}

// beginBuildWrites opens the queue for a build about to take buildMu.
func (s *Service) beginBuildWrites() {
	q := &s.buildWrites
	q.mu.Lock()
	if q.owners == 0 {
		q.writtenNodes, q.writtenEdges = nil, nil
		q.dimsAdopted = false
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
func (s *Service) queueBuildWrite(write deferredIndexWrite) (queued, wait bool, err error) {
	q := &s.buildWrites
	if !q.open.Load() || !s.buildDefersWrites() {
		return false, false, nil
	}
	if write.node != nil {
		if wait, err := s.admitQueuedNode(write.node); wait || err != nil {
			return false, wait, err
		}
	}
	write.node = copyQueuedNode(write.node)
	write.edge = copyQueuedEdge(write.edge)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.owners == 0 {
		return false, false, nil
	}
	q.writes = append(q.writes, write)
	if write.apply != nil {
		q.pendingOps++
	}
	return true, false, nil
}

// orderWithBuild runs an administrative operation that changes what a write
// applies to (a property-vector index removal, an indexing setting, the
// dimension mode) in order with the writes: while a build owns or awaits
// buildMu it is handed to the build behind the writes already acknowledged,
// so the final state equals that of the same calls made one after another;
// otherwise it runs now.
func (s *Service) orderWithBuild(op func()) {
	if queued, _, _ := s.queueBuildWrite(deferredIndexWrite{apply: op}); queued {
		return
	}
	op()
}

func (q *buildWriteQueue) opApplied() {
	q.mu.Lock()
	q.pendingOps--
	q.mu.Unlock()
}

// admitQueuedNode applies the outcome checks of indexNodeOwned that do not
// depend on the build's progress: the temporal-currency read and the
// dimension rule. A node with explicit embeddings on an index whose
// dimensions are still undecided (not fixed, no vectors, and the build has not
// chosen them from a stored vector yet) must wait for the build: in the order
// of a write that waited, the build's first stored vector chooses the
// dimensions before this write, so the write's outcome is not known until the
// build has decided them.
func (s *Service) admitQueuedNode(node *storage.Node) (wait bool, err error) {
	if !s.bm25Enabled.Load() && !s.vectorEnabled.Load() {
		return false, nil
	}
	shouldIndex, err := s.shouldIndexNode(node)
	if err != nil || !shouldIndex {
		return false, err
	}
	q := &s.buildWrites
	q.mu.Lock()
	adopted, pendingOps := q.dimsAdopted, q.pendingOps
	q.mu.Unlock()
	// A queued administrative operation (the dimension mode, the embedding
	// space, the index flags) may change the dimension rule before this write
	// applies, so its outcome is decided after it.
	if pendingOps > 0 && (len(node.NamedEmbeddings) > 0 || len(node.ChunkEmbeddings) > 0) {
		return true, nil
	}
	if !s.vectorEnabled.Load() || !s.hasExplicitEmbeddings(node) {
		return false, nil
	}
	if !s.vectorDimensionsFixed.Load() && s.EmbeddingCount() == 0 && !adopted {
		return true, nil
	}
	return false, s.checkEmbeddingDimensions(node)
}

// hasExplicitEmbeddings reports whether node carries an embedding the
// dimension rule (checkEmbeddingDimensions) applies to.
func (s *Service) hasExplicitEmbeddings(node *storage.Node) bool {
	for _, embedding := range node.NamedEmbeddings {
		if len(embedding) > 0 {
			return true
		}
	}
	if s.managedEmbeddingEligible(node) {
		for _, embedding := range node.ChunkEmbeddings {
			if len(embedding) > 0 {
				return true
			}
		}
	}
	return false
}

// adoptBuildVectorDimensions is the build's choice of the index dimensions
// from its first stored vector (adoptFirstVectorDimensions). It records that
// the dimensions are decided, so later writes are checked against them.
// Caller is the build.
func (s *Service) adoptBuildVectorDimensions(batch []*storage.Node) {
	s.adoptFirstVectorDimensions(batch)
	if s.EmbeddingCount() > 0 || s.vectorDimensionsFixed.Load() {
		return
	}
	for _, node := range batch {
		if firstVectorDimensions(node) > 0 {
			q := &s.buildWrites
			q.mu.Lock()
			q.dimsAdopted = true
			q.mu.Unlock()
			return
		}
	}
}

// readLockOrQueue admits a live write: it queues write on the build that owns
// or awaits buildMu and returns true, or takes buildMu's read lock and returns
// false (the caller releases it). A failed TryRLock with the queue closed
// means a short holder (ClearVectorIndex, or a build releasing buildMu), so
// waiting for it is short.
func (s *Service) readLockOrQueue(write deferredIndexWrite) (queued bool, err error) {
	for {
		queued, wait, err := s.queueBuildWrite(write)
		if queued || err != nil {
			return queued, err
		}
		if wait {
			s.buildMu.RLock()
			return false, nil
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
		case write.apply != nil:
			write.apply()
			s.buildWrites.opApplied()
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
