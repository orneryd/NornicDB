package search

import (
	"maps"
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
// reports whether it did. Outside a build it costs one atomic load.
func (s *Service) queueBuildWrite(write deferredIndexWrite) bool {
	q := &s.buildWrites
	if !q.open.Load() || !s.buildDefersWrites() {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.owners == 0 {
		return false
	}
	write.node = copyQueuedNode(write.node)
	write.edge = copyQueuedEdge(write.edge)
	q.writes = append(q.writes, write)
	return true
}

// readLockOrQueue admits a live write: it queues write on the build that owns
// or awaits buildMu and returns true, or takes buildMu's read lock and returns
// false (the caller releases it). A failed TryRLock with the queue closed
// means a short holder (ClearVectorIndex, or a build releasing buildMu), so
// waiting for it is short.
func (s *Service) readLockOrQueue(write deferredIndexWrite) bool {
	for {
		if s.queueBuildWrite(write) {
			return true
		}
		if s.buildMu.TryRLock() {
			return false
		}
		if !s.buildWrites.open.Load() || !s.buildDefersWrites() {
			s.buildMu.RLock()
			return false
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
// live entry points run, and records each ID as written. Caller owns buildMu.
func (s *Service) applyBuildWrites(writes []deferredIndexWrite) bool {
	q := &s.buildWrites
	for _, write := range writes {
		var err error
		switch {
		case write.node != nil:
			q.markWritten(&q.writtenNodes, string(write.node.ID))
			err = s.indexNodeOwned(write.node)
		case write.nodeID != "":
			q.markWritten(&q.writtenNodes, write.nodeID)
			s.removeNodeOwned(storage.NodeID(write.nodeID))
		case write.edge != nil:
			q.markWritten(&q.writtenEdges, string(write.edge.ID))
			err = s.indexEdgeOwned(write.edge)
		case write.edgeID != "":
			q.markWritten(&q.writtenEdges, write.edgeID)
			s.removeEdgeOwned(storage.EdgeID(write.edgeID))
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
