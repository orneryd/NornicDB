package search

import (
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// A live write never waits for an index build. BuildIndexes owns buildMu for
// the whole build, and its population and checkpoint restoration must not be
// overtaken by a live write (buildMu's contract). While a build owns buildMu,
// or waits to own it, every operation that changes what the search indexes
// hold is therefore prepared at the call and handed to the build, which
// installs it later in arrival order (the build sequence). The contract: the
// indexes after the build, and every outcome reported to a caller, equal those
// of the same calls made one after another once the build has finished. A
// write becomes visible when the build installs it; searches refuse while a
// build runs.
//
// Preparing at the call (queueBuildWrite) captures everything the outcome
// depends on that is not index state: a copy of the caller's data that shares
// no buffer with it, the temporal decision, and the checks the live path
// reports as errors. The dimension decision has one owner
// (vectorDimensionsUndecided): while it is still open, an operation carrying
// a vector waits for the build, because the build's first stored vector
// chooses the dimensions first in the serial order.
//
// Installing: node and edge writes are installed where the build already
// serializes its own work (before each node batch, between streamed edges),
// and a node or edge one of them set is never replaced by the build's older
// storage copy. An administrative operation (orderWithBuild: a property-vector
// index removal, an indexing setting, the dimension mode) changes what the
// build's own remaining batches would read, so it, and everything queued after
// it, is installed only once the build's population is complete. A build that
// finishes while another waits for buildMu leaves the rest of the sequence to
// that build, so nothing is installed between its final save and the next
// build's restore.
//
// A service with an index memory budget keeps waiting for buildMu, so a write
// still returns the budget's real answer.

// deferredIndexWrite is one operation prepared for the build sequence.
type deferredIndexWrite struct {
	node   *storage.Node // IndexNode
	nodeID string        // RemoveNode
	edge   *storage.Edge // IndexEdge
	edgeID string        // RemoveEdge
	apply  func()        // an administrative operation (orderWithBuild)
	// dims marks an administrative operation that can change the dimension
	// decision (orderDimensionChange).
	dims bool
	// current is the node's temporal currency, decided at the call.
	current bool
	// atEnd marks an administrative operation that changes the build's own
	// saves (the persistence toggle): it, and everything after it, is installed
	// only once the build has finished them (endBuildWrites).
	atEnd bool
	// relationship marks edge or edgeID as a relationship-search update
	// (IndexRelationship, RemoveRelationship). Its text and source depend on
	// settings ordered in this sequence, so it is ordered here too; it is not
	// an administrative operation, so writes after it are not held.
	relationship bool
}

type buildWriteQueue struct {
	open atomic.Bool // fast path: a build owns or awaits buildMu
	mu   sync.Mutex
	// owners counts builds that own or await buildMu. The queue stays open
	// until the last of them has installed it.
	owners       int
	writes       []deferredIndexWrite
	writtenNodes map[string]struct{}
	writtenEdges map[string]struct{}
	// dimsAdopted is set once the build has chosen the index dimensions from
	// a stored vector (adoptBuildVectorDimensions).
	dimsAdopted bool
	// populated is set once the build's population (node batches and the
	// relationship stream) is complete; from then on every queued operation
	// may be installed.
	populated bool
	// pendingOps counts queued dimension-changing operations not installed
	// yet.
	pendingOps int
}

// beginBuildWrites opens the queue for a build about to take buildMu.
func (s *Service) beginBuildWrites() {
	q := &s.buildWrites
	q.mu.Lock()
	q.owners++
	q.open.Store(true)
	q.mu.Unlock()
}

// startBuildWrites starts this build's part of the sequence once it owns
// buildMu: the IDs written and the dimension choice belong to the build that
// reads its own storage snapshot.
func (s *Service) startBuildWrites() {
	q := &s.buildWrites
	q.mu.Lock()
	q.writtenNodes, q.writtenEdges = nil, nil
	q.dimsAdopted = false
	q.populated = false
	q.mu.Unlock()
}

// endBuildWrites installs the rest of the sequence and, for the last build,
// closes it. A build that another one waits for leaves the rest to it. Caller
// owns buildMu and releases it next, so a write that finds the queue closed
// waits only for that release.
func (s *Service) endBuildWrites() {
	q := &s.buildWrites
	q.mu.Lock()
	q.owners--
	if q.owners > 0 {
		q.mu.Unlock()
		return
	}
	q.open.Store(false)
	writes := q.writes
	q.writes = nil
	q.mu.Unlock()
	if s.applyBuildWrites(writes) && !s.buildInProgress.Load() {
		s.schedulePersist()
	}
}

// completeBuildPopulation records that the build's population is complete and
// installs every queued operation, administrative ones included. Caller is the
// build, before it saves the base indexes.
func (s *Service) completeBuildPopulation() {
	q := &s.buildWrites
	q.mu.Lock()
	q.populated = true
	q.mu.Unlock()
	s.drainBuildWrites()
}

// queueBuildWrite prepares write for the build that owns or awaits buildMu
// and reports whether it queued it. Outside a build it costs one atomic load.
// An error from the checks the live path reports is returned and nothing is
// queued, as a write that waited would have failed; wait reports that the
// outcome is not known until the build has decided the dimensions.
func (s *Service) queueBuildWrite(write deferredIndexWrite) (queued, wait bool, err error) {
	q := &s.buildWrites
	if !q.open.Load() || !s.buildDefersWrites() {
		return false, false, nil
	}
	switch {
	case write.node != nil:
		current, wait, err := s.admitQueuedNode(write.node)
		if wait || err != nil {
			return false, wait, err
		}
		write.current = current
	case write.edge != nil && !write.relationship:
		if firstEdgeVectorDimensions(write.edge) > 0 && s.vectorDimensionsUndecided(true) {
			return false, true, nil
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
	if write.dims {
		q.pendingOps++
	}
	return true, false, nil
}

// orderWithBuild runs an administrative operation that changes what a write
// applies to (a property-vector index removal, an indexing setting, the
// dimension mode) in the build sequence: while a build owns or awaits buildMu
// it is installed behind the writes already acknowledged and after the
// build's population, so the final state equals that of the same calls made
// one after another; otherwise it runs now.
func (s *Service) orderWithBuild(op func()) {
	if queued, _, _ := s.queueBuildWrite(deferredIndexWrite{apply: op}); queued {
		return
	}
	op()
}

// orderDimensionChange is orderWithBuild for an operation that can change the
// dimension decision (the dimension mode, the embedding space, the index
// flags): until it is installed, a write carrying a vector waits for the
// build (vectorDimensionsUndecided).
func (s *Service) orderDimensionChange(op func()) {
	if queued, _, _ := s.queueBuildWrite(deferredIndexWrite{apply: op, dims: true}); queued {
		return
	}
	op()
}

// orderWithBuildEnd is orderWithBuild for an operation that changes the
// build's own saves (the persistence toggle): in the serial order it comes
// after them, so it is installed once the build has finished (endBuildWrites).
func (s *Service) orderWithBuildEnd(op func()) {
	if queued, _, _ := s.queueBuildWrite(deferredIndexWrite{apply: op, atEnd: true}); queued {
		return
	}
	op()
}

func (q *buildWriteQueue) opApplied() {
	q.mu.Lock()
	q.pendingOps--
	q.mu.Unlock()
}

// admitQueuedNode decides at the call what the live path would decide: the
// node's temporal currency (returned, and installed as decided), and the
// dimension rule for its explicit embeddings.
func (s *Service) admitQueuedNode(node *storage.Node) (current, wait bool, err error) {
	if !s.bm25Enabled.Load() && !s.vectorEnabled.Load() {
		return false, false, nil
	}
	current, err = s.shouldIndexNode(node)
	if err != nil || !current {
		return current, false, err
	}
	if firstVectorDimensions(node) == 0 || !s.vectorEnabled.Load() {
		return true, false, nil
	}
	if s.vectorDimensionsUndecided(true) {
		return true, true, nil
	}
	return true, false, s.checkEmbeddingDimensions(node)
}

// vectorDimensionsUndecided is the one owner of the question "may a vector
// written now still choose the index dimensions, or be judged by a choice not
// made yet?". It is true while the dimensions are not fixed, the index holds no
// vectors and the build has not chosen them from a stored vector, and, when
// pendingOps is set, while a queued administrative operation (the dimension
// mode, the embedding space, the index flags) is not installed yet.
func (s *Service) vectorDimensionsUndecided(pendingOps bool) bool {
	q := &s.buildWrites
	q.mu.Lock()
	adopted, pending := q.dimsAdopted, q.pendingOps
	q.mu.Unlock()
	if pendingOps && pending > 0 {
		return true
	}
	return !s.vectorDimensionsFixed.Load() && s.EmbeddingCount() == 0 && !adopted
}

// adoptBuildVectorDimensions is the build's choice of the index dimensions
// from its first stored vector, for node batches and streamed edges alike. It
// records that the dimensions are decided. Caller is the build.
func (s *Service) adoptBuildVectorDimensions(dims int) {
	if dims <= 0 {
		return
	}
	s.maybeAutoSetVectorDimensions(dims)
	q := &s.buildWrites
	q.mu.Lock()
	q.dimsAdopted = true
	q.mu.Unlock()
}

// adoptBuildBatchDimensions applies IndexNode's rule to a batch of a rebuild
// from storage: the first vector of an empty index whose dimension isn't fixed
// sets the dimension, whichever stored node carries it. A node without a
// vector (a document, a Qdrant collection's metadata node) never decides it.
func (s *Service) adoptBuildBatchDimensions(batch []*storage.Node) {
	for _, node := range batch {
		if dims := firstVectorDimensions(node); dims > 0 {
			s.adoptBuildVectorDimensions(dims)
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

// drainBuildWrites installs the operations queued so far. Before the build's
// population is complete it stops at the first administrative operation,
// which waits, with everything after it, for completeBuildPopulation; an
// atEnd operation waits for endBuildWrites. Caller owns buildMu (the build)
// and holds no index lock.
func (s *Service) drainBuildWrites() {
	q := &s.buildWrites
	if !q.open.Load() {
		return
	}
	q.mu.Lock()
	n := len(q.writes)
	for i, write := range q.writes {
		if write.apply != nil && (!q.populated || write.atEnd) {
			n = i
			break
		}
	}
	writes := q.writes[:n:n]
	q.writes = slices.Clone(q.writes[n:])
	q.mu.Unlock()
	s.applyBuildWrites(writes)
}

// applyBuildWrites installs operations in arrival order through the same
// bodies the live entry points run. An ID is recorded as written only once
// its write was installed: a write that failed leaves the build's storage copy
// in force, as a failed write that waited would have. Caller owns buildMu.
func (s *Service) applyBuildWrites(writes []deferredIndexWrite) bool {
	q := &s.buildWrites
	for _, write := range writes {
		var err error
		switch {
		case write.relationship:
			s.updateRelationshipNow(write)
		case write.node != nil:
			if err = s.indexNodeOwned(write.node, &write.current); err == nil {
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
			if write.dims {
				s.buildWrites.opApplied()
			}
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

// cloneQueuedValue copies every mutable container a property value can hold,
// whatever its type: the deferred apply reads property values in several
// places (BM25 text, property vectors, and the chunk-span fallback, which
// JSON-encodes arbitrary values). Common types take the fast path; any other
// slice, map, array, pointer, interface or struct is copied recursively.
// Scalars and strings are immutable and shared.
func cloneQueuedValue(value any) any {
	switch v := value.(type) {
	case nil, string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return value
	case []float32:
		return slices.Clone(v)
	case []float64:
		return slices.Clone(v)
	case []string:
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
		return cloneQueuedReflect(reflect.ValueOf(value)).Interface()
	}
}

// cloneQueuedReflect is cloneQueuedValue for any other type. A struct's
// unexported fields are copied by value, since they cannot be set.
func cloneQueuedReflect(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		copied := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		if queuedScalarKind(v.Type().Elem().Kind()) {
			reflect.Copy(copied, v)
			return copied
		}
		for i := range v.Len() {
			copied.Index(i).Set(cloneQueuedReflect(v.Index(i)))
		}
		return copied
	case reflect.Array:
		copied := reflect.New(v.Type()).Elem()
		copied.Set(v)
		if !queuedScalarKind(v.Type().Elem().Kind()) {
			for i := range v.Len() {
				copied.Index(i).Set(cloneQueuedReflect(v.Index(i)))
			}
		}
		return copied
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		copied := reflect.MakeMapWithSize(v.Type(), v.Len())
		for it := v.MapRange(); it.Next(); {
			copied.SetMapIndex(it.Key(), cloneQueuedReflect(it.Value()))
		}
		return copied
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		copied := reflect.New(v.Type().Elem())
		copied.Elem().Set(cloneQueuedReflect(v.Elem()))
		return copied
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		copied := reflect.New(v.Type()).Elem()
		copied.Set(cloneQueuedReflect(v.Elem()))
		return copied
	case reflect.Struct:
		copied := reflect.New(v.Type()).Elem()
		copied.Set(v)
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				copied.Field(i).Set(cloneQueuedReflect(v.Field(i)))
			}
		}
		return copied
	default:
		return v
	}
}

func queuedScalarKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
		return true
	}
	return false
}
