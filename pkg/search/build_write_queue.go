package search

import (
	"math"
	"reflect"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

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
// The baseline has one owner: the build reads it through one storage read
// snapshot (storage.ReadSnapshot), opened after the queue, so every write the
// queue holds comes after it. Nodes are installed from the snapshot (or from
// the restored files), then the saved graph, then the relationships streamed
// from the snapshot; only then are the queued operations installed, once, in
// order (completeBuildPopulation). Nothing from the baseline is installed
// after a queued write, so no write can be overtaken by an older copy. An
// engine without read snapshots gives the build no baseline to order writes
// against: there, writes wait for the build.
//
// Preparing at the call (queueBuildWrite) captures everything the outcome
// depends on that is not index state: a copy of the caller's data that shares
// no buffer with it, the temporal decision, and the checks the live path
// reports as errors. The dimension decision has one owner
// (vectorNeedsOrderedDecision): unless the dimensions are fixed, it depends on
// the index state everything ahead in the sequence leaves (the build's own
// stored vectors, a removal of the last vector), so an operation carrying a
// vector waits for the build and is decided in order by the live path. A
// synchronous caller therefore always gets the dimension error the serial
// order would give.
//
// Every retained copy is admitted against one memory reservation
// (queueFitsLocked): the headroom the runtime's soft memory limit leaves when
// the queue opens. A copy past it waits for the build instead. A build that
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
	// settings ordered in this sequence, so it is ordered here too.
	relationship bool
	// valid, when set, is checked when the write is installed, before it is
	// applied: false means a later write superseded it (a refresh whose
	// reservation is no longer current), so it is skipped. It is called with
	// buildMu owned and takes its own lock.
	valid func() bool
	// size is the payload the queue retains for the operation.
	size int
}

type buildWriteQueue struct {
	open atomic.Bool // fast path: a build owns or awaits buildMu
	// deferring is set while the queue's builds read through a storage
	// snapshot; without one writes wait for the build.
	deferring atomic.Bool
	mu        sync.Mutex
	// owners counts builds that own or await buildMu. The queue stays open
	// until the last of them has installed it.
	owners int
	writes []deferredIndexWrite
	// populated is set once the build's baseline is installed; from then on
	// every queued operation may be installed.
	populated bool
	// pendingOps counts queued operations that change a setting an entry
	// decision reads (the dimension mode, the embedding space, the index
	// flags) and are not installed yet.
	pendingOps int
	// bytes is the payload retained by the queued operations; headroom is
	// what the runtime's soft memory limit left when the queue opened (-1:
	// no limit is configured).
	bytes, headroom int
	// flags, when flagsQueued, are the index flags the queued SetIndexFlags
	// calls leave (bm25, vector).
	flags       [2]bool
	flagsQueued bool
}

// beginBuildWrites opens the queue for a build about to take buildMu.
// deferring reports that the build reads its baseline through a storage
// snapshot; otherwise writes wait for it.
func (s *Service) beginBuildWrites(deferring bool) {
	q := &s.buildWrites
	q.mu.Lock()
	if q.owners == 0 {
		q.headroom = memoryHeadroom()
		q.deferring.Store(deferring)
	}
	q.owners++
	q.open.Store(true)
	q.mu.Unlock()
}

// startBuildWrites starts this build's part of the sequence once it owns
// buildMu.
func (s *Service) startBuildWrites() {
	q := &s.buildWrites
	q.mu.Lock()
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
	q.deferring.Store(false)
	writes := q.takeLocked(len(q.writes))
	q.flagsQueued = false
	q.mu.Unlock()
	if s.applyBuildWrites(writes) && !s.buildInProgress.Load() {
		s.schedulePersist()
	}
}

// completeBuildPopulation records that the build's baseline is installed and
// installs every queued operation, administrative ones included, in order.
// Caller is the build, before it saves the base indexes.
func (s *Service) completeBuildPopulation() {
	q := &s.buildWrites
	q.mu.Lock()
	q.populated = true
	q.mu.Unlock()
	s.drainBuildWrites()
}

// queueBuildWrite prepares write for the build that owns or awaits buildMu
// and reports whether it queued it. Outside a build it costs one atomic load.
// An entry decision reads only inputs no queued operation ahead can change:
// the caller's data (copied), the temporal decision, and the settings while
// no setting change is queued. An error from the checks the live path
// reports is returned and nothing is queued, as a write that waited would
// have failed. wait reports that the outcome depends on the operations ahead
// (vectorNeedsOrderedDecision, a queued setting change) or that the copy does
// not fit the queue's memory reservation (queueFitsLocked): the write waits
// for the build and the live path decides it in order. Administrative
// operations carry no payload and are always queued.
func (s *Service) queueBuildWrite(write deferredIndexWrite) (queued, wait bool, err error) {
	q := &s.buildWrites
	if !q.open.Load() || !s.buildDefersWrites() {
		return false, false, nil
	}
	switch {
	case write.node != nil:
		current, wait, err := s.admitQueuedNode(write.node)
		if err != nil {
			return false, false, err
		}
		if wait {
			return false, true, nil
		}
		write.current = current
	case write.edge != nil && !write.relationship:
		// An edge's vectors are judged at install, by the dimensions and
		// flags of its turn; only automatic dimensions make that turn's
		// dimensions unknown at the call.
		if firstEdgeVectorDimensions(write.edge) > 0 && !s.vectorDimensionsFixed.Load() {
			return false, true, nil
		}
	}
	var nodeSize, edgeSize int
	write.node, nodeSize = copyQueuedNode(write.node)
	write.edge, edgeSize = copyQueuedEdge(write.edge)
	write.size = max(nodeSize+edgeSize, queuedEntryBytes)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.owners == 0 {
		return false, false, nil
	}
	if write.apply == nil && !q.fitsLocked(write.size) {
		return false, true, nil
	}
	q.writes = append(q.writes, write)
	q.bytes += write.size
	if write.dims {
		q.pendingOps++
	}
	return true, false, nil
}

// fitsLocked reports whether size more retained bytes fit the queue's
// reservation. Caller holds q.mu.
func (q *buildWriteQueue) fitsLocked(size int) bool {
	return q.headroom < 0 || q.bytes+size <= q.headroom
}

// memoryHeadroom is what the Go runtime's soft memory limit
// (memory.runtime_limit, GOMEMLIMIT), the process's configured memory budget,
// leaves above the live heap: the reservation every copy the queue retains is
// counted against, so the queue holds at most what the budget leaves and a
// copy past it waits for the build, as every write did before writes were
// handed to a build. -1 means no limit is configured: the queue is then not
// bounded beyond the process's memory, as the runtime is not.
func memoryHeadroom() int {
	limit := debug.SetMemoryLimit(-1)
	if limit == math.MaxInt64 {
		return -1
	}
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return -1
	}
	live := sample[0].Value.Uint64()
	if live >= uint64(limit) {
		return 0
	}
	return int(uint64(limit) - live)
}

// orderIndexFlags is SetIndexFlags in the build sequence. changed is decided
// against the flags the operations ahead leave, as in the serial order, in
// the same section that publishes this call's flags.
func (s *Service) orderIndexFlags(bm25Enabled, vectorEnabled bool) (changed bool) {
	set := func() {
		prevBM25 := s.bm25Enabled.Swap(bm25Enabled)
		prevVec := s.vectorEnabled.Swap(vectorEnabled)
		if prevBM25 != bm25Enabled || prevVec != vectorEnabled {
			s.indexChanged()
		}
	}
	next := [2]bool{bm25Enabled, vectorEnabled}
	q := &s.buildWrites
	if q.open.Load() && s.buildDefersWrites() {
		q.mu.Lock()
		if q.owners > 0 {
			previous := [2]bool{s.bm25Enabled.Load(), s.vectorEnabled.Load()}
			if q.flagsQueued {
				previous = q.flags
			}
			changed = previous != next
			q.flags, q.flagsQueued = next, true
			q.writes = append(q.writes, deferredIndexWrite{apply: set, dims: true, size: queuedEntryBytes})
			q.bytes += queuedEntryBytes
			q.pendingOps++
			q.mu.Unlock()
			return changed
		}
		q.mu.Unlock()
	}
	changed = s.bm25Enabled.Load() != bm25Enabled || s.vectorEnabled.Load() != vectorEnabled
	set()
	return changed
}

// orderWithBuild runs an administrative operation that changes what a write
// applies to (a property-vector index removal, an indexing setting, the
// dimension mode) in the build sequence: while a build owns or awaits buildMu
// it is installed behind the writes already acknowledged and after the
// build's baseline, so the final state equals that of the same calls made one
// after another; otherwise it runs now.
func (s *Service) orderWithBuild(op func()) {
	if queued, _, _ := s.queueBuildWrite(deferredIndexWrite{apply: op}); queued {
		return
	}
	op()
}

// orderDimensionChange is orderWithBuild for an operation that can change the
// dimension decision (the dimension mode, the embedding space, the index
// flags): until it is installed, a write carrying a vector waits for the
// build (vectorNeedsOrderedDecision).
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

// orderRelationshipUpdate runs update, a relationship-search update, in the
// build sequence: queued with write, or, when its copy does not fit the
// queue's reservation, after the build; otherwise now.
func (s *Service) orderRelationshipUpdate(write deferredIndexWrite, update func()) {
	queued, wait, _ := s.queueBuildWrite(write)
	if queued {
		return
	}
	if wait {
		s.buildMu.RLock()
		defer s.buildMu.RUnlock()
	}
	update()
}

func (q *buildWriteQueue) opApplied() {
	q.mu.Lock()
	q.pendingOps--
	q.mu.Unlock()
}

// admitQueuedNode decides at the call what the live path would decide, from
// inputs no queued operation ahead can change: the node's temporal currency
// (returned, and installed as decided) and the dimension rule for its
// explicit embeddings. The index flags are read at install, by the node's
// turn; at the call only while no setting change is queued. A node carrying
// a vector waits while its dimension decision depends on the operations
// ahead (vectorNeedsOrderedDecision).
func (s *Service) admitQueuedNode(node *storage.Node) (current, wait bool, err error) {
	pending := s.settingChangeQueued()
	if !pending && !s.bm25Enabled.Load() && !s.vectorEnabled.Load() {
		return false, false, nil
	}
	current, err = s.shouldIndexNode(node)
	if err != nil || !current {
		return current, false, err
	}
	if firstVectorDimensions(node) == 0 || (!pending && !s.vectorEnabled.Load()) {
		return true, false, nil
	}
	if s.vectorNeedsOrderedDecision() {
		return true, true, nil
	}
	return true, false, s.checkEmbeddingDimensions(node)
}

// vectorNeedsOrderedDecision is the one owner of the dimension decision for an
// operation carrying a vector. Unless the dimensions are fixed, whether the
// vector is accepted, and which dimensions it gives an empty index, depends on
// the index state left by everything ahead of it in the sequence: the build's
// own stored vectors (a forced rebuild clears the index first), a removal of
// the last vector, a setting change. That state exists only once those are
// installed, so the operation waits for the build and the live path decides
// it in order. With fixed dimensions the decision depends only on the vector
// and is made at the call, unless a setting change is queued.
func (s *Service) vectorNeedsOrderedDecision() bool {
	return !s.vectorDimensionsFixed.Load() || s.settingChangeQueued()
}

// settingChangeQueued reports a queued, not yet installed, change of a
// setting an entry decision reads (the dimension mode, the embedding space,
// the index flags).
func (s *Service) settingChangeQueued() bool {
	q := &s.buildWrites
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pendingOps > 0
}

// adoptBuildVectorDimensions is the build's choice of the index dimensions
// from a stored vector, for node batches and streamed edges alike. Caller is
// the build.
func (s *Service) adoptBuildVectorDimensions(dims int) {
	if dims <= 0 {
		return
	}
	s.maybeAutoSetVectorDimensions(dims)
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

// buildDefersWrites reports whether live writes are queued during a build:
// the build reads through a storage snapshot and no index memory budget is
// configured.
func (s *Service) buildDefersWrites() bool {
	return s.buildWrites.deferring.Load() && s.bm25MemoryMaxBytes <= 0 && s.vectorMemoryMaxBytes <= 0 && s.metadataMemoryMaxBytes <= 0
}

// drainBuildWrites installs the operations queued so far. Before the build's
// baseline is installed it installs nothing; an atEnd operation, and
// everything after it, waits for endBuildWrites. Caller owns buildMu (the
// build) and holds no index lock.
func (s *Service) drainBuildWrites() {
	q := &s.buildWrites
	if !q.open.Load() {
		return
	}
	q.mu.Lock()
	n := len(q.writes)
	if !q.populated {
		n = 0
	}
	for i, write := range q.writes[:n] {
		if write.atEnd {
			n = i
			break
		}
	}
	writes := q.takeLocked(n)
	q.mu.Unlock()
	s.applyBuildWrites(writes)
}

// takeLocked removes and returns the first n queued operations. Caller holds
// q.mu.
func (q *buildWriteQueue) takeLocked(n int) []deferredIndexWrite {
	if n == 0 {
		return nil
	}
	writes := q.writes[:n:n]
	q.writes = slices.Clone(q.writes[n:])
	for _, write := range writes {
		q.bytes -= write.size
	}
	return writes
}

// applyBuildWrites installs operations in arrival order through the same
// bodies the live entry points run. A write its valid hook reports superseded
// is skipped. Caller owns buildMu.
func (s *Service) applyBuildWrites(writes []deferredIndexWrite) bool {
	for _, write := range writes {
		if write.valid != nil && !write.valid() {
			continue
		}
		var err error
		switch {
		case write.relationship:
			s.updateRelationshipNow(write)
		case write.node != nil:
			err = s.indexNodeOwned(write.node, &write.current)
		case write.nodeID != "":
			s.removeNodeOwned(storage.NodeID(write.nodeID))
		case write.edge != nil:
			err = s.indexEdgeOwned(write.edge)
		case write.edgeID != "":
			s.removeEdgeOwned(storage.EdgeID(write.edgeID))
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

// copyQueuedNode makes a queued node independent of its caller, who may reuse
// any buffer once IndexNode returns: the build reads the queued copy later,
// as the live path read the caller's node before returning. Every value the
// index reads by reference is copied: labels, embedding vectors, and the
// slices and maps inside properties (property vectors) and embedding metadata.
// The struct copy keeps every other field. size is the payload the queue
// retains for it.
func copyQueuedNode(node *storage.Node) (copied *storage.Node, size int) {
	if node == nil {
		return nil, 0
	}
	size = queuedEntryBytes + int(unsafe.Sizeof(*node))
	clone := *node
	clone.Labels = slices.Clone(node.Labels)
	size += 16 * len(node.Labels)
	for _, label := range node.Labels {
		size += len(label)
	}
	clone.Properties = cloneQueuedMapSized(node.Properties, &size)
	clone.EmbedMeta = cloneQueuedMapSized(node.EmbedMeta, &size)
	if node.NamedEmbeddings != nil {
		clone.NamedEmbeddings = make(map[string][]float32, len(node.NamedEmbeddings))
		size += queuedMapBytes(len(node.NamedEmbeddings), 16+24)
		for name, embedding := range node.NamedEmbeddings {
			clone.NamedEmbeddings[name] = slices.Clone(embedding)
			size += len(name) + 4*len(embedding)
		}
	}
	if node.ChunkEmbeddings != nil {
		clone.ChunkEmbeddings = make([][]float32, len(node.ChunkEmbeddings))
		size += 24 * len(node.ChunkEmbeddings)
		for i, embedding := range node.ChunkEmbeddings {
			clone.ChunkEmbeddings[i] = slices.Clone(embedding)
			size += 4 * len(embedding)
		}
	}
	return &clone, size
}

func copyQueuedEdge(edge *storage.Edge) (copied *storage.Edge, size int) {
	if edge == nil {
		return nil, 0
	}
	size = queuedEntryBytes + int(unsafe.Sizeof(*edge))
	clone := *edge
	clone.Properties = cloneQueuedMapSized(edge.Properties, &size)
	return &clone, size
}

// queuedEntryBytes is what the queue counts for an entry besides what it
// copies: the entry itself, twice over for the slice's spare capacity.
const queuedEntryBytes = 2 * int(unsafe.Sizeof(deferredIndexWrite{}))

// queuedMapBytes is what a copied map of entries slots of slotBytes each
// takes on the heap: its header and its slot groups, allocated eight slots
// at a time (one control byte each) and filled to seven of eight. The keys'
// and values' own payload is counted apart.
func queuedMapBytes(entries, slotBytes int) int {
	const header = 48
	if entries == 0 {
		return header
	}
	slots := 8 * ((entries + 6) / 7)
	return header + slots*(slotBytes+1)
}

func cloneQueuedMap(source map[string]any) map[string]any {
	var size int
	return cloneQueuedMapSized(source, &size)
}

func cloneQueuedMapSized(source map[string]any, size *int) map[string]any {
	if source == nil {
		return nil
	}
	copied := make(map[string]any, len(source))
	*size += queuedMapBytes(len(source), 16+16)
	for key, value := range source {
		*size += len(key)
		copied[key] = cloneQueuedValue(value, size)
	}
	return copied
}

// cloneQueuedValue copies every mutable container a property value can hold,
// whatever its type: the deferred apply reads property values in several
// places (BM25 text, property vectors, and the chunk-span fallback, which
// JSON-encodes arbitrary values). Common types take the fast path; any other
// slice, map, array, pointer, interface or struct is copied recursively.
// Scalars and strings are immutable and shared. size accumulates the bytes
// the copy retains, strings included.
func cloneQueuedValue(value any, size *int) any {
	switch v := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		*size += 8
		return value
	case string:
		*size += 16 + len(v)
		return value
	case []float32:
		*size += 24 + 4*len(v)
		return slices.Clone(v)
	case []float64:
		*size += 24 + 8*len(v)
		return slices.Clone(v)
	case []string:
		*size += 24 + 16*len(v)
		for _, item := range v {
			*size += len(item)
		}
		return slices.Clone(v)
	case []any:
		*size += 24
		copied := make([]any, len(v))
		for i, item := range v {
			copied[i] = cloneQueuedValue(item, size)
		}
		return copied
	case map[string]any:
		return cloneQueuedMapSized(v, size)
	default:
		return cloneQueuedReflect(reflect.ValueOf(value), size).Interface()
	}
}

// cloneQueuedReflect is cloneQueuedValue for any other type. A struct's
// unexported fields are copied by value, since they cannot be set.
func cloneQueuedReflect(v reflect.Value, size *int) reflect.Value {
	switch v.Kind() {
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		*size += 24 + v.Len()*int(v.Type().Elem().Size())
		copied := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		if queuedScalarKind(v.Type().Elem().Kind()) {
			reflect.Copy(copied, v)
			if v.Type().Elem().Kind() == reflect.String {
				for i := range v.Len() {
					*size += v.Index(i).Len()
				}
			}
			return copied
		}
		for i := range v.Len() {
			copied.Index(i).Set(cloneQueuedReflect(v.Index(i), size))
		}
		return copied
	case reflect.Array:
		copied := reflect.New(v.Type()).Elem()
		copied.Set(v)
		*size += int(v.Type().Size())
		if !queuedScalarKind(v.Type().Elem().Kind()) {
			for i := range v.Len() {
				copied.Index(i).Set(cloneQueuedReflect(v.Index(i), size))
			}
		}
		return copied
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		copied := reflect.MakeMapWithSize(v.Type(), v.Len())
		*size += queuedMapBytes(v.Len(), int(v.Type().Key().Size()+v.Type().Elem().Size()))
		for it := v.MapRange(); it.Next(); {
			copied.SetMapIndex(it.Key(), cloneQueuedReflect(it.Value(), size))
		}
		return copied
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		copied := reflect.New(v.Type().Elem())
		*size += int(v.Type().Elem().Size())
		copied.Elem().Set(cloneQueuedReflect(v.Elem(), size))
		return copied
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		copied := reflect.New(v.Type()).Elem()
		copied.Set(cloneQueuedReflect(v.Elem(), size))
		return copied
	case reflect.Struct:
		copied := reflect.New(v.Type()).Elem()
		copied.Set(v)
		*size += int(v.Type().Size())
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				copied.Field(i).Set(cloneQueuedReflect(v.Field(i), size))
			}
		}
		return copied
	case reflect.String:
		*size += v.Len()
		return v
	default:
		*size += int(v.Type().Size())
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
