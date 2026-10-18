package search

import (
	"math"
	"reflect"
	"runtime/debug"
	"runtime/metrics"
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

// deferredIndexWrite is one operation prepared for the population sequence.
type deferredIndexWrite struct {
	node   *storage.Node // IndexNode
	nodeID string        // RemoveNode
	edge   *storage.Edge // IndexEdge
	edgeID string        // RemoveEdge
	apply  func()        // an administrative operation (orderWithPopulation)
	// dims marks an administrative operation that can change the dimension
	// decision (orderDimensionChange).
	dims bool
	// current is the node's temporal currency, decided at the call.
	current bool
	// relationship marks edge or edgeID as a relationship-search update
	// (IndexRelationship, RemoveRelationship). Its text and source depend on
	// settings ordered in this sequence, so it is ordered here too; it is not
	// an administrative operation, so writes after it are not held.
	relationship bool
	// valid, when set, is checked when the write is applied, before it
	// changes anything: false means a later write superseded it (a refresh
	// whose reservation is no longer current), so it is skipped and not
	// recorded as written. It is called with indexMu held and takes its own
	// lock.
	valid func() bool
	// size is the payload the queue retains for the operation.
	size int
}

// populationDefersWrites reports whether live writes are queued during a
// population: the population reads through a storage snapshot and no index
// memory budget is configured.
func (s *Service) populationDefersWrites() bool {
	return s.populationDeferring.Load() && s.bm25MemoryMaxBytes <= 0 && s.vectorMemoryMaxBytes <= 0 && s.metadataMemoryMaxBytes <= 0
}

// queueOnPopulation prepares write for the running population and reports
// whether it queued it. It takes only baseBuildControlMu, never indexMu, so a
// write never waits for anything the population holds indexMu for. Outside a
// population it costs one atomic load. An entry decision reads only inputs no
// queued operation ahead can change: the caller's data (copied, sharing no
// buffer with it), the temporal decision, and the settings while no setting
// change is queued. An error from the checks the live path reports is
// returned and nothing is queued. wait reports that the outcome depends on
// the operations ahead (vectorNeedsOrderedDecision, a queued setting change)
// or that the copy does not fit the queue's memory reservation
// (fitsLocked): the write waits for the population and the live
// path decides it in order. Only callers that can wait in order
// (lockNodeMutation, orderRelationshipUpdate) are refused; administrative
// operations carry no payload and are always queued.
func (s *Service) queueOnPopulation(write deferredIndexWrite) (queued, wait bool, err error) {
	if !s.populationRunning.Load() || !s.populationDefersWrites() {
		return false, false, nil
	}
	if s.lifecycleCtx != nil {
		if err := s.lifecycleCtx.Err(); err != nil {
			return false, false, err
		}
	}
	switch {
	case write.node != nil:
		current, err := s.shouldIndexNode(write.node)
		if err != nil {
			return false, false, err
		}
		write.current = current
		// The flags are read at the call only while no setting change is
		// queued; otherwise the node's turn decides them.
		vectorOn := s.vectorEnabled.Load() || s.settingChangeQueued()
		if current && vectorOn && firstVectorDimensions(write.node) > 0 {
			if s.vectorNeedsOrderedDecision() {
				return false, true, nil
			}
			if err := s.checkEmbeddingDimensions(write.node); err != nil {
				return false, false, err
			}
		}
	case write.edge != nil && !write.relationship:
		// An edge's vectors are judged when it is applied, by the dimensions
		// and flags of its turn; only automatic dimensions make that turn's
		// dimensions unknown at the call.
		if firstEdgeVectorDimensions(write.edge) > 0 && !s.vectorDimensionsFixed.Load() {
			return false, true, nil
		}
	}
	var nodeSize, edgeSize int
	write.node, nodeSize = copyQueuedNode(write.node)
	write.edge, edgeSize = copyQueuedEdge(write.edge)
	write.size = max(nodeSize+edgeSize, queuedEntryBytes)
	s.baseBuildControlMu.Lock()
	defer s.baseBuildControlMu.Unlock()
	job := s.nodeAdmission
	if job == nil || nodeAdmissionClosed(job) || s.baseBuildPaused > 0 {
		return false, false, nil
	}
	if write.apply == nil && !job.fitsLocked(write.size) {
		return false, true, nil
	}
	job.deferred = append(job.deferred, write)
	job.bytes += write.size
	if write.dims {
		job.pendingOps++
	}
	job.queued.Store(true)
	return true, false, nil
}

// fitsLocked reports whether size more retained bytes fit the job's
// reservation. Caller holds baseBuildControlMu.
func (job *nodePopulationAdmission) fitsLocked(size int) bool {
	return job.headroom < 0 || job.bytes+size <= job.headroom
}

// memoryHeadroom is what the Go runtime's soft memory limit
// (memory.runtime_limit, GOMEMLIMIT), the process's configured memory budget,
// leaves above the live heap: the reservation every copy the queue retains is
// counted against, so the queue holds at most what the budget leaves and a
// copy past it waits for the population, as every write did before writes
// were queued on it. -1 means no limit is configured: the queue is then not
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

// orderIndexFlags is SetIndexFlags in the population sequence. changed is
// decided against the flags the operations ahead leave, as in the serial
// order, in the same section that publishes this call's flags.
func (s *Service) orderIndexFlags(bm25Enabled, vectorEnabled bool) (changed bool) {
	set := func() {
		prevBM25 := s.bm25Enabled.Swap(bm25Enabled)
		prevVec := s.vectorEnabled.Swap(vectorEnabled)
		if prevBM25 != bm25Enabled || prevVec != vectorEnabled {
			s.indexChanged()
		}
	}
	next := [2]bool{bm25Enabled, vectorEnabled}
	if s.populationRunning.Load() && s.populationDefersWrites() {
		s.baseBuildControlMu.Lock()
		job := s.nodeAdmission
		if job != nil && !nodeAdmissionClosed(job) && s.baseBuildPaused == 0 {
			previous := [2]bool{s.bm25Enabled.Load(), s.vectorEnabled.Load()}
			if job.flagsQueued {
				previous = job.flags
			}
			changed = previous != next
			job.flags, job.flagsQueued = next, true
			job.deferred = append(job.deferred, deferredIndexWrite{apply: set, dims: true, size: queuedEntryBytes})
			job.bytes += queuedEntryBytes
			job.pendingOps++
			job.queued.Store(true)
			s.baseBuildControlMu.Unlock()
			return changed
		}
		s.baseBuildControlMu.Unlock()
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	changed = s.bm25Enabled.Load() != bm25Enabled || s.vectorEnabled.Load() != vectorEnabled
	set()
	return changed
}

// orderRelationshipUpdate runs update, a relationship-search update, in the
// population sequence: queued with write, or, when its copy does not fit the
// queue's reservation, after the population; otherwise now.
func (s *Service) orderRelationshipUpdate(write deferredIndexWrite, update func()) {
	queued, wait, _ := s.queueOnPopulation(write)
	if queued {
		return
	}
	if wait {
		s.baseBuildControlMu.Lock()
		job := s.nodeAdmission
		s.baseBuildControlMu.Unlock()
		if job != nil {
			_ = s.awaitNodeAdmission(job)
		}
	}
	update()
}

// vectorNeedsOrderedDecision is the one owner of the dimension decision for an
// operation carrying a vector. Unless the dimensions are fixed, whether the
// vector is accepted, and which dimensions it gives an empty index, depends on
// the index state left by everything ahead of it in the sequence: the
// population's own stored vectors (a forced rebuild clears the index first),
// a removal of the last vector, a setting change. That state exists only once
// those are applied, so the operation waits for the population and the live
// path decides it in order (lockNodeMutation). With fixed dimensions the
// decision depends only on the vector and is made at the call, unless a
// setting change is queued.
func (s *Service) vectorNeedsOrderedDecision() bool {
	return !s.vectorDimensionsFixed.Load() || s.settingChangeQueued()
}

// settingChangeQueued reports a queued, not yet applied, change of a setting
// an entry decision reads (the dimension mode, the embedding space, the index
// flags).
func (s *Service) settingChangeQueued() bool {
	s.baseBuildControlMu.Lock()
	defer s.baseBuildControlMu.Unlock()
	return s.nodeAdmission != nil && s.nodeAdmission.pendingOps > 0
}

// adoptPopulationDimensions is the population's choice of the index
// dimensions from a stored vector, for node batches and streamed edges alike.
// Caller is the population.
func (s *Service) adoptPopulationDimensions(dims int) {
	if dims <= 0 {
		return
	}
	s.maybeAutoSetVectorDimensions(dims)
}

// completePopulationSequence records that the population (node batches and
// the relationship stream) is complete and applies every queued operation,
// administrative ones included. Caller is the population, before the base
// save.
func (s *Service) completePopulationSequence(job *nodePopulationAdmission) {
	s.baseBuildControlMu.Lock()
	job.populated = true
	s.baseBuildControlMu.Unlock()
	s.applyDeferredIndexWrites(job)
}

// orderWithPopulation runs an administrative operation that changes what a
// write applies to (a property-vector index removal, an indexing setting, the
// dimension mode) in the population sequence: while a population runs it is
// applied behind the writes already acknowledged and after the population's
// own batches and stream, so the final state equals that of the same calls
// made one after another; otherwise it runs now. op runs with indexMu held.
func (s *Service) orderWithPopulation(op func()) {
	s.orderWithPopulationAs(deferredIndexWrite{apply: op})
}

// orderDimensionChange is orderWithPopulation for an operation that can
// change the dimension decision (the dimension mode, the embedding space, the
// index flags): until it is applied, a write carrying a vector waits for the
// population (vectorNeedsOrderedDecision).
func (s *Service) orderDimensionChange(op func()) {
	s.orderWithPopulationAs(deferredIndexWrite{apply: op, dims: true})
}

func (s *Service) orderWithPopulationAs(write deferredIndexWrite) {
	if queued, _, _ := s.queueOnPopulation(write); queued {
		return
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	write.apply()
}

// takeDeferredLocked removes and returns the queued writes. Before the
// population is complete it stops at the first administrative operation,
// which waits, with everything after it, for completePopulationSequence;
// all takes everything (NODE publication). Caller holds baseBuildControlMu.
func (job *nodePopulationAdmission) takeDeferredLocked(all bool) []deferredIndexWrite {
	n := len(job.deferred)
	if !all && !job.populated {
		for i, write := range job.deferred {
			if write.apply != nil {
				n = i
				break
			}
		}
	}
	if n == 0 {
		return nil
	}
	writes := job.deferred[:n:n]
	job.deferred = slices.Clone(job.deferred[n:])
	for _, write := range writes {
		job.bytes -= write.size
	}
	if all {
		job.flagsQueued = false
	}
	job.queued.Store(len(job.deferred) > 0)
	return writes
}

// copyQueuedNode makes a queued node independent of its caller, who may reuse
// any buffer once IndexNode returns: the population reads the queued copy
// later, as the live path read the caller's node before returning. Every value the
// index reads by reference is copied: labels, embedding vectors, and the
// slices and maps inside properties (property vectors) and embedding metadata.
// The struct copy keeps every other field. size is the payload the queue
// retains for it (queuedBytes).
func copyQueuedNode(node *storage.Node) (copied *storage.Node, size int) {
	if node == nil {
		return nil, 0
	}
	size = queuedEntryBytes
	clone := *node
	clone.Labels = slices.Clone(node.Labels)
	for _, label := range node.Labels {
		size += len(label)
	}
	clone.Properties = cloneQueuedMapSized(node.Properties, &size)
	clone.EmbedMeta = cloneQueuedMapSized(node.EmbedMeta, &size)
	if node.NamedEmbeddings != nil {
		clone.NamedEmbeddings = make(map[string][]float32, len(node.NamedEmbeddings))
		for name, embedding := range node.NamedEmbeddings {
			clone.NamedEmbeddings[name] = slices.Clone(embedding)
			size += len(name) + 4*len(embedding)
		}
	}
	if node.ChunkEmbeddings != nil {
		clone.ChunkEmbeddings = make([][]float32, len(node.ChunkEmbeddings))
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
	size = queuedEntryBytes
	clone := *edge
	clone.Properties = cloneQueuedMapSized(edge.Properties, &size)
	return &clone, size
}

// queuedEntryBytes is what the queue counts for an operation besides its
// payload: the entry, the struct copy and the map headers, approximately.
const queuedEntryBytes = 256

func cloneQueuedMap(source map[string]any) map[string]any {
	var size int
	return cloneQueuedMapSized(source, &size)
}

func cloneQueuedMapSized(source map[string]any, size *int) map[string]any {
	if source == nil {
		return nil
	}
	copied := make(map[string]any, len(source))
	for key, value := range source {
		*size += len(key) + 16
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
		for it := v.MapRange(); it.Next(); {
			*size += int(it.Key().Type().Size()) + 16
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

// applyDeferredIndexWrites applies the queued writes. Only the population
// calls it, between its steps, holding no index lock. No queued write chooses
// the index dimensions: a write carrying a vector is queued only when they
// are fixed (vectorNeedsOrderedDecision).
func (s *Service) applyDeferredIndexWrites(job *nodePopulationAdmission) {
	if job == nil || !job.queued.Load() {
		return
	}
	s.baseBuildControlMu.Lock()
	writes := job.takeDeferredLocked(false)
	s.baseBuildControlMu.Unlock()
	s.indexMu.Lock()
	ops := s.applyDeferredIndexWritesLocked(job, writes)
	s.indexMu.Unlock()
	s.baseBuildControlMu.Lock()
	job.pendingOps -= ops
	s.baseBuildControlMu.Unlock()
}

// applyDeferredIndexWritesLocked applies queued writes in arrival order, after
// the population's baseline: nothing from the baseline is applied after them.
// A write its valid hook reports superseded is skipped. Each write reads the
// settings of its own turn (an earlier operation in the same batch may change
// them). Caller holds indexMu and is the population.
func (s *Service) applyDeferredIndexWritesLocked(job *nodePopulationAdmission, writes []deferredIndexWrite) (ops int) {
	for _, write := range writes {
		if write.valid != nil && !write.valid() {
			continue
		}
		var err error
		switch {
		case write.relationship:
			s.updateRelationshipNow(write)
		case write.node != nil && !s.bm25Enabled.Load() && !s.vectorEnabled.Load():
			// Both indexes off at its turn: a no-op, as IndexNode is.
		case write.node != nil:
			if err = s.admitNodeWriteLocked(true); err == nil {
				err = s.indexNodeAsLocked(write.node, !s.bm25Enabled.Load(), write.current)
			}
		case write.nodeID != "":
			if err = s.admitNodeWriteLocked(true); err == nil {
				s.removeNodeLocked(write.nodeID)
			}
		case write.edge != nil:
			if err = s.admitNodeWriteLocked(false); err == nil {
				err = s.indexEdgeLocked(write.edge)
			}
		case write.edgeID != "":
			if err = s.admitNodeWriteLocked(false); err == nil {
				s.removeEdgeLocked(write.edgeID)
			}
		case write.apply != nil:
			write.apply()
			if write.dims {
				ops++
			}
		}
		if err != nil {
			s.logPrintf("⚠️ Search: queued index write failed: %v", err)
		}
	}
	return ops
}
