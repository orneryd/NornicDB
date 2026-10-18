package search

import (
	"reflect"
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
}

// populationDefersWrites reports whether live writes are queued during a
// population (no index memory budget is configured).
func (s *Service) populationDefersWrites() bool {
	return s.bm25MemoryMaxBytes <= 0 && s.vectorMemoryMaxBytes <= 0 && s.metadataMemoryMaxBytes <= 0
}

// queueOnPopulation prepares write for the running population and reports
// whether it queued it. It takes only baseBuildControlMu, never indexMu, so a
// write never waits for anything the population holds indexMu for. Outside a
// population it costs one atomic load. Preparing captures at the call what the
// outcome depends on beyond the index state: the temporal decision (an error
// from it is returned and nothing is queued), and a copy of the caller's data
// that shares no buffer with it. wait reports that the write carries a vector
// while the population has not decided the index dimensions, so it must wait
// for the population (the population's first stored vector chooses them first
// in the serial order).
func (s *Service) queueOnPopulation(write deferredIndexWrite) (queued, wait bool, err error) {
	if !s.populationRunning.Load() || !s.populationDefersWrites() {
		return false, false, nil
	}
	if s.lifecycleCtx != nil {
		if err := s.lifecycleCtx.Err(); err != nil {
			return false, false, err
		}
	}
	vector := false
	switch {
	case write.node != nil:
		current, err := s.shouldIndexNode(write.node)
		if err != nil {
			return false, false, err
		}
		write.current = current
		vector = current && firstVectorDimensions(write.node) > 0
	case write.edge != nil && !write.relationship:
		vector = firstEdgeVectorDimensions(write.edge) > 0
	}
	if vector && s.vectorEnabled.Load() && s.populationDimensionsUndecided() {
		return false, true, nil
	}
	write.node = copyQueuedNode(write.node)
	write.edge = copyQueuedEdge(write.edge)
	s.baseBuildControlMu.Lock()
	defer s.baseBuildControlMu.Unlock()
	job := s.nodeAdmission
	if job == nil || nodeAdmissionClosed(job) || s.baseBuildPaused > 0 {
		return false, false, nil
	}
	job.deferred = append(job.deferred, write)
	if write.dims {
		job.pendingOps++
	}
	job.queued.Store(true)
	return true, false, nil
}

// populationDimensionsUndecided is the one owner of the question "may a
// vector written now still be judged by a dimension choice not made yet?": true
// while the dimensions are not fixed, the index holds no vectors and the
// running population has not chosen them from a stored vector, or while a
// queued administrative operation (the dimension mode, the embedding space,
// the index flags) is not applied yet.
func (s *Service) populationDimensionsUndecided() bool {
	s.baseBuildControlMu.Lock()
	job := s.nodeAdmission
	adopted, pending := false, 0
	if job != nil {
		adopted, pending = job.dimsAdopted, job.pendingOps
	}
	s.baseBuildControlMu.Unlock()
	if pending > 0 {
		return true
	}
	return !s.vectorDimensionsFixed.Load() && s.EmbeddingCount() == 0 && !adopted
}

// adoptPopulationDimensions is the population's choice of the index
// dimensions from a stored vector, for node batches and streamed edges alike,
// and records that they are decided. Caller is the population.
func (s *Service) adoptPopulationDimensions(job *nodePopulationAdmission, dims int) {
	if dims <= 0 {
		return
	}
	s.maybeAutoSetVectorDimensions(dims)
	s.baseBuildControlMu.Lock()
	job.dimsAdopted = true
	s.baseBuildControlMu.Unlock()
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
// population (populationDimensionsUndecided).
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
	writes := job.deferred[:n:n]
	job.deferred = slices.Clone(job.deferred[n:])
	job.queued.Store(len(job.deferred) > 0)
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

// applyDeferredIndexWrites applies the queued writes. Only the population
// calls it, between its steps, holding no index lock. A first vector that
// fixes the index dimensions is admitted outside indexMu first, as the live
// boundary does.
func (s *Service) applyDeferredIndexWrites(job *nodePopulationAdmission) {
	if job == nil || !job.queued.Load() {
		return
	}
	s.baseBuildControlMu.Lock()
	writes := job.takeDeferredLocked(false)
	s.baseBuildControlMu.Unlock()
	for _, write := range writes {
		if write.node != nil {
			s.maybeAutoSetVectorDimensions(firstVectorDimensions(write.node))
		} else if write.edge != nil && !write.relationship {
			s.maybeAutoSetVectorDimensions(firstEdgeVectorDimensions(write.edge))
		}
	}
	s.indexMu.Lock()
	ops := s.applyDeferredIndexWritesLocked(job, writes)
	s.indexMu.Unlock()
	s.baseBuildControlMu.Lock()
	job.pendingOps -= ops
	s.baseBuildControlMu.Unlock()
}

// applyDeferredIndexWritesLocked applies queued writes in arrival order. An ID
// is recorded as written only once its write applied: a write that failed
// leaves the population's storage copy in force, as a failed write that
// waited would have. Caller holds indexMu and is the population.
func (s *Service) applyDeferredIndexWritesLocked(job *nodePopulationAdmission, writes []deferredIndexWrite) (ops int) {
	bm25On := s.bm25Enabled.Load()
	for _, write := range writes {
		var err error
		switch {
		case write.relationship:
			s.updateRelationshipNow(write)
		case write.node != nil:
			if err = s.admitNodeWriteLocked(true); err == nil {
				err = s.indexNodeLockedAs(write.node, !bm25On, write.current)
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
