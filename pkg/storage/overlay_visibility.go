package storage

// The write-behind overlay serves acknowledged bodies until the flush lands
// them, so every read that merges it with storage must leave out what the
// visibility layer hides from that read, exactly as its storage path does.
// Scans use the adapters here; point reads pass a complete bufferedReadVisibility
// to entityStateLocked. TestOverlayBodiesAreReadThroughVisibility rejects raw
// body reads outside these owners. Each takes the read's visibility and applies
// filterEntityByDecay, the predicate the storage paths use. A hidden
// buffered body stays in the touched or
// suppressed set it comes with, so it masks its stored predecessor instead of
// revealing it. All of them run where the caller holds the buffer's read
// lock (withReadView's capture, captureReadView's cached callback).

// visibleBufferedEntities compacts either fresh overlay slice through the same
// visibility owner. Hidden bodies still mask their stored predecessor through
// the caller's untouched touched/suppressed sets.
func visibleBufferedEntities[E *Node | *Edge](b *BadgerEngine, v readVisibility, entities []E) []E {
	if v == storedRead || len(entities) == 0 || !b.visibilityFiltered() {
		return entities
	}
	now := DecayScoringTime()
	visible := entities[:0]
	for _, entity := range entities {
		if !b.filterEntityByDecay(v, entity, now) {
			visible = append(visible, entity)
		}
	}
	return visible
}

// bufferedReadVisibility is a read's stack-only policy. Omission means a
// physical buffered read; no policy is retained on the shared buffer.
type bufferedReadVisibility struct {
	engine     *BadgerEngine
	visibility readVisibility
}

// bufferedEntityRead selects one overlay index while retaining the read policy.
// An empty label/type still means that exact index key; only kind selects a
// whole-entity scan. Prefix is empty for the complete physical namespace set.
type bufferedEntityRead struct {
	kind                              bufferedEntityReadKind
	prefix, label, edgeType, property string
	valueKey                          any
	source, target                    NodeID
	outgoing                          bool
}
type bufferedEntityReadKind uint8

const (
	bufferedNodes bufferedEntityReadKind = iota
	bufferedLabel
	bufferedKey
	bufferedEdges
	bufferedType
	bufferedAdjacency
	bufferedLiveAdjacency
	bufferedBetween
)

type bufferedEntityResult struct {
	nodes                        []*Node
	edges                        []*Edge
	nodesTouched                 map[NodeID]bool
	edgesTouched                 map[EdgeID]bool
	suppressed                   map[EdgeID]struct{}
	sourceDeleted, targetDeleted bool
}

// readBufferedEntities is the sole indexed-overlay capture/filter owner. The
// caller holds w's read lock; touched IDs and deletion masks remain unfiltered
// so hidden buffered bodies cannot reveal their older committed predecessors.
func (b *BadgerEngine) readBufferedEntities(w *WriteBehindBuffer, v readVisibility, request bufferedEntityRead) (result bufferedEntityResult) {
	switch request.kind {
	case bufferedNodes:
		result.nodes, result.nodesTouched = w.allNodesOverlayLocked(request.prefix)
	case bufferedLabel:
		result.nodes, result.nodesTouched = w.labelOverlayLocked(request.label, request.prefix)
	case bufferedKey:
		result.nodes = w.keyCandidatesLocked(request.label, request.property, request.valueKey)
	case bufferedEdges:
		result.edges, result.edgesTouched = w.allEdgesOverlayLocked(request.prefix)
	case bufferedType:
		result.edges, result.edgesTouched = w.typeOverlayLocked(request.edgeType, request.prefix)
	case bufferedAdjacency:
		result.edges, result.suppressed, result.sourceDeleted = w.edgeAdjacencyOverlayLocked(request.source, request.outgoing)
	case bufferedLiveAdjacency:
		result.edges = w.liveAdjacencyLocked(request.source, request.outgoing)
	case bufferedBetween:
		result.edges, result.suppressed, result.sourceDeleted, result.targetDeleted = w.edgesBetweenOverlayLocked(request.source, request.target)
	}
	if request.kind <= bufferedKey {
		result.nodes = visibleBufferedEntities(b, v, result.nodes)
	} else {
		result.edges = visibleBufferedEntities(b, v, result.edges)
	}
	return result
}

// commitsSince visits the operations of each commit the buffer acknowledged
// after since and has not applied yet, newest first within a generation,
// until visit returns false.
func (w *WriteBehindBuffer) commitsSince(since MVCCVersion, visit func(MVCCVersion, []Operation) bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, buf := range append([]*CommitBuffer{w.active}, w.draining...) {
		for i := len(buf.commits) - 1; i >= 0; i-- {
			c := &buf.commits[i]
			if c.version.Compare(since) <= 0 {
				break
			}
			if !visit(c.version, c.ops) {
				return
			}
		}
	}
}

// readBufferedBeginPresence projects physical existence at an immutable BEGIN
// cut. It deliberately uses stored visibility: a hidden entity still existed
// for missing-write conflict classification. Only presence facts escape this
// composition owner, never buffered bodies or indexes.
func (presence *transactionBeginPresence) readBufferedBeginPresence(kind bufferedEntityKind, id string) (found, present bool, err error) {
	if presence == nil {
		return false, false, nil
	}
	presence.mu.RLock()
	defer presence.mu.RUnlock()
	if presence.released {
		return false, false, ErrStorageClosed
	}
	state := presence.active.entityState(kind, id, presence.commits)
	if !state.found {
		for _, generation := range presence.draining {
			state = generation.entityState(kind, id)
			if state.found {
				break
			}
		}
	}
	return state.found, state.found && !state.deleted, nil
}
