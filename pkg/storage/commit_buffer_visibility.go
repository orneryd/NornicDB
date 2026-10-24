package storage

// Entity IDs are independently allocated: a node and an edge may have the
// same string ID. The legacy AppendDelete API deliberately hides both kinds;
// its mask lets a later write revive only its own kind.
type bufferedEntityKind uint8

const (
	bufferedNodeEntity bufferedEntityKind = 1 << iota
	bufferedEdgeEntity
)

type bufferedEntityState struct {
	node    *Node
	edge    *Edge
	found   bool
	deleted bool
	// deletedAt is a typed tombstone's commit version; zero for generic deletes.
	deletedAt MVCCVersion
}

// entityState is the single per-generation visibility owner. Typed commit
// tombstones and legacy generic deletes are resolved before the stored body.
func (b *CommitBuffer) entityState(kind bufferedEntityKind, id string) bufferedEntityState {
	if b.deletes[id]&kind != 0 {
		return bufferedEntityState{found: true, deleted: true}
	}
	if kind == bufferedNodeEntity {
		if version, deleted := b.deletedNodeIDs[NodeID(id)]; deleted {
			return bufferedEntityState{found: true, deleted: true, deletedAt: version}
		}
		if node, found := b.nodes[NodeID(id)]; found {
			return bufferedEntityState{node: node, found: true}
		}
	} else {
		if version, deleted := b.deletedEdgeIDs[EdgeID(id)]; deleted {
			return bufferedEntityState{found: true, deleted: true, deletedAt: version}
		}
		if edge, found := b.edges[EdgeID(id)]; found {
			return bufferedEntityState{edge: edge, found: true}
		}
	}
	return bufferedEntityState{}
}

// entityVersion is entityState plus the ACK version of that state: the
// tombstone's for a delete, the body's for a live write. Zero means the state
// came from the legacy untyped append API, which carries no version.
func (b *CommitBuffer) entityVersion(kind bufferedEntityKind, id string) (bufferedEntityState, MVCCVersion) {
	state := b.entityState(kind, id)
	switch {
	case !state.found:
		return state, MVCCVersion{}
	case state.deleted:
		return state, state.deletedAt
	case kind == bufferedNodeEntity:
		return state, b.nodeVersions[NodeID(id)]
	default:
		return state, b.edgeVersions[EdgeID(id)]
	}
}

func (b *CommitBuffer) clearEntityDeletion(kind bufferedEntityKind, id string) {
	if remaining := b.deletes[id] &^ kind; remaining != 0 {
		b.deletes[id] = remaining
	} else {
		delete(b.deletes, id)
	}
	if kind == bufferedNodeEntity {
		delete(b.deletedNodeIDs, NodeID(id))
	} else {
		delete(b.deletedEdgeIDs, EdgeID(id))
	}
}

func (b *CommitBuffer) deletedEntityIDs(kind bufferedEntityKind, visit func(string)) {
	if kind == bufferedNodeEntity {
		for id := range b.deletedNodeIDs {
			visit(string(id))
		}
	} else {
		for id := range b.deletedEdgeIDs {
			visit(string(id))
		}
	}
	for id, mask := range b.deletes {
		if mask&kind != 0 {
			visit(id)
		}
	}
}

// entityStateLocked resolves the first relevant state, newest generation
// first. All point, boolean and overlay readers use this same decision.
func (w *WriteBehindBuffer) entityStateLocked(kind bufferedEntityKind, id string) bufferedEntityState {
	if w == nil {
		return bufferedEntityState{}
	}
	if state := w.active.entityState(kind, id); state.found {
		return state
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		buf := w.draining[i]
		if state := buf.entityState(kind, id); state.found {
			return state
		}
	}
	return bufferedEntityState{}
}

// entityVersionLocked is entityStateLocked with the resolved state's ACK
// version (CommitBuffer.entityVersion). Caller holds w.mu.
func (w *WriteBehindBuffer) entityVersionLocked(kind bufferedEntityKind, id string) (bufferedEntityState, MVCCVersion) {
	if state, version := w.active.entityVersion(kind, id); state.found {
		return state, version
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		if state, version := w.draining[i].entityVersion(kind, id); state.found {
			return state, version
		}
	}
	return bufferedEntityState{}, MVCCVersion{}
}

// visitEntityOverlayLocked resolves each candidate only once. Enumeration
// remains label/type/adjacency scoped; pure deletions participate even when
// their generation has no body or corresponding index entry.
func (w *WriteBehindBuffer) visitEntityOverlayLocked(kind bufferedEntityKind, candidates func(*CommitBuffer, func(string)), visit func(string, bufferedEntityState)) {
	if w == nil {
		return
	}
	seen := make(map[string]struct{})
	emit := func(id string) {
		if _, found := seen[id]; found {
			return
		}
		seen[id] = struct{}{}
		state := w.entityStateLocked(kind, id)
		if state.found {
			visit(id, state)
		}
	}
	collect := func(buf *CommitBuffer) {
		candidates(buf, emit)
		buf.deletedEntityIDs(kind, emit)
	}
	collect(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		collect(w.draining[i])
	}
}
