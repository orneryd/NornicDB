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
}

// entityState is the single per-generation visibility owner. Typed commit
// tombstones and legacy generic deletes are resolved before the stored body.
func (b *CommitBuffer) entityState(kind bufferedEntityKind, id string) bufferedEntityState {
	if b.deletes[id]&kind != 0 {
		return bufferedEntityState{found: true, deleted: true}
	}
	if kind == bufferedNodeEntity {
		if _, deleted := b.deletedNodeIDs[NodeID(id)]; deleted {
			return bufferedEntityState{found: true, deleted: true}
		}
		if node, found := b.nodes[NodeID(id)]; found {
			return bufferedEntityState{node: node, found: true}
		}
	} else {
		if _, deleted := b.deletedEdgeIDs[EdgeID(id)]; deleted {
			return bufferedEntityState{found: true, deleted: true}
		}
		if edge, found := b.edges[EdgeID(id)]; found {
			return bufferedEntityState{edge: edge, found: true}
		}
	}
	return bufferedEntityState{}
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
	if state := w.active.entityState(kind, id); state.found {
		return state
	}
	for i := len(w.draining) - 1; i >= 0; i-- {
		buf := w.draining[i]
		if !buf.IsApplied() {
			if state := buf.entityState(kind, id); state.found {
				return state
			}
		}
	}
	return bufferedEntityState{}
}

// visitEntityOverlayLocked resolves each candidate only once. Enumeration
// remains label/type/adjacency scoped; pure deletions participate even when
// their generation has no body or corresponding index entry.
func (w *WriteBehindBuffer) visitEntityOverlayLocked(kind bufferedEntityKind, candidates func(*CommitBuffer, func(string)), visit func(string, bufferedEntityState)) {
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
		if !w.draining[i].IsApplied() {
			collect(w.draining[i])
		}
	}
}
