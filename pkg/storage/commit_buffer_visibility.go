package storage

import (
	"context"
	"strings"
)

// Entity IDs are independently allocated: a node and an edge may have the
// same string ID. The legacy AppendDelete API deliberately hides both kinds;
// its mask lets a later write revive only its own kind.
type bufferedEntityKind uint8

const (
	bufferedNodeEntity bufferedEntityKind = 1 << iota
	bufferedEdgeEntity
)

type bufferedEntityState struct {
	version MVCCVersion

	node    *Node
	edge    *Edge
	found   bool
	deleted bool
}

// entityState is the single per-generation visibility owner. Typed commit
// tombstones and legacy generic deletes are resolved before the stored body.
func (b *CommitBuffer) entityState(kind bufferedEntityKind, id string) (state bufferedEntityState) {
	defer func() {
		if state.found {
			state.version = b.versions[bufferedEntityID{kind, id}]
		}
	}()

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

func (b *CommitBuffer) deletedEntityIDs(kind bufferedEntityKind, visit func(string) bool) {
	if kind == bufferedNodeEntity {
		for id := range b.deletedNodeIDs {
			if !visit(string(id)) {
				return
			}
		}
	} else {
		for id := range b.deletedEdgeIDs {
			if !visit(string(id)) {
				return
			}
		}
	}
	for id, mask := range b.deletes {
		if mask&kind != 0 {
			if !visit(id) {
				return
			}
		}
	}
}

// entityStateLocked resolves the first relevant state, newest generation
// first. All point, boolean and overlay readers use this same decision.
func (w *WriteBehindBuffer) entityStateLocked(kind bufferedEntityKind, id string, read ...bufferedReadVisibility) bufferedEntityState {
	if w == nil {
		return bufferedEntityState{}
	}
	state := w.active.entityState(kind, id)
	if !state.found {
		for i := len(w.draining) - 1; i >= 0; i-- {
			state = w.draining[i].entityState(kind, id)
			if state.found {
				break
			}
		}
	}
	if !state.found || state.deleted || len(read) == 0 || read[0].visibility == storedRead || !read[0].engine.visibilityFiltered() {
		return state
	}
	policy := read[0]
	now := DecayScoringTime()
	if (state.node != nil && policy.engine.filterEntityByDecay(policy.visibility, state.node, now)) || (state.edge != nil && policy.engine.filterEntityByDecay(policy.visibility, state.edge, now)) {
		return bufferedEntityState{found: true, deleted: true}
	}
	return state
}

// visitEntityOverlayLocked resolves each candidate only once. Enumeration
// remains label/type/adjacency scoped; pure deletions participate even when
// their generation has no body or corresponding index entry.
func (w *WriteBehindBuffer) visitEntityOverlayLocked(ctx context.Context, kind bufferedEntityKind, candidates func(*CommitBuffer, func(string) bool), visit func(string, bufferedEntityState), prefixes ...string) error {
	if w == nil {
		return ctx.Err()
	}
	seen := make(map[string]struct{})
	emit := func(id string) bool {
		if ctx.Err() != nil {
			return false
		}
		if len(prefixes) > 0 && !strings.HasPrefix(id, prefixes[0]) {
			return true
		}
		if _, found := seen[id]; found {
			return true
		}
		seen[id] = struct{}{}
		state := w.entityStateLocked(kind, id)
		if err := ctx.Err(); err != nil {
			return false
		}
		if state.found {
			visit(id, state)
		}
		return true
	}
	collect := func(buf *CommitBuffer) {
		candidates(buf, emit)
		if ctx.Err() == nil {
			buf.deletedEntityIDs(kind, emit)
		}
	}
	collect(w.active)
	for i := len(w.draining) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		collect(w.draining[i])
	}
	return ctx.Err()
}
