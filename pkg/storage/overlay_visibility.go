package storage

import "context"

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
func visibleBufferedEntitiesWithContext[E *Node | *Edge](ctx context.Context, b *BadgerEngine, v readVisibility, entities []E) ([]E, error) {
	if v == storedRead || len(entities) == 0 || !b.visibilityFiltered() {
		return entities, ctx.Err()
	}
	now := DecayScoringTime()
	visible := entities[:0]
	for _, entity := range entities {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !b.filterEntityByDecay(v, entity, now) {
			visible = append(visible, entity)
		}
	}
	return visible, nil
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
func (b *BadgerEngine) readBufferedEntities(w *WriteBehindBuffer, v readVisibility, request bufferedEntityRead) bufferedEntityResult {
	result, _ := b.readBufferedEntitiesWithContext(context.Background(), w, v, request)
	return result
}

// readBufferedEntitiesWithContext captures the selected overlay index and applies
// the shared visibility predicate before returning. The caller holds w's read
// lock. Cancellation never yields partial candidates or changes deletion masks.
func (b *BadgerEngine) readBufferedEntitiesWithContext(ctx context.Context, w *WriteBehindBuffer, v readVisibility, request bufferedEntityRead) (result bufferedEntityResult, err error) {
	switch request.kind {
	case bufferedNodes:
		result.nodes, result.nodesTouched, err = w.allNodesOverlayLockedWithContext(ctx, request.prefix)
	case bufferedLabel:
		result.nodes, result.nodesTouched, err = w.labelOverlayLockedWithContext(ctx, request.label, request.prefix)
	case bufferedKey:
		result.nodes, err = w.keyCandidatesLockedWithContext(ctx, request.label, request.property, request.valueKey)
	case bufferedEdges:
		result.edges, result.edgesTouched, err = w.allEdgesOverlayLockedWithContext(ctx, request.prefix)
	case bufferedType:
		result.edges, result.edgesTouched, err = w.typeOverlayLockedWithContext(ctx, request.edgeType, request.prefix)
	case bufferedAdjacency:
		result.edges, result.suppressed, result.sourceDeleted, err = w.edgeAdjacencyOverlayLockedWithContext(ctx, request.source, request.outgoing)
	case bufferedLiveAdjacency:
		result.edges, err = w.liveAdjacencyLockedWithContext(ctx, request.source, request.outgoing)
	case bufferedBetween:
		result.edges, result.suppressed, result.sourceDeleted, result.targetDeleted, err = w.edgesBetweenOverlayLockedWithContext(ctx, request.source, request.target)
	}
	if err != nil {
		return bufferedEntityResult{}, err
	}
	if request.kind <= bufferedKey {
		result.nodes, err = visibleBufferedEntitiesWithContext(ctx, b, v, result.nodes)
	} else {
		result.edges, err = visibleBufferedEntitiesWithContext(ctx, b, v, result.edges)
	}
	if err != nil {
		return bufferedEntityResult{}, err
	}
	return result, nil
}

// commitsSince visits the operations of each commit the buffer acknowledged
// after since and has not applied yet, newest first within a generation,
// until visit returns false.
func (w *WriteBehindBuffer) commitsSince(ctx context.Context, since MVCCVersion, visit func(MVCCVersion, []Operation) (bool, error)) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, buf := range append([]*CommitBuffer{w.active}, w.draining...) {
		if err := ctx.Err(); err != nil {
			return err
		}
		for i := len(buf.commits) - 1; i >= 0; i-- {
			if err := ctx.Err(); err != nil {
				return err
			}
			c := &buf.commits[i]
			if c.version.Compare(since) <= 0 {
				break
			}
			more, err := visit(c.version, c.ops)
			if err != nil {
				return err
			}
			if !more {
				return nil
			}
		}
	}
	return nil
}
