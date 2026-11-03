package storage

import (
	"context"
	"github.com/dgraph-io/badger/v4"
)

// writeMVCCHeadInTxn writes one entity kind's head, carrying an existing
// FloorVersion forward via a fresh read transaction so the lookup stays out
// of the user txn's SSI read set (see the docs/plans consumer-pinned-error
// contract rationale). The type arguments select the node or edge key
// lookups; the node and edge head writers are one implementation.
func writeMVCCHeadInTxn[KL mvccHeadKeyLookup, KW mvccHeadKeyWriter](b *BadgerEngine, txn *badger.Txn, id string, version MVCCVersion, tombstoned bool) error {
	floorVersion := version
	if existing, err := loadMVCCHead[KL](b, id); err == nil {
		floorVersion = existing.FloorVersion
	} else if err != ErrNotFound {
		return err
	}
	return writeMVCCHeadWithFloorInTxn[KW](b, txn, id, version, tombstoned, floorVersion)
}

// archiveNodeBodyInTxn writes a known node body to mvccNodeVersionKey(id,
// atVersion). Used by the delete path where the caller already has the old
// body in memory — avoids re-reading the primary key.
func (b *BadgerEngine) archiveNodeBodyInTxn(txn kvWriter, id NodeID, body *Node, atVersion MVCCVersion) error {
	if body == nil {
		return nil
	}
	return archiveMVCCBodyInTxn[*Node, nodeMVCCVersionKeyer](b, txn, string(id), body, atVersion, encodeMVCCNodeRecord)
}

// archiveEdgeBodyInTxn is the edge analogue of archiveNodeBodyInTxn.
func (b *BadgerEngine) archiveEdgeBodyInTxn(txn kvWriter, id EdgeID, body *Edge, atVersion MVCCVersion) error {
	if body == nil {
		return nil
	}
	return archiveMVCCBodyInTxn[*Edge, edgeMVCCVersionKeyer](b, txn, string(id), body, atVersion, encodeMVCCEdgeRecord)
}

// archiveMVCCBodyInTxn archives one entity kind's superseded body at
// atVersion, unless history is not needed or the version is already
// archived (staged writes included). The node and edge archivers are this
// one implementation.
func archiveMVCCBodyInTxn[R any, K mvccVersionKeyer](b *BadgerEngine, txn kvWriter, id string, body R, atVersion MVCCVersion, encode func(R, bool) ([]byte, error)) error {
	if !b.mustArchiveForHistory() {
		return nil
	}
	var keyer K
	if existing := keyer.versionKeyLookup(b, id, atVersion); existing != nil {
		if has, err := kvHas(txn, existing); err != nil || has {
			return err
		}
	}
	return writeMVCCVersionInTxn[R, K](b, txn, id, body, false, atVersion, encode)
}

// materializeMVCCCommit writes MVCC head metadata for every committed
// operation. Post-refactor invariant: primary keys (nodeKey/edgeKey) hold
// the current head body; historical bodies live only at mvccVersionKey for
// the version they were superseded at. This function stores only heads on
// the create path, and archives op.OldNode / op.OldEdge into a version
// record on update/delete (gated on retentionRetainsHistory). No-op arcs
// vanish when the engine runs head-only retention — the common case.
//
// Each entity's writes are one unit written through w, so a commit larger
// than one Badger batch moves to the next batch between units (#703). A
// node deletion's relationships are units of their own, so deleting a node
// with any number of relationships fits. A unit repeated in a new batch
// after it did not fit writes the same keys and values again.
func (b *BadgerEngine) materializeMVCCCommit(ctx context.Context, w *batchWriter, version MVCCVersion, operations []Operation) error {
	return b.materializeMVCCCommitWithHeads(ctx, w, version, operations, false)
}

func (b *BadgerEngine) materializeMVCCCommitWithHeads(ctx context.Context, w *batchWriter, version MVCCVersion, operations []Operation, staged bool) error {
	heads := commitMVCCHeads{b: b, staged: staged}
	states, err := newCommitStates(ctx, operations)
	if err != nil {
		return err
	}
	// Bodies of edges created in this commit are already known. Reading their
	// nonexistent committed body during a cascade opens a Badger history
	// iterator per edge and repeatedly sorts the growing pending-write set.
	// Keep the operation-ordered body until deletion; it has no committed
	// predecessor to archive. Existing/replaced edges retain the normal reader.
	var createdEdgeBodies map[EdgeID]*Edge
	hasCascade := false
	for _, op := range operations {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(op.DeletedEdgeIDs) > 0 {
			hasCascade = true
			break
		}
	}
	// Archive superseded bodies when retention policy demands history OR
	// when an active snapshot reader needs to resolve the old version —
	// snapshot isolation must hold regardless of retention config.
	retainsHistory := b.mustArchiveForHistory()
	if staged && retainsHistory {
		heads.nodes = make(map[NodeID]commitMVCCHeadResult, len(states.nodes))
		heads.edges = make(map[EdgeID]commitMVCCHeadResult, len(states.edges))
		// All operations in this statement archive the same predecessor, even
		// when the statement updates the same entity repeatedly.
		for id := range states.nodes {
			if _, err := heads.node(w.txn, id); err != nil && err != ErrNotFound {
				return err
			}
		}
		// The fold carries a node delete's cascaded relationships too, so every
		// edge this statement archives has its committed head read here once.
		for id := range states.edges {
			if _, err := heads.edge(w.txn, id); err != nil && err != ErrNotFound {
				return err
			}
		}
	}
	for i := range operations {
		op := &operations[i]
		var err error
		switch op.Type {
		case OpCreateNode:
			if op.Node == nil {
				continue
			}
			err = w.write(ctx, func(txn *badger.Txn) error {
				// FreshID is set only when the ID was asserted new at
				// CreateNode time (UUID-shape). For explicit user-supplied
				// IDs we fall back to the load-existing-floor path to keep
				// snapshot reads correct across tombstone → recreate cycles.
				if op.FreshID {
					return b.writeNodeMVCCHeadForFreshCreateInTxn(txn, op.Node.ID, version)
				}
				return heads.writeNode(txn, op.Node.ID, version, false)
			})
		case OpUpdateNode:
			if op.Node == nil {
				continue
			}
			err = w.write(ctx, func(txn *badger.Txn) error {
				var err error
				if old := states.nodes[op.Node.ID].before; retainsHistory && old != nil {
					err = heads.archiveNodeUpdate(txn, op.Node.ID, old, states.nodes[op.Node.ID].after, version)
				}
				if err == nil {
					err = heads.writeNode(txn, op.Node.ID, version, false)
				}
				return err
			})
		case OpDeleteNode:
			err = w.write(ctx, func(txn *badger.Txn) error {
				var err error
				if old := states.nodes[op.NodeID].before; retainsHistory && old != nil {
					err = heads.archiveNode(txn, op.NodeID, old)
				}
				// Tombstone marker (tiny, no body) preserves delete semantics.
				if err == nil {
					err = b.writeNodeMVCCTombstoneInTxn(txn, op.NodeID, version)
				}
				if err == nil {
					err = heads.writeNode(txn, op.NodeID, version, true)
				}
				return err
			})
			for _, edgeID := range op.DeletedEdgeIDs {
				if err != nil {
					break
				}
				err = w.write(ctx, func(txn *badger.Txn) error {
					edge, fresh := createdEdgeBodies[edgeID]
					var err error
					if !fresh {
						edge, err = b.loadEdgeForAdjacencyTombstoneInTxn(txn, edgeID)
					}
					if err == nil {
						err = b.writeEdgeAdjacencyDeltaInTxn(txn, edge, nil, version)
					}
					// Only existing edges have a predecessor to archive. Their
					// stored records are unchanged until buffered writes flush.
					if err == nil && retainsHistory && !fresh {
						err = heads.archiveEdge(txn, edgeID, edge)
					} else if err == ErrNotFound {
						err = nil
					}
					if err == nil {
						err = b.writeEdgeMVCCTombstoneInTxn(txn, edgeID, version)
					}
					if err == nil {
						err = heads.writeEdge(txn, edgeID, version, true)
					}
					return err
				})
				delete(createdEdgeBodies, edgeID)
			}
		case OpCreateEdge:
			if op.Edge == nil {
				continue
			}
			if hasCascade && !op.ReplacesExisting {
				if createdEdgeBodies == nil {
					createdEdgeBodies = make(map[EdgeID]*Edge)
				}
				createdEdgeBodies[op.Edge.ID] = op.Edge
			}
			err = w.write(ctx, func(txn *badger.Txn) error {
				if err := b.writeEdgeAdjacencyDeltaInTxn(txn, nil, op.Edge, version); err != nil {
					return err
				}
				if op.FreshID {
					return b.writeEdgeMVCCHeadForFreshCreateInTxn(txn, op.Edge.ID, version)
				}
				return heads.writeEdge(txn, op.Edge.ID, version, false)
			})
		case OpUpdateEdge:
			if op.Edge == nil {
				continue
			}
			if _, fresh := createdEdgeBodies[op.Edge.ID]; fresh {
				createdEdgeBodies[op.Edge.ID] = op.Edge
			}
			err = w.write(ctx, func(txn *badger.Txn) error {
				var err error
				if op.OldEdge != nil && (op.OldEdge.StartNode != op.Edge.StartNode || op.OldEdge.EndNode != op.Edge.EndNode) {
					err = b.writeEdgeAdjacencyDeltaInTxn(txn, op.OldEdge, op.Edge, version)
				}
				if old := states.edges[op.Edge.ID].before; err == nil && retainsHistory && old != nil {
					err = heads.archiveEdgeUpdate(txn, op.Edge.ID, old, states.edges[op.Edge.ID].after, version)
				}
				if err == nil {
					err = heads.writeEdge(txn, op.Edge.ID, version, false)
				}
				return err
			})
		case OpDeleteEdge:
			delete(createdEdgeBodies, op.EdgeID)
			err = w.write(ctx, func(txn *badger.Txn) error {
				var err error
				if op.OldEdge != nil {
					err = b.writeEdgeAdjacencyDeltaInTxn(txn, op.OldEdge, nil, version)
				}
				if old := states.edges[op.EdgeID].before; err == nil && retainsHistory && old != nil {
					err = heads.archiveEdge(txn, op.EdgeID, old)
				}
				// Tombstone marker preserves delete semantics.
				if err == nil {
					err = b.writeEdgeMVCCTombstoneInTxn(txn, op.EdgeID, version)
				}
				if err == nil {
					err = heads.writeEdge(txn, op.EdgeID, version, true)
				}
				return err
			})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// archiveAtLiveHead archives an entity's superseded body, through archive,
// at the version its MVCC head records. A missing head (the entity is new)
// or a tombstoned one leaves nothing to archive. Every archive of a body an
// update or delete supersedes goes through it: the commit loop
// (materializeMVCCCommit) and a transaction's statement-time deletes.
func archiveAtLiveHead(head MVCCHead, headErr error, archive func(atVersion MVCCVersion) error) error {
	if headErr == ErrNotFound || (headErr == nil && head.Tombstoned) {
		return nil
	}
	if headErr != nil {
		return headErr
	}
	return archive(head.Version)
}

// archiveNodeAtCommittedHeadInTxn archives body, the node version an update
// or delete supersedes, at the node's committed head (archiveAtLiveHead).
// The head is read in a separate read transaction so the lookup stays out
// of the commit's conflict set; see writeNodeMVCCHeadInTxn.
func (b *BadgerEngine) archiveNodeAtCommittedHeadInTxn(txn kvWriter, id NodeID, body *Node) error {
	head, err := b.loadNodeMVCCHead(id)
	return archiveAtLiveHead(head, err, func(atVersion MVCCVersion) error {
		return b.archiveNodeBodyInTxn(txn, id, body, atVersion)
	})
}

// archiveNodeUpdateAtCommittedHeadInTxn archives old, the node version an
// update supersedes, at the node's committed head. next is the node's final
// state in the commit at nextVersion (nil when the commit deletes it); see
// archiveNodeUpdateBodyInTxn.
func (b *BadgerEngine) archiveNodeUpdateAtCommittedHeadInTxn(txn kvWriter, id NodeID, old, next *Node, nextVersion MVCCVersion) error {
	head, err := b.loadNodeMVCCHead(id)
	return archiveAtLiveHead(head, err, func(atVersion MVCCVersion) error {
		return b.archiveNodeUpdateBodyInTxn(txn, id, old, next, atVersion, nextVersion)
	})
}

// archiveEdgeUpdateAtCommittedHeadInTxn is the relationship analogue of
// archiveNodeUpdateAtCommittedHeadInTxn.
func (b *BadgerEngine) archiveEdgeUpdateAtCommittedHeadInTxn(txn kvWriter, id EdgeID, old, next *Edge, nextVersion MVCCVersion) error {
	head, err := b.loadEdgeMVCCHead(id)
	return archiveAtLiveHead(head, err, func(atVersion MVCCVersion) error {
		return b.archiveEdgeUpdateBodyInTxn(txn, id, old, next, atVersion, nextVersion)
	})
}

// commitEntityState keeps one entity's committed predecessor and final image
// together. A nil predecessor is creation (or an implicit cascade's bodyless
// deletion); a nil final image is deletion.
type commitEntityState[T any] struct {
	before, after T
}

// commitStates is shared by history, claims, indexes, pending views and cache
// publication, so every consumer agrees on the entity's publication unit.
type commitStates struct {
	nodes map[NodeID]commitEntityState[*Node]
	edges map[EdgeID]commitEntityState[*Edge]
}

// newCommitStates is the one before/final fold. Several acknowledged frames
// can share a physical publication boundary; consumers never select staged
// intermediate images independently. ctx is the frame's own lifetime: a
// cancelled caller selects no images and folds none.
func newCommitStates(ctx context.Context, operationFrames ...[]Operation) (commitStates, error) {
	var states commitStates
	var err error
	for _, operations := range operationFrames {
		for _, op := range operations {
			if err := ctx.Err(); err != nil {
				return commitStates{}, err
			}
			switch {
			case (op.Type == OpCreateNode || op.Type == OpUpdateNode) && op.Node != nil:
				states.nodes, err = foldCommitEntity(ctx, states.nodes, op.Node.ID, op.OldNode, op.Node)
			case op.Type == OpDeleteNode:
				states.nodes, err = foldCommitEntity(ctx, states.nodes, op.NodeID, op.OldNode, nil)
				for _, id := range op.DeletedEdgeIDs {
					if err == nil {
						states.edges, err = foldCommitEntity(ctx, states.edges, id, nil, nil)
					}
				}
			case (op.Type == OpCreateEdge || op.Type == OpUpdateEdge) && op.Edge != nil:
				states.edges, err = foldCommitEntity(ctx, states.edges, op.Edge.ID, op.OldEdge, op.Edge)
			case op.Type == OpDeleteEdge:
				states.edges, err = foldCommitEntity(ctx, states.edges, op.EdgeID, op.OldEdge, nil)
			}
			if err != nil {
				return commitStates{}, err
			}
		}
	}
	return states, nil
}

// foldCommitEntity folds one operation into its entity's pair: the first
// operation keeps the committed predecessor, the last keeps the final image.
// ctx is the operation's own lifetime, so a cancelled frame folds nothing.
func foldCommitEntity[K comparable, T any](ctx context.Context, states map[K]commitEntityState[T], id K, before, after T) (map[K]commitEntityState[T], error) {
	if err := ctx.Err(); err != nil {
		return states, err
	}
	if states == nil {
		states = make(map[K]commitEntityState[T])
	}
	pair, seen := states[id]
	if !seen {
		pair.before = before
	}
	pair.after = after
	states[id] = pair
	return states, nil
}

// archiveEdgeAtCommittedHeadInTxn is the edge analogue of
// archiveNodeAtCommittedHeadInTxn.
func (b *BadgerEngine) archiveEdgeAtCommittedHeadInTxn(txn kvWriter, id EdgeID, body *Edge) error {
	head, err := b.loadEdgeMVCCHead(id)
	return archiveAtLiveHead(head, err, func(atVersion MVCCVersion) error {
		return b.archiveEdgeBodyInTxn(txn, id, body, atVersion)
	})
}

// Buffered generations contain several acknowledged commits in one Badger
// transaction. Their history and floor must read the previous staged head, not
// the older externally published head. Sync commits keep the fresh-view reads
// that deliberately avoid expanding the user's SSI conflict set.
type commitMVCCHeadResult struct {
	head MVCCHead
	err  error
}
type commitMVCCHeads struct {
	b      *BadgerEngine
	staged bool
	nodes  map[NodeID]commitMVCCHeadResult
	edges  map[EdgeID]commitMVCCHeadResult
}

func (h commitMVCCHeads) node(txn *badger.Txn, id NodeID) (MVCCHead, error) {
	if h.staged {
		if found, ok := h.nodes[id]; ok {
			return found.head, found.err
		}
		head, err := h.b.loadNodeMVCCHeadInTxn(txn, id)
		if h.nodes != nil {
			h.nodes[id] = commitMVCCHeadResult{head, err}
		}
		return head, err
	}
	return h.b.loadNodeMVCCHead(id)
}
func (h commitMVCCHeads) edge(txn *badger.Txn, id EdgeID) (MVCCHead, error) {
	if h.staged {
		if found, ok := h.edges[id]; ok {
			return found.head, found.err
		}
		head, err := h.b.loadEdgeMVCCHeadInTxn(txn, id)
		if h.edges != nil {
			h.edges[id] = commitMVCCHeadResult{head, err}
		}
		return head, err
	}
	return h.b.loadEdgeMVCCHead(id)
}
func (h commitMVCCHeads) archiveNode(txn *badger.Txn, id NodeID, body *Node) error {
	head, err := h.node(txn, id)
	return archiveAtLiveHead(head, err, func(v MVCCVersion) error { return h.b.archiveNodeBodyInTxn(txn, id, body, v) })
}
func (h commitMVCCHeads) archiveEdge(txn *badger.Txn, id EdgeID, body *Edge) error {
	head, err := h.edge(txn, id)
	return archiveAtLiveHead(head, err, func(v MVCCVersion) error { return h.b.archiveEdgeBodyInTxn(txn, id, body, v) })
}
func (h commitMVCCHeads) archiveNodeUpdate(txn *badger.Txn, id NodeID, old, next *Node, version MVCCVersion) error {
	head, err := h.node(txn, id)
	return archiveAtLiveHead(head, err, func(v MVCCVersion) error { return h.b.archiveNodeUpdateBodyInTxn(txn, id, old, next, v, version) })
}
func (h commitMVCCHeads) archiveEdgeUpdate(txn *badger.Txn, id EdgeID, old, next *Edge, version MVCCVersion) error {
	head, err := h.edge(txn, id)
	return archiveAtLiveHead(head, err, func(v MVCCVersion) error { return h.b.archiveEdgeUpdateBodyInTxn(txn, id, old, next, v, version) })
}
func (h commitMVCCHeads) writeNode(txn *badger.Txn, id NodeID, version MVCCVersion, deleted bool) error {
	if !h.staged {
		return h.b.writeNodeMVCCHeadInTxn(txn, id, version, deleted)
	}
	head, err := h.node(txn, id)
	floor := version
	if err == nil {
		floor = head.FloorVersion
	} else if err != ErrNotFound {
		return err
	}
	return h.b.writeNodeMVCCHeadWithFloorInTxn(txn, id, version, deleted, floor)
}
func (h commitMVCCHeads) writeEdge(txn *badger.Txn, id EdgeID, version MVCCVersion, deleted bool) error {
	if !h.staged {
		return h.b.writeEdgeMVCCHeadInTxn(txn, id, version, deleted)
	}
	head, err := h.edge(txn, id)
	floor := version
	if err == nil {
		floor = head.FloorVersion
	} else if err != ErrNotFound {
		return err
	}
	return h.b.writeEdgeMVCCHeadWithFloorInTxn(txn, id, version, deleted, floor)
}

// writeNodeMVCCHeadForFreshCreateInTxn skips the pre-read that carries an
// existing FloorVersion forward. Callers must only invoke this for brand-new
// IDs where no prior head can exist (OpCreateNode in the commit loop). For a
// fresh entity the floor == the current version. Halves the Badger op count
// on create-heavy workloads (one Set instead of one Get + one Set per entity).
func (b *BadgerEngine) writeNodeMVCCHeadForFreshCreateInTxn(txn *badger.Txn, id NodeID, version MVCCVersion) error {
	return b.writeNodeMVCCHeadWithFloorInTxn(txn, id, version, false, version)
}
