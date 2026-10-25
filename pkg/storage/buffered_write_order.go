package storage

// Entity ordering is the merged release's striped owner. Buffered admission
// and direct/synchronous writes share these stripes; callbacks own none.
import (
	"errors"
	"hash/maphash"
	"math/bits"
	"slices"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// writtenEntities are the entities one write touches, as the entity order
// compares them with the buffer: nodes it writes or that its relationships
// connect, nodes it deletes with their
// relationships, and relationships it writes or deletes.
type writtenEntities struct {
	nodes        []NodeID
	deletedNodes []NodeID
	edges        []EdgeID
}

func (e *writtenEntities) node(id NodeID) {
	if id != "" {
		e.nodes = append(e.nodes, id)
	}
}

func (e *writtenEntities) edge(edge *Edge) {
	if edge != nil {
		e.edges = append(e.edges, edge.ID)
		e.node(edge.StartNode)
		e.node(edge.EndNode)
	}
}

// affectedEntities is the single definition of a producer's complete write
// set. Stored reads include hidden and buffered relationships. OldEdge preserves
// the endpoints a move removes (NOR-169); cascades include every relationship
// and its far endpoint. Callers recheck this set under the entity order.
func (b *BadgerEngine) affectedEntities(seed writtenEntities, ops []Operation) (writtenEntities, error) {
	e := seed
	var relabelled []NodeID
	for i := range ops {
		op := &ops[i]
		e.node(op.NodeID)
		if op.EdgeID != "" {
			e.edges = append(e.edges, op.EdgeID)
		}
		if op.Type == OpDeleteNode {
			e.deletedNodes = append(e.deletedNodes, op.NodeID)
		}
		if op.Node != nil {
			e.node(op.Node.ID)
		}
		e.edge(op.Edge)
		e.edge(op.OldEdge)
		e.edges = append(e.edges, op.DeletedEdgeIDs...)
		if op.Type == OpUpdateNode && op.Node != nil {
			old := op.OldNode
			if old == nil {
				var err error
				old, err = b.GetStoredNode(op.Node.ID)
				if err != nil && !errors.Is(err, ErrNotFound) {
					return writtenEntities{}, err
				}
			}
			var labels []string
			if old != nil {
				labels = old.Labels
			}
			if !slices.Equal(labels, op.Node.Labels) {
				relabelled = append(relabelled, op.Node.ID)
			}
		}
	}
	seenNodes := make(map[NodeID]bool, len(e.deletedNodes))
	// Relabels also change incident relationships' positional count buckets.
	for _, id := range append(relabelled, e.deletedNodes...) {
		if seenNodes[id] {
			continue
		}
		seenNodes[id] = true
		out, in, err := b.readAdjacency(id, true, true, storedRead)
		if err != nil {
			return writtenEntities{}, err
		}
		for _, edges := range [2][]*Edge{out, in} {
			for _, edge := range edges {
				e.edge(edge)
			}
		}
	}
	seenEdges := make(map[EdgeID]bool, len(e.edges))
	for _, id := range e.edges {
		if id == "" || seenEdges[id] {
			continue
		}
		seenEdges[id] = true
		edge, err := b.GetStoredEdge(id)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return writtenEntities{}, err
		}
		if edge != nil {
			e.node(edge.StartNode)
			e.node(edge.EndNode)
		}
		// A buffered deletion hides its old body from logical reads, but
		// replay still removes that body's adjacency and endpoint counts.
		// Include the physical image too, before its generation is applied.
		if b.writeBehind != nil {
			err = b.withView(func(txn *badger.Txn) error {
				physical, _, err := b.readEdgeBodyInTxn(txn, id, storedRead)
				if physical != nil {
					e.node(physical.StartNode)
					e.node(physical.EndNode)
				}
				return err
			})
			if err != nil {
				return writtenEntities{}, err
			}
		}
	}
	return e, nil
}

// writeOrderStripes partitions the write-behind order by entity, so writes of
// different entities never wait for each other.
const writeOrderStripes = 1024

var writeOrderSeed = maphash.MakeSeed()

// orderStripeSet is a set of write-order stripes.
type orderStripeSet [writeOrderStripes / 64]uint64

func (s *orderStripeSet) add(id string) {
	if id != "" {
		i := maphash.String(writeOrderSeed, id) % writeOrderStripes
		s[i/64] |= 1 << (i % 64)
	}
}

func (s *orderStripeSet) entities(e writtenEntities) {
	for _, ids := range [...][]NodeID{e.nodes, e.deletedNodes} {
		for _, id := range ids {
			s.add(string(id))
		}
	}
	for _, id := range e.edges {
		s.add(string(id))
	}
}

// writeOrder is the write-behind order: per stripe, a commit the buffer
// acknowledges holds it shared while it enters the buffer, and a write that
// commits to Badger itself holds it exclusively until committed. Stripes are
// always taken in ascending order.
type writeOrder [writeOrderStripes]sync.RWMutex

func (o *writeOrder) lock(s *orderStripeSet, exclusive bool) {
	for word, bitsSet := range s {
		for bitsSet != 0 {
			i := word*64 + bits.TrailingZeros64(bitsSet)
			bitsSet &= bitsSet - 1
			if exclusive {
				o[i].Lock()
			} else {
				o[i].RLock()
			}
		}
	}
}

// lockMixed takes the stripes of s in ascending order, those also in
// exclusive exclusively and the others shared.
func (o *writeOrder) lockMixed(s, exclusive *orderStripeSet) {
	for word, bitsSet := range s {
		for bitsSet != 0 {
			bit := bitsSet & -bitsSet
			i := word*64 + bits.TrailingZeros64(bitsSet)
			bitsSet &= bitsSet - 1
			if exclusive[word]&bit != 0 {
				o[i].Lock()
			} else {
				o[i].RLock()
			}
		}
	}
}

func (o *writeOrder) unlockMixed(s, exclusive *orderStripeSet) {
	for word, bitsSet := range s {
		for bitsSet != 0 {
			bit := bitsSet & -bitsSet
			i := word*64 + bits.TrailingZeros64(bitsSet)
			bitsSet &= bitsSet - 1
			if exclusive[word]&bit != 0 {
				o[i].Unlock()
			} else {
				o[i].RUnlock()
			}
		}
	}
}

func (o *writeOrder) unlock(s *orderStripeSet, exclusive bool) {
	for word, bitsSet := range s {
		for bitsSet != 0 {
			i := word*64 + bits.TrailingZeros64(bitsSet)
			bitsSet &= bitsSet - 1
			if exclusive {
				o[i].Unlock()
			} else {
				o[i].RUnlock()
			}
		}
	}
}

// allOrderStripes is used only by lifecycle and value-ordered writes.
var allOrderStripes = func() (s orderStripeSet) {
	for i := range s {
		s[i] = ^uint64(0)
	}
	return s
}()

// orderAffectedEntities acquires only through the existing entity owner.
// Discovery can grow the set, so release and reacquire in sorted order rather
// than upgrading a partial hold. Recheck after acquisition and after landing.
// Replay takes this order too: a caller must release its stripes before waiting
// for a generation, then reacquire and recheck. No callback is consumed here.
func (b *BadgerEngine) orderAffectedEntities(seed writtenEntities, ops []Operation, mode entityOrderOptions) (func(), bool, error) {
	var stripes, exclusive orderStripeSet
	drained := false
	for {
		b.entityOrder.lockMixed(&stripes, &exclusive)
		release := func() { b.entityOrder.unlockMixed(&stripes, &exclusive) }
		e, err := b.affectedEntities(seed, ops)
		if err != nil {
			release()
			return nil, drained, err
		}
		var wanted, deletions orderStripeSet
		wanted.entities(e)
		if mode.allStripes {
			wanted = allOrderStripes
		}
		if mode.buffered {
			for _, id := range e.deletedNodes {
				deletions.add(string(id))
			}
		} else {
			deletions = wanted
		}
		grown := false
		for i := range stripes {
			grown = grown || wanted[i]&^stripes[i] != 0 || deletions[i]&^exclusive[i] != 0
		}
		if grown {
			release()
			for i := range stripes {
				stripes[i] |= wanted[i]
				exclusive[i] |= deletions[i]
			}
			continue
		}
		if w := b.writeBehind; w != nil && !mode.buffered && !mode.replay {
			w.mu.RLock()
			land := w.bufferedTouchesLocked(e.nodes, e.edges, false) || ((mode.allStripes || mode.landAll) && (w.opCount > 0 || len(w.draining) > 0))
			for _, id := range e.deletedNodes {
				land = land || w.bufferedTouchesLocked([]NodeID{id}, nil, true)
			}
			w.mu.RUnlock()
			if land {
				release()
				if err := w.flush(false); err != nil {
					return nil, drained, err
				}
				drained = true
				continue
			}
		}
		return release, drained, nil
	}
}

// MERGE key holders land the complete buffer without expanding their entity
// stripes. Lifecycle, value and schema ordering retain their all-stripe hold.
type entityOrderOptions struct {
	buffered, allStripes, replay, landAll bool
}

func (b *BadgerEngine) orderDirectEntities(nodes []NodeID, edges []EdgeID, incident, all bool, ops ...Operation) (func(), error) {
	e := writtenEntities{nodes: nodes, edges: edges}
	if incident {
		e.deletedNodes = nodes
	}
	release, _, err := b.orderAffectedEntities(e, ops, entityOrderOptions{allStripes: all})
	return release, err
}

func (tx *BadgerTransaction) enterWriteBehindCommitOrderLocked(buffered, landAll bool) error {
	w := tx.engine.writeBehind
	if len(tx.operations) == 0 && len(tx.pendingWrites) == 0 && len(tx.pendingDeletes) == 0 {
		return nil
	}
	mode := entityOrderOptions{buffered: buffered, allStripes: landAll || tx.valueOrderedLocked(), landAll: len(tx.mergeKeyReleases) > 0}
	release, drained, err := tx.engine.orderAffectedEntities(writtenEntities{}, tx.operations, mode)
	if err != nil {
		return err
	}
	tx.commitReleaseOrder = release
	if buffered {
		tx.bufferedRetireEpoch = w.retireEpoch.Load()
		return nil
	}
	if w != nil && (drained || (tx.bufferEpoch&1 == 1 && w.readEpoch.Load() != tx.bufferEpoch)) {
		return tx.rebaseBadgerAfterDrainLocked()
	}
	return nil
}
