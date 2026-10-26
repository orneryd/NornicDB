package storage

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math/bits"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// writtenEntities are the entities one write touches, as the write-behind
// order compares them with the buffer (orderAfterBufferedCommits): nodes it
// writes or that its relationships connect, nodes it deletes with their
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

// operationEntities are the entities a transaction's operations write.
func operationEntities(ops []Operation) writtenEntities {
	var e writtenEntities
	for i := range ops {
		op := &ops[i]
		switch op.Type {
		case OpDeleteNode:
			e.deletedNodes = append(e.deletedNodes, op.NodeID)
		case OpDeleteEdge:
			e.edges = append(e.edges, op.EdgeID)
		}
		if op.Node != nil {
			e.node(op.Node.ID)
		}
		e.edge(op.Edge)
		e.edge(op.OldEdge)
		e.edges = append(e.edges, op.DeletedEdgeIDs...)
	}
	return e
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

// operations adds the stripes of the entities ops write, as entities would
// for operationEntities(ops), without collecting them.
func (s *orderStripeSet) operations(ops []Operation) {
	for i := range ops {
		op := &ops[i]
		s.add(string(op.NodeID))
		s.add(string(op.EdgeID))
		if op.Node != nil && op.Node.ID != op.NodeID {
			s.add(string(op.Node.ID))
		}
		for _, edge := range [...]*Edge{op.Edge, op.OldEdge} {
			if edge != nil {
				if edge.ID != op.EdgeID {
					s.add(string(edge.ID))
				}
				s.add(string(edge.StartNode))
				s.add(string(edge.EndNode))
			}
		}
		for _, id := range op.DeletedEdgeIDs {
			s.add(string(id))
		}
	}
}

// deletions adds the stripes of the nodes ops delete.
func (s *orderStripeSet) deletions(ops []Operation) {
	for i := range ops {
		if ops[i].Type == OpDeleteNode {
			s.add(string(ops[i].NodeID))
		}
	}
}

// writeOrder is the write-behind order: per stripe, a commit the buffer
// acknowledges holds it shared while it enters the buffer, and a write that
// commits to Badger itself holds it exclusively until committed. Stripes are
// always taken in ascending order.
type writeOrder [writeOrderStripes]trackedRWMutex

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

// trackedRWMutex is a sync.RWMutex whose holds tests observe through
// lockHoldHooks: the write-order stripes and the commit gate.
type trackedRWMutex struct{ sync.RWMutex }

func (m *trackedRWMutex) Lock() {
	m.RWMutex.Lock()
	if lockHoldHooks != nil {
		lockHoldHooks.held()
	}
}

func (m *trackedRWMutex) Unlock() {
	if lockHoldHooks != nil {
		lockHoldHooks.released()
	}
	m.RWMutex.Unlock()
}

func (m *trackedRWMutex) RLock() {
	m.RWMutex.RLock()
	if lockHoldHooks != nil {
		lockHoldHooks.held()
	}
}

func (m *trackedRWMutex) RUnlock() {
	if lockHoldHooks != nil {
		lockHoldHooks.released()
	}
	m.RWMutex.RUnlock()
}

// lockHoldHooks, installed by tests, observe the write-order stripes and the
// commit gate each goroutine holds and every delivery to listeners, so that a
// listener running on a goroutine that holds one fails the tests. Nil in
// production.
var lockHoldHooks interface {
	held()
	released()
	delivering()
}

// deliveringToListeners reports a delivery to listeners to lockHoldHooks.
func deliveringToListeners() {
	if lockHoldHooks != nil {
		lockHoldHooks.delivering()
	}
}

// holdsEntities reports whether a generation not yet applied holds a body or
// deletion of one of e's entities, or a relationship of a node e deletes.
func (w *WriteBehindBuffer) holdsEntities(e writtenEntities) bool {
	// The read epoch is odd while any generation is unapplied. Under the
	// caller's exclusive stripes no commit of e's entities is appended in
	// the meantime, so an even epoch means none holds them.
	if w.readEpoch.Load()&1 == 0 {
		return false
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, id := range e.edges {
		if w.entityStateLocked(bufferedEdgeEntity, string(id)).found {
			return true
		}
	}
	for _, ids := range [...][]NodeID{e.nodes, e.deletedNodes} {
		for _, id := range ids {
			if w.entityStateLocked(bufferedNodeEntity, string(id)).found {
				return true
			}
		}
	}
	for _, id := range e.deletedNodes {
		for _, outgoing := range [...]bool{true, false} {
			if live, touched, _ := w.edgeAdjacencyOverlayLocked(id, outgoing); len(live) > 0 || len(touched) > 0 {
				return true
			}
		}
	}
	return false
}

// orderHold is the write-order stripes a write holds exclusively
// (orderAfterBufferedCommits), kept on the writer's stack.
type orderHold struct {
	writeRelease[bufferedOrderResources]
}

type bufferedOrderResources struct {
	w       *WriteBehindBuffer
	b       *BadgerEngine
	e       writtenEntities
	stripes orderStripeSet
}

// writeRelease owns terminal write teardown. Resource types retain their own
// idempotence mechanism: entity leases may release authority during publication,
// while reusable stack-held buffer order is armed by its non-nil buffer.
type writeRelease[T entityAuthorityResources | bufferedOrderResources] struct{ resources T }

func (lease *writeRelease[T]) release() {
	switch resource := any(&lease.resources).(type) {
	case *entityAuthorityResources:
		resource.once.Do(func() {
			resource.releaseKeys()
			resource.ordered.finish()
		})
	case *bufferedOrderResources:
		if resource.w != nil {
			resource.b.evictOrderedImages(resource.e)
			resource.w.order.unlock(&resource.stripes, true)
			resource.w = nil
		}
	}
}

// evictOrderedImages evicts the cached images of e's entities: their node and
// relationship bodies and the adjacency of each node (a relationship's
// endpoints are among e's nodes).
func (b *BadgerEngine) evictOrderedImages(e writtenEntities) {
	for _, ids := range [...][]NodeID{e.nodes, e.deletedNodes} {
		for _, id := range ids {
			b.cacheDeleteNode(id, nodeCacheAllForms)
			b.adjCacheInvalidate(id)
		}
	}
	for _, id := range e.edges {
		b.cacheDeleteEdge(id)
	}
}

// orderAfterBufferedCommits orders a write that commits to Badger itself (a
// direct engine write, a transaction commit the buffer does not take, a
// suppression rewrite) after every write-behind commit acknowledged before
// it, so that commit's replay can never land over it: h holds the stripes of
// e's entities exclusively until the caller's write has committed and
// reserved its notifications (h.release), so no commit of those entities is
// acknowledged in between, and a generation not yet applied that holds one
// of them is drained first, without delivering its notifications: the
// caller's publication point delivers them once the caller released every
// lock it took. A buffered relationship's commit holds its endpoints'
// stripes, so a node delete also waits for the relationships it cascades to.
// drained reports a drain: those commits landed after any Badger snapshot
// the caller opened before. Without write-behind there is nothing to order.
func (b *BadgerEngine) orderAfterBufferedCommits(h *orderHold, e writtenEntities) (drained bool, err error) {
	w := b.writeBehind
	if w == nil {
		return false, nil
	}
	h.resources.stripes.entities(e)
	h.resources.b, h.resources.e = b, e
	w.order.lock(&h.resources.stripes, true)
	if w.holdsEntities(e) {
		if err := w.flush(false, true); err != nil {
			w.order.unlock(&h.resources.stripes, true)
			return false, err
		}
		drained = true
	}
	h.resources.w = w
	return drained, nil
}

// orderCovering orders a write after the write-behind commits of e and of
// every node it reaches through stored state (reach, read under the order:
// a relationship's endpoints, the far endpoints of the relationships a node
// delete cascades to). The write reads or writes those nodes' bodies and
// adjacency, so a commit of one replayed while it runs would make it read a
// stale image or fail with a conflict. When reach finds a node the order
// does not hold, the order is taken again with it; the set only grows.
func (b *BadgerEngine) orderCovering(h *orderHold, e writtenEntities, reach func() ([]NodeID, error)) error {
	for {
		if _, err := b.orderAfterBufferedCommits(h, e); err != nil {
			return err
		}
		reached, err := reach()
		if err != nil {
			h.release()
			return err
		}
		grown := false
		for _, id := range reached {
			if id != "" && !slices.Contains(e.nodes, id) && !slices.Contains(e.deletedNodes, id) {
				e.nodes = append(e.nodes, id)
				grown = true
			}
		}
		if !grown {
			return nil
		}
		h.release()
	}
}

// storedEdgeEndpoints reaches the endpoints of edge id's stored image, the
// newest the write-behind buffer holds included; image, when set, receives
// the image read last (nil when there is none).
func (b *BadgerEngine) storedEdgeEndpoints(id EdgeID, image **Edge) func() ([]NodeID, error) {
	return func() ([]NodeID, error) {
		edge, err := b.GetStoredEdge(id)
		if errors.Is(err, ErrNotFound) {
			edge, err = nil, nil
		}
		if image != nil {
			*image = edge
		}
		if err != nil || edge == nil {
			return nil, err
		}
		return []NodeID{edge.StartNode, edge.EndNode}, nil
	}
}

// cascadeEndpoints reaches the endpoints of the relationships node id's
// stored adjacency holds: the ones a delete of it cascades to.
func (b *BadgerEngine) cascadeEndpoints(id NodeID) func() ([]NodeID, error) {
	return func() ([]NodeID, error) {
		var nodes []NodeID
		err := b.withView(func(txn *badger.Txn) error {
			for _, prefix := range [...][]byte{b.outgoingIndexPrefixString(id), b.incomingIndexPrefixString(id)} {
				if prefix == nil {
					continue
				}
				var edges []EdgeID
				it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					if num, ok := extractEdgeNumIDFromOutgoingKey(it.Item().Key()); ok {
						if edgeID, ok := b.idDict.lookupEdgeIDByNum(num); ok {
							edges = append(edges, edgeID)
						}
					}
				}
				it.Close()
				for _, edgeID := range edges {
					edge, err := b.loadEdgeForAdjacencyTombstoneInTxn(txn, edgeID)
					if errors.Is(err, ErrNotFound) {
						continue
					}
					if err != nil {
						return err
					}
					nodes = append(nodes, edge.StartNode, edge.EndNode)
				}
			}
			return nil
		})
		return nodes, err
	}
}

// allOrderStripes is every write-order stripe.
var allOrderStripes = func() (s orderStripeSet) {
	for i := range s {
		s[i] = ^uint64(0)
	}
	return s
}()

// lifecycleWrite is a write that replaces or removes stored state wholesale
// (beginLifecycleWrite), kept on the writer's stack.
type lifecycleWrite struct {
	b       *BadgerEngine
	held    *commitNotification
	dropped bool
	// suppressions: it holds suppressionWriteMu, so the end of a deleted
	// entity's stored suppression does not run across it.
	suppressions bool
}

// beginLifecycleWrite admits a write that replaces or removes stored state
// wholesale: a prefix delete or a restore. It holds the write barrier
// exclusively, so no write is admitted or acknowledged meanwhile, and every
// write-order stripe, so no ordered writer that takes no barrier runs
// either; and it applies every write-behind commit acknowledged before it, so
// none lands over it afterwards. dropped, when set, is the prefix the write
// deletes: a transaction open across it that writes there fails its commit
// (prefixDroppedSince). finish, deferred by the caller, releases everything
// and then delivers the notifications of the drained commits and of the
// write itself, after every lock.
func (b *BadgerEngine) beginLifecycleWrite(dropped string) (lifecycleWrite, error) {
	b.writeBarrier.Lock()
	if err := b.ensureOpen(true); err != nil {
		b.writeBarrier.Unlock()
		return lifecycleWrite{}, err
	}
	l := lifecycleWrite{b: b, held: b.queueCommitNotification(&commitNotification{})}
	if w := b.writeBehind; w != nil {
		w.order.lock(&allOrderStripes, true)
		if err := w.flush(false, false); err != nil {
			l.finish()
			return lifecycleWrite{}, err
		}
	}
	b.suppressionWriteMu.Lock()
	l.suppressions = true
	if dropped != "" {
		b.startPrefixDrop(dropped)
		l.dropped = true
	}
	return l, nil
}

// beginMaintenanceWrite admits a maintenance pass that rewrites stored or
// derived state wholesale (an index rebuild or backfill, a repair, the
// start-up migrations) as a lifecycle write (beginLifecycleWrite). scope
// names the databases the pass touches, nil the whole store. It is the
// pass's only admission: nothing the pass calls takes another.
//
// The pass rewrites state a transaction stages against (numeric IDs, index
// entries): a transaction open across it that writes in its scope fails its
// commit with a retryable conflict, as across a prefix delete
// (prefixDroppedSince).
func (b *BadgerEngine) beginMaintenanceWrite(scope []string) (lifecycleWrite, error) {
	l, err := b.beginLifecycleWrite("")
	if err != nil {
		return l, err
	}
	if len(scope) == 0 {
		if err := b.pendingNamespaceConflict(""); err != nil {
			l.finish()
			return lifecycleWrite{}, err
		}
	} else {
		for _, namespace := range scope {
			if err := b.pendingNamespaceConflict(namespace); err != nil {
				l.finish()
				return lifecycleWrite{}, err
			}
		}
	}
	rewritten := ""
	if len(scope) == 1 {
		rewritten = scope[0] + ":"
	}
	b.startPrefixDrop(rewritten)
	l.dropped = true
	return l, nil
}

func (l lifecycleWrite) finish() {
	if l.b == nil {
		return
	}
	if l.dropped {
		l.b.prefixDrops.seq.Add(1)
	}
	if l.suppressions {
		l.b.suppressionWriteMu.Unlock()
	}
	if w := l.b.writeBehind; w != nil {
		w.order.unlock(&allOrderStripes, true)
	}
	l.b.writeBarrier.Unlock()
	l.b.completeCommitNotification(l.held)
}

// prefixDrops records the prefix deletes (beginLifecycleWrite): per prefix,
// the lifecycle sequence its latest delete started at. lifecycleSeq advances
// when a delete starts and again when it ends, so it is odd while one runs.
type prefixDrops struct {
	pendingCount atomic.Int64
	pending      map[string]*namespaceDropIntent
	mu           sync.Mutex
	seq          atomic.Uint64
	start        map[string]uint64
}

// startPrefixDrop starts a delete of prefix; the caller holds the write
// barrier exclusively, and its finish ends the delete before releasing it.
func (b *BadgerEngine) startPrefixDrop(prefix string) {
	b.prefixDrops.mu.Lock()
	if b.prefixDrops.start == nil {
		b.prefixDrops.start = make(map[string]uint64)
	}
	b.prefixDrops.start[prefix] = b.prefixDrops.seq.Add(1)
	b.prefixDrops.mu.Unlock()
}

// prefixDroppedSince reports whether a delete of a prefix overlapping
// namespace, or a maintenance pass over it (beginMaintenanceWrite), started
// after a transaction began at lifecycle sequence begin, or was running then:
// the transaction staged its writes against state that delete removed or that
// pass rewrote (and encoded them with dictionaries it dropped or changed).
func (b *BadgerEngine) prefixDroppedSince(begin uint64, namespace string) bool {
	if namespace == "" || b.prefixDrops.seq.Load() == begin {
		return false
	}
	b.prefixDrops.mu.Lock()
	defer b.prefixDrops.mu.Unlock()
	ns := namespace + ":"
	for prefix, start := range b.prefixDrops.start {
		if start+(begin&1) > begin && (strings.HasPrefix(ns, prefix) || strings.HasPrefix(prefix, ns)) {
			return true
		}
	}
	return false
}

// bufferedDeleteConflictLocked fails a commit that writes an entity a
// commit acknowledged after tx began deleted, while the write-behind buffer
// still holds that delete: the stored head shows it only once it is applied.
// Committing over it would bring the entity back without counting it, or
// delete it, and count it, a second time.
func (tx *BadgerTransaction) bufferedDeleteConflictLocked() error {
	w := tx.engine.writeBehind
	if w == nil || len(tx.operations) == 0 || w.readEpoch.Load()&1 == 0 {
		return nil
	}
	var nodes map[NodeID]struct{}
	var edges map[EdgeID]struct{}
	written := func() {
		nodes, edges = make(map[NodeID]struct{}), make(map[EdgeID]struct{})
		for i := range tx.operations {
			op := &tx.operations[i]
			switch op.Type {
			case OpUpdateNode, OpDeleteNode, OpUpdateEmbedding:
				nodes[op.NodeID] = struct{}{}
				for _, id := range op.DeletedEdgeIDs {
					edges[id] = struct{}{}
				}
			case OpUpdateEdge:
				if _, deleted := tx.deletedEdges[op.EdgeID]; !deleted {
					edges[op.EdgeID] = struct{}{}
				}
			case OpDeleteEdge:
				edges[op.EdgeID] = struct{}{}
			}
		}
	}
	var node NodeID
	var edge EdgeID
	var version MVCCVersion
	w.commitsSince(tx.readTS, func(v MVCCVersion, ops []Operation) bool {
		if nodes == nil {
			written()
		}
		for j := range ops {
			op := &ops[j]
			switch op.Type {
			case OpDeleteNode:
				if _, ok := nodes[op.NodeID]; ok {
					node, version = op.NodeID, v
					return false
				}
				for _, id := range op.DeletedEdgeIDs {
					if _, ok := edges[id]; ok {
						edge, version = id, v
						return false
					}
				}
			case OpDeleteEdge:
				if _, ok := edges[op.EdgeID]; ok {
					edge, version = op.EdgeID, v
					return false
				}
			}
		}
		return true
	})
	switch {
	case node != "":
		return localizedError(localization.StorageTransactionNodeChangedDetailed(string(node), version.String(), tx.readTS.String()), ErrConflict)
	case edge != "":
		return localizedError(localization.StorageTransactionEdgeChanged(string(edge)), ErrConflict)
	}
	return nil
}

// errEndpointChanged fails a commit validated before a commit of a
// relationship's endpoint, or of a deleted node's relationships, that landed
// ahead of it.
var errEndpointChanged = fmt.Errorf("%w: a relationship endpoint changed before commit", ErrConflict)

// revalidateEndpointsLocked checks tx's final graph against the latest
// state, buffered commits included: every relationship tx leaves still has
// both endpoints, and no node tx deletes has a relationship its cascade
// missed. A transaction validates these when it stages them; a commit of the
// other side acknowledged or committed after that would otherwise leave a
// relationship without its endpoint. Only the final image counts: a body tx
// replaced, or a relationship it moved away from a node it deletes, is not
// checked. The caller holds the stripes of tx's entities, a deleted node's
// exclusively, so neither side changes meanwhile.
func (tx *BadgerTransaction) revalidateEndpointsLocked() error {
	if len(tx.pendingEdges) == 0 && len(tx.deletedNodes) == 0 {
		return nil
	}
	var checked map[NodeID]struct{}
	for _, edge := range tx.pendingEdges {
		for _, id := range [...]NodeID{edge.StartNode, edge.EndNode} {
			if _, deleted := tx.deletedNodes[id]; deleted {
				return errEndpointChanged
			}
			if _, staged := tx.pendingNodes[id]; staged {
				continue
			}
			if _, done := checked[id]; done {
				continue
			}
			if checked == nil {
				checked = make(map[NodeID]struct{})
			}
			checked[id] = struct{}{}
			if _, err := tx.engine.GetStoredNode(id); errors.Is(err, ErrNotFound) {
				return errEndpointChanged
			} else if err != nil {
				return err
			}
		}
	}
	for id := range tx.deletedNodes {
		out, err := tx.engine.GetStoredOutgoingEdges(id)
		if err != nil {
			return err
		}
		in, err := tx.engine.GetStoredIncomingEdges(id)
		if err != nil {
			return err
		}
		for _, edge := range append(out, in...) {
			if _, deleted := tx.deletedEdges[edge.ID]; deleted {
				continue
			}
			if moved := tx.pendingEdges[edge.ID]; moved != nil && moved.StartNode != id && moved.EndNode != id {
				continue
			}
			return errEndpointChanged
		}
	}
	return nil
}
