package storage

import (
	"hash/maphash"
	"slices"
	"sync"
)

// Entity commit order.
//
// A commit that writes a node or relationship it did not create checks at
// commit that the target still exists, and a commit that deletes one makes
// it gone. The check decides only while no delete of the target can publish
// between it and the writer's own publication. entityCommitOrder provides
// that order: a commit takes, before its checks, the lock of every node and
// relationship it updates, deletes or attaches a new relationship to, and
// keeps them until its write is published (a Badger commit, or a buffered
// acknowledgement). Locks are exact per entity, so commits on disjoint
// entities never wait for each other, and they are taken in one sorted order,
// so two commits never wait on each other in a cycle. A delete whose targets
// are not listed in advance (a namespace drop) takes the whole order.
//
// Lock order: writeBarrier, constraint key locks, entityOrder, then the count
// locks, the commit gate and the buffer lock. Nothing that holds a later lock
// waits for an entity lock, and the flusher takes none.
type entityCommitOrder struct {
	// all is held shared by every keyed acquisition and exclusively by lockAll.
	all    sync.RWMutex
	seed   maphash.Seed
	once   sync.Once
	shards [64]entityOrderShard
}

type entityOrderShard struct {
	mu    sync.Mutex
	locks map[string]*entityOrderLock
}

type entityOrderLock struct {
	mu   sync.Mutex
	refs int
}

// entityOrderSet is the set of entities one commit orders itself on.
type entityOrderSet struct {
	keys map[string]struct{}
}

func (s *entityOrderSet) add(kind bufferedEntityKind, id string) {
	if id == "" {
		return
	}
	if s.keys == nil {
		s.keys = make(map[string]struct{})
	}
	s.keys[string(rune('0'+kind))+id] = struct{}{}
}

func (s *entityOrderSet) addNode(id NodeID) { s.add(bufferedNodeEntity, string(id)) }

func (s *entityOrderSet) addEdge(id EdgeID) { s.add(bufferedEdgeEntity, string(id)) }

func (o *entityCommitOrder) shard(key string) *entityOrderShard {
	o.once.Do(func() { o.seed = maphash.MakeSeed() })
	return &o.shards[maphash.String(o.seed, key)%uint64(len(o.shards))]
}

// lock takes the set's entity locks in sorted order and returns their release.
func (o *entityCommitOrder) lock(s *entityOrderSet) func() {
	if len(s.keys) == 0 {
		return func() {}
	}
	keys := make([]string, 0, len(s.keys))
	for key := range s.keys {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	o.all.RLock()
	held := make([]*entityOrderLock, len(keys))
	for i, key := range keys {
		shard := o.shard(key)
		shard.mu.Lock()
		l := shard.locks[key]
		if l == nil {
			if shard.locks == nil {
				shard.locks = make(map[string]*entityOrderLock)
			}
			l = &entityOrderLock{}
			shard.locks[key] = l
		}
		l.refs++
		shard.mu.Unlock()
		l.mu.Lock()
		held[i] = l
	}
	return func() {
		for i, key := range keys {
			held[i].mu.Unlock()
			shard := o.shard(key)
			shard.mu.Lock()
			if held[i].refs--; held[i].refs == 0 {
				delete(shard.locks, key)
			}
			shard.mu.Unlock()
		}
		o.all.RUnlock()
	}
}

// lockAll orders a delete whose targets are not listed against every commit.
func (o *entityCommitOrder) lockAll() func() {
	o.all.Lock()
	return o.all.Unlock
}

// lockEntityOrderLocked takes the locks of every existing node and
// relationship the transaction's commit writes: updated and deleted nodes
// and relationships, and the endpoints of the relationships it creates or
// updates. Entities the transaction created itself are not visible to any
// other commit and take no lock.
func (tx *BadgerTransaction) lockEntityOrderLocked() func() {
	var set entityOrderSet
	createdNodes := map[NodeID]struct{}{}
	createdEdges := map[EdgeID]struct{}{}
	node := func(id NodeID) {
		if _, own := createdNodes[id]; !own {
			set.addNode(id)
		}
	}
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			createdNodes[op.NodeID] = struct{}{}
		case OpUpdateNode, OpUpdateEmbedding, OpDeleteNode:
			node(op.NodeID)
		case OpCreateEdge:
			createdEdges[op.EdgeID] = struct{}{}
			if op.Edge != nil {
				node(op.Edge.StartNode)
				node(op.Edge.EndNode)
			}
		case OpUpdateEdge, OpDeleteEdge:
			if _, own := createdEdges[op.EdgeID]; !own {
				set.addEdge(op.EdgeID)
			}
			if op.Edge != nil {
				node(op.Edge.StartNode)
				node(op.Edge.EndNode)
			}
		}
	}
	return tx.engine.entityOrder.lock(&set)
}
