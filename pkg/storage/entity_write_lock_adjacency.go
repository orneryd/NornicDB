package storage

// readsLockedAdjacencyLatestLocked reports whether the transaction reads
// nodeID's relationships as last committed instead of at its snapshot
// (NOR-213). It does once it holds the node's write lock: its statements then
// decide on the node as last committed, so they must also see the
// relationships other transactions committed on it, as they already see the
// node's committed properties (LockNodeForWrite).
//
// The lock's property refresh cannot tell when that matters: a commit that
// creates or deletes a relationship does not move its end nodes' heads. What
// can tell is the namespace: while no commit followed the snapshot, the
// snapshot is the last committed state and its cached read answers; after
// one, the read takes the engine's last committed view, the view write-behind
// transactions already read. Relationship creates take no end-node lock, so
// each read is current as of that read, not frozen at the lock grant.
// Unpinned and write-behind transactions already read the last committed view.
// Caller holds tx.mu.
func (tx *BadgerTransaction) readsLockedAdjacencyLatestLocked(nodeID NodeID) bool {
	if len(tx.entityLocks.held) == 0 || tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		return false
	}
	if tx.entityLocks.find(entityWriteLockKey(bufferedNodeEntity, string(nodeID))) == nil {
		return false
	}
	// The same "no commit followed the snapshot" test the lock's property
	// refresh makes (entityCommittedAfterSnapshot); saturated sequences and
	// in-flight begin frames count as a later commit.
	current := tx.readTS.CommitSequence < maxMVCCCommitSequence && tx.inflightAtBegin == nil && tx.namespace != "" &&
		tx.engine.currentMVCCReadVersion(tx.namespace).CommitSequence == tx.readTS.CommitSequence
	return !current
}

// recordLatestEndpointsLocked records the relationships a read of locked node
// nodeID returned as last committed, and the nodes at their other ends. The
// rest of the statement, and later statements, consume them: they look the
// relationship up again (startNode, endNode, DELETE, SET), read the other end
// (its labels, properties, existence) and maintain stored counts from its
// labels. Every such by-ID resolution must see the same last committed
// entity the read returned, never the snapshot's older or missing one
// (readsNodeLastCommittedLocked, readsEdgeLastCommittedLocked). Caller holds
// tx.mu.
func (tx *BadgerTransaction) recordLatestEndpointsLocked(nodeID NodeID, edges []*Edge) {
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		if tx.entityLocks.edges == nil {
			tx.entityLocks.edges = make(map[EdgeID]struct{})
		}
		tx.entityLocks.edges[edge.ID] = struct{}{}
		other := edge.EndNode
		if other == nodeID {
			other = edge.StartNode
		}
		if other == nodeID {
			continue
		}
		if tx.entityLocks.endpoints == nil {
			tx.entityLocks.endpoints = make(map[NodeID]struct{})
		}
		tx.entityLocks.endpoints[other] = struct{}{}
	}
}

// readsNodeLastCommittedLocked reports whether the transaction resolves node
// id as last committed instead of at its snapshot: it holds the node's write
// lock taken after a later commit (readsLatestLocked), or a locked node's last
// committed relationship read returned it as an other end. Every by-ID read
// of a committed node consults it ahead of the snapshot and its caches.
// Write validation does not: only the lock exempts a write from it. Caller
// holds tx.mu.
func (tx *BadgerTransaction) readsNodeLastCommittedLocked(id NodeID) bool {
	if tx.readsLatestLocked(bufferedNodeEntity, string(id)) {
		return true
	}
	if len(tx.entityLocks.endpoints) == 0 {
		return false
	}
	_, ok := tx.entityLocks.endpoints[id]
	return ok
}

// readsEdgeLastCommittedLocked is readsNodeLastCommittedLocked for a
// relationship. Caller holds tx.mu.
func (tx *BadgerTransaction) readsEdgeLastCommittedLocked(id EdgeID) bool {
	if tx.readsLatestLocked(bufferedEdgeEntity, string(id)) {
		return true
	}
	if len(tx.entityLocks.edges) == 0 {
		return false
	}
	_, ok := tx.entityLocks.edges[id]
	return ok
}
