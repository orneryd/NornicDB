package storage

import "encoding/binary"

// Scope is supplied before a writer is constructed and before every durable
// issuance. Inventory fields borrow immutable writer state; they do not copy it.
type nodeCacheWriteScope struct {
	nodeID      NodeID
	transaction *BadgerTransaction
	writes      map[string][]byte
	deletes     map[string]bool
	buffered    []bufferedCommit
	nodeIDs     func(func(NodeID))
	known       bool
	declared    bool
}

type nodeCacheUnknownEntrance string

const (
	nodeCacheUnknownManaged     nodeCacheUnknownEntrance = "arbitrary managed callback"
	nodeCacheUnknownMaintenance nodeCacheUnknownEntrance = "physical maintenance"
	nodeCacheUnknownRollback    nodeCacheUnknownEntrance = "large commit rollback"
	nodeCacheUnknownSerializer  nodeCacheUnknownEntrance = "offline serializer"
)

func noNodeCacheWrites() nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true}
}

func singleNodeCacheWrites(id NodeID) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, nodeID: id}
}

func transactionNodeCacheWrites(tx *BadgerTransaction) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, transaction: tx, writes: tx.pendingWrites, deletes: tx.pendingDeletes}
}

func bufferedNodeCacheWrites(commits []bufferedCommit) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, buffered: commits}
}

// The migration already owns this immutable key batch. Borrow it until commit.
func migrationNodeCacheWrites(batch []migrationItem) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, nodeIDs: func(visit func(NodeID)) {
		for _, item := range batch {
			if len(item.key) > 1 && item.key[0] == prefixNode {
				visit(NodeID(item.key[1:]))
			}
		}
	}}
}

func unknownNodeCacheWrites(entrance nodeCacheUnknownEntrance) nodeCacheWriteScope {
	switch entrance {
	case nodeCacheUnknownManaged, nodeCacheUnknownMaintenance, nodeCacheUnknownRollback, nodeCacheUnknownSerializer:
		return nodeCacheWriteScope{declared: true}
	default:
		panic("storage: unknown cache scope requires a listed entrance")
	}
}

// Caller holds nodeCacheMu. Preserve a newer payload if older issuance arrives late.
func (b *BadgerEngine) invalidateNodeCacheIDLocked(id NodeID, ts uint64) {
	if entry, ok := b.nodeCache[id]; ok && entry.readTs < ts {
		delete(b.nodeCache, id)
	}
	if entry, ok := b.queryNodeCache[id]; ok && entry.readTs < ts {
		delete(b.queryNodeCache, id)
	}
}

func operationCacheNodeID(op Operation) NodeID {
	if op.NodeID != "" {
		return op.NodeID
	}
	if op.Node != nil {
		return op.Node.ID
	}
	return ""
}

func operationWritesNodeBody(op Operation) bool {
	switch op.Type {
	case OpCreateNode, OpUpdateNode, OpDeleteNode, OpUpdateEmbedding:
		return true
	}
	return false
}

func cacheOperationsCoverNode(ops []Operation, id NodeID) bool {
	for _, op := range ops {
		if operationWritesNodeBody(op) && operationCacheNodeID(op) == id {
			return true
		}
	}
	return false
}

func (b *BadgerEngine) fenceUnknownNodeCacheWritesLocked(ts uint64) {
	if ts > b.nodeCacheInvalidationTs {
		b.nodeCacheInvalidationTs = ts
	}
}

func (b *BadgerEngine) invalidateNodeCacheOperationsLocked(ops []Operation, ts uint64) {
	for _, op := range ops {
		if operationWritesNodeBody(op) {
			id := operationCacheNodeID(op)
			if id == "" {
				b.fenceUnknownNodeCacheWritesLocked(ts)
			} else {
				b.invalidateNodeCacheIDLocked(id, ts)
			}
		}
	}
}

// Resolve keys through the storage codec, retaining the operation inventory.
// A unique owner need not have a node-body operation (e.g. a metadata sidecar).
// An authoritative operation identifies a legacy shard write. Invalidate every
// codec candidate, including aliases outside that operation. Unaccompanied
// ambiguous raw keys retain the full fence.
func (b *BadgerEngine) invalidateNodeCacheRawKeyLocked(key string, ops []Operation, ts uint64) {
	if len(key) == 0 {
		return
	}
	if key[0] == prefixNode {
		if len(key) == 1 {
			b.fenceUnknownNodeCacheWritesLocked(ts)
		} else {
			b.invalidateNodeCacheIDLocked(NodeID(key[1:]), ts)
		}
		return
	}
	if !nodeCacheEmbeddingPrefix(key[0]) {
		return
	}
	var candidates [2]embeddingKeyOwner
	count := embeddingKeyOwnersInto([]byte(key), key, &candidates)
	owners := candidates[:count]
	if len(owners) == 0 {
		b.fenceUnknownNodeCacheWritesLocked(ts)
		return
	}
	if len(owners) > 1 {
		classified := false
		for _, owner := range owners {
			classified = classified || cacheOperationsCoverNode(ops, owner.id)
		}
		if !classified {
			b.fenceUnknownNodeCacheWritesLocked(ts)
			return
		}
	}
	for _, owner := range owners {
		if owner.id == "" {
			b.fenceUnknownNodeCacheWritesLocked(ts)
			return
		}
		if !cacheOperationsCoverNode(ops, owner.id) {
			b.invalidateNodeCacheIDLocked(owner.id, ts)
		}
	}
}

func (b *BadgerEngine) invalidateNodeCacheRawLocked(writes map[string][]byte, deletes map[string]bool, ops []Operation, ts uint64) {
	for key := range writes {
		b.invalidateNodeCacheRawKeyLocked(key, ops, ts)
	}
	for key := range deletes {
		b.invalidateNodeCacheRawKeyLocked(key, ops, ts)
	}
}

func (b *BadgerEngine) invalidateNodeCacheScopeLocked(scope nodeCacheWriteScope, ts uint64) {
	if !scope.known {
		b.fenceUnknownNodeCacheWritesLocked(ts)
		return
	}
	if scope.nodeID != "" {
		b.invalidateNodeCacheIDLocked(scope.nodeID, ts)
	}
	if scope.nodeIDs != nil {
		scope.nodeIDs(func(id NodeID) { b.invalidateNodeCacheIDLocked(id, ts) })
	}
	if tx := scope.transaction; tx != nil {
		b.invalidateNodeCacheOperationsLocked(tx.operations, ts)
		b.invalidateNodeCacheRawLocked(scope.writes, scope.deletes, tx.operations, ts)
	}
	for i := range scope.buffered {
		c := &scope.buffered[i]
		b.invalidateNodeCacheOperationsLocked(c.ops, ts)
		b.invalidateNodeCacheRawLocked(c.writes, c.deletes, c.ops, ts)
	}
}

// The codec owns the complete current/compatibility family inventory.
func nodeCacheEmbeddingPrefix(prefix byte) bool {
	for _, family := range embeddingKeyFamilies {
		if prefix == family {
			return true
		}
	}
	return false
}

// Maintenance batches already retain these IDs until their commit returns.
func nodeHeadCacheWrites(batch []nodeHeadWrite) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, nodeIDs: func(visit func(NodeID)) {
		for _, write := range batch {
			visit(write.id)
		}
	}}
}

func bootstrapNodeCacheWrites(nodes []*Node) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, nodeIDs: func(visit func(NodeID)) {
		for _, node := range nodes {
			if node != nil {
				visit(node.ID)
			}
		}
	}}
}

// MVCC lifecycle keys name numeric dictionary IDs rather than body keys.
func (b *BadgerEngine) logicalNodeCacheWrites(logical []byte) nodeCacheWriteScope {
	if len(logical) == 9 && logical[0] == prefixMVCCNode {
		if id, ok := b.idDict.lookupNodeIDByNum(binary.BigEndian.Uint64(logical[1:])); ok {
			return singleNodeCacheWrites(id)
		}
	}
	return noNodeCacheWrites()
}
