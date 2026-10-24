package storage

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v4"
)

// Scope is supplied before a writer is constructed and before every durable
// issuance. Inventory fields borrow immutable writer state; they do not copy it.
type nodeCacheWriteScope struct {
	nodeID               NodeID
	transaction          *BadgerTransaction
	writes               map[string][]byte
	deletes              map[string]bool
	buffered             []bufferedCommit
	relationshipKeys     *relationshipCacheInvalidations
	relationshipPrepared bool
	cacheNode            **Node
	cacheEdge            *Edge
	cacheRelationships   bool
	nodeIDs              func(func(NodeID))
	relationships        func(func(*Edge))
	known                bool
	declared             bool
}

// Static staged inventories are captured once before physical units run.
// Direct/batch callbacks populate borrowed edge references while staging, so
// their inventory is captured by the durable terminal after those callbacks.
func (b *BadgerEngine) prepareRelationshipCacheWrites(scope nodeCacheWriteScope, txn *badger.Txn, stagedOnly bool) (nodeCacheWriteScope, error) {
	if scope.relationshipPrepared || !scope.known {
		return scope, nil
	}
	if stagedOnly && scope.transaction == nil && len(scope.buffered) == 0 && len(scope.writes) == 0 && len(scope.deletes) == 0 {
		return scope, nil
	}
	keys, err := b.collectRelationshipCacheWrites(scope, txn)
	if err != nil {
		return scope, err
	}
	scope.relationshipKeys = keys
	scope.relationshipPrepared = true
	return scope, nil
}

// Single writers borrow their before/after bodies until durability. The
// callback's inventory is populated by the same transaction that writes them.
func relationshipCacheWrites(edges ...*Edge) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, relationships: func(visit func(*Edge)) {
		for _, edge := range edges {
			if edge != nil {
				visit(edge)
			}
		}
	}}
}

func nodeDeleteCacheWrites(id NodeID, edges *[]*Edge) nodeCacheWriteScope {
	scope := relationshipBatchCacheWrites(edges)
	scope.nodeID = id
	return scope
}

// A batch writer populates this borrowed inventory in the same transaction
// that writes its bodies. The durable owner interprets the complete scope.
func relationshipBatchCacheWrites(edges *[]*Edge) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, relationships: func(visit func(*Edge)) {
		for _, edge := range *edges {
			if edge != nil {
				visit(edge)
			}
		}
	}}
}

func relationshipUpdateCacheWrites(edge *Edge, old **Edge) nodeCacheWriteScope {
	return nodeCacheWriteScope{known: true, declared: true, cacheEdge: edge, relationships: func(visit func(*Edge)) {
		if edge != nil {
			visit(edge)
		}
		if *old != nil {
			visit(*old)
		}
	}}
}

type nodeCacheUnknownEntrance string

func nodeBodyCacheWrites(node **Node, id NodeID) nodeCacheWriteScope {
	scope := singleNodeCacheWrites(id)
	scope.cacheNode = node
	return scope
}

func relationshipBodyCacheWrites(edge *Edge) nodeCacheWriteScope {
	scope := relationshipCacheWrites(edge)
	scope.cacheEdge = edge
	return scope
}

func relationshipRewriteCacheWrites(edges *[]*Edge) nodeCacheWriteScope {
	scope := relationshipBatchCacheWrites(edges)
	scope.cacheRelationships = true
	return scope
}

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
