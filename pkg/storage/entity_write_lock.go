package storage

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// EntityWriteLocker is the capability of a transactional storage view that
// takes the write lock of an existing node or relationship before a statement
// changes it, as Neo4j does: the lock is held until the transaction ends, so
// a concurrent writer of the same entity waits at its write instead of
// failing at commit because both wrote the entity from the same snapshot
// (NOR-213).
//
// Each method returns the entity's latest committed image when a transaction
// committed it after this one's snapshot, and nil otherwise. The caller
// applies its change to that image: from then on the transaction reads the
// entity as last committed, and its commit no longer conflicts with the
// writer it waited for. A wait ends with ErrDeadlock when it would close a
// cycle of transactions waiting for each other, with ctx's error, or, after
// the server's transaction timeout (EngineOptions.LockWait), with
// ErrLockWaitTimeout; a wait past the slow-query threshold is logged. An
// entity another transaction deleted fails with ErrConflict.
//
// Example:
//
//	if locker, ok := store.(storage.EntityWriteLocker); ok {
//		latest, err := locker.LockNodeForWrite(ctx, node.ID)
//		if err != nil {
//			return err
//		}
//		if latest != nil {
//			*node = *latest // apply the change to the committed predecessor
//		}
//	}
type EntityWriteLocker interface {
	LockNodeForWrite(ctx context.Context, id NodeID) (*Node, error)
	LockEdgeForWrite(ctx context.Context, id EdgeID) (*Edge, error)
}

var _ EntityWriteLocker = (*BadgerTransaction)(nil)

// entityWriteLock is one entity write lock a transaction holds until it ends.
// latest marks an entity committed after the transaction's snapshot: the
// transaction reads it as last committed, and the lock, not the snapshot,
// keeps that image current until the transaction ends.
type entityWriteLock struct {
	request uniqueConstraintLockRequest
	latest  bool
}

// entityWriteLocks are the entity write locks of one transaction, in the
// order taken. A statement usually writes one or two entities, so they are
// kept inline and found by scanning; a transaction that writes many indexes
// them.
type entityWriteLocks struct {
	inline [2]entityWriteLock
	held   []entityWriteLock
	index  map[uniqueConstraintLockKey]int
	schema *SchemaManager
	latest int
}

const entityWriteLockIndexAfter = 8

func (locks *entityWriteLocks) find(key uniqueConstraintLockKey) *entityWriteLock {
	if locks.index != nil {
		if at, found := locks.index[key]; found {
			return &locks.held[at]
		}
		return nil
	}
	for at := range locks.held {
		if locks.held[at].request.key == key {
			return &locks.held[at]
		}
	}
	return nil
}

func (locks *entityWriteLocks) add(request uniqueConstraintLockRequest) *entityWriteLock {
	if locks.held == nil {
		locks.held = locks.inline[:0]
	}
	locks.held = append(locks.held, entityWriteLock{request: request})
	if locks.index != nil {
		locks.index[request.key] = len(locks.held) - 1
	} else if len(locks.held) > entityWriteLockIndexAfter {
		locks.index = make(map[uniqueConstraintLockKey]int, 2*len(locks.held))
		for at := range locks.held {
			locks.index[locks.held[at].request.key] = at
		}
	}
	return &locks.held[len(locks.held)-1]
}

func entityWriteLockKey(kind bufferedEntityKind, id string) uniqueConstraintLockKey {
	if kind == bufferedEdgeEntity {
		return uniqueConstraintLockKey{entityKind: 2, entityID: id}
	}
	return uniqueConstraintLockKey{entityKind: 1, entityID: id}
}

// LockNodeForWrite takes node id's write lock for the transaction
// (EntityWriteLocker). A node the transaction created, already wrote or
// deleted needs none.
func (tx *BadgerTransaction) LockNodeForWrite(ctx context.Context, id NodeID) (*Node, error) {
	latest, err := tx.lockEntityForWrite(ctx, bufferedNodeEntity, string(id))
	if err != nil || !latest {
		return nil, err
	}
	node, err := tx.engine.getNode(ctx, id, storedRead)
	if errors.Is(err, ErrNotFound) {
		return nil, localizedError(localization.StorageTransactionNodeChanged(string(id)), ErrConflict)
	}
	return node, err
}

// LockEdgeForWrite takes relationship id's write lock for the transaction
// (EntityWriteLocker), as LockNodeForWrite does for a node.
func (tx *BadgerTransaction) LockEdgeForWrite(ctx context.Context, id EdgeID) (*Edge, error) {
	latest, err := tx.lockEntityForWrite(ctx, bufferedEdgeEntity, string(id))
	if err != nil || !latest {
		return nil, err
	}
	edge, err := tx.engine.getEdge(ctx, id, storedRead)
	if errors.Is(err, ErrNotFound) {
		return nil, localizedError(localization.StorageTransactionEdgeChanged(string(id)), ErrConflict)
	}
	return edge, err
}

// lockEntityForWrite holds the write lock of entity id until the transaction
// ends, and reports whether the caller must apply its write to the entity's
// latest committed image: the entity was committed after the transaction's
// snapshot and the transaction has not written it yet.
func (tx *BadgerTransaction) lockEntityForWrite(ctx context.Context, kind bufferedEntityKind, id string) (bool, error) {
	if id == "" {
		return false, nil
	}
	key := entityWriteLockKey(kind, id)
	if err := tx.mu.LockContext(ctx); err != nil {
		return false, err
	}
	// A write the transaction may not make reports that itself.
	if tx.ensureOperationAllowedLocked(transactionRead) != nil || tx.schemaTransaction || tx.schemaRuntime != nil || tx.acceptedApplication {
		tx.mu.Unlock()
		return false, nil
	}
	if err := tx.pinNamespaceFromIDLocked(id); err != nil {
		tx.mu.Unlock()
		return false, nil
	}
	if tx.ownsEntityLocked(kind, id) {
		tx.mu.Unlock()
		return false, nil
	}
	if held := tx.entityLocks.find(key); held != nil {
		latest := held.latest
		tx.mu.Unlock()
		return latest, nil
	}
	namespace, engine := tx.namespace, tx.engine
	schema := tx.entityLocks.schema
	if schema == nil {
		schema = engine.GetSchemaForNamespace(namespace)
	}
	tx.mu.Unlock()

	wait := tx.mergeKeyWait(namespace)
	request, err := schema.acquireEntityWriteLock(ctx, tx.ID, key, &wait)
	if err != nil {
		return false, err
	}
	stale, err := tx.entityCommittedAfterSnapshot(ctx, kind, id)
	if err != nil {
		schema.releaseEntityWriteLocks(tx.ID, []entityWriteLock{{request: request}})
		return false, err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.Status != TxStatusActive {
		schema.releaseEntityWriteLocks(tx.ID, []entityWriteLock{{request: request}})
		return false, nil
	}
	tx.entityLocks.schema = schema
	held := tx.entityLocks.add(request)
	if stale {
		held.latest = true
		tx.entityLocks.latest++
	}
	return stale, nil
}

// ownsEntityLocked reports whether the transaction already wrote entity id:
// it created it, staged a change to it under its lock, or deleted it.
func (tx *BadgerTransaction) ownsEntityLocked(kind bufferedEntityKind, id string) bool {
	if kind == bufferedEdgeEntity {
		_, pending := tx.pendingEdges[EdgeID(id)]
		_, deleted := tx.deletedEdges[EdgeID(id)]
		_, candidate := tx.edgeCreateNoOps[EdgeID(id)]
		return pending || deleted || candidate
	}
	_, pending := tx.pendingNodes[NodeID(id)]
	_, deleted := tx.deletedNodes[NodeID(id)]
	return pending || deleted
}

// entityCommittedAfterSnapshot reports whether entity id, whose write lock
// the transaction now holds, was committed after the transaction's snapshot:
// by a commit still in the write-behind buffer, or one whose head the
// snapshot does not hold. No writer can commit the entity while the lock is
// held, so the answer stays true to the transaction's end. A snapshot no
// commit of its namespace followed needs no read.
func (tx *BadgerTransaction) entityCommittedAfterSnapshot(ctx context.Context, kind bufferedEntityKind, id string) (bool, error) {
	tx.mu.Lock()
	unchanged := tx.inflightAtBegin == nil && tx.namespace != "" && !tx.readTS.IsZero() &&
		tx.engine.currentMVCCReadVersion(tx.namespace).CommitSequence == tx.readTS.CommitSequence
	tx.mu.Unlock()
	if unchanged {
		return false, nil
	}
	if buffer := tx.engine.writeBehind; buffer != nil {
		var pending bool
		if kind == bufferedEdgeEntity {
			_, pending, _ = buffer.GetEdge(EdgeID(id))
		} else {
			_, pending, _ = buffer.GetNode(NodeID(id))
		}
		if pending {
			return true, nil
		}
	}
	var head MVCCHead
	var err error
	if kind == bufferedEdgeEntity {
		head, err = tx.engine.GetEdgeCurrentHead(ctx, EdgeID(id))
	} else {
		head, err = tx.engine.GetNodeCurrentHead(ctx, NodeID(id))
	}
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.Status != TxStatusActive {
		return false, nil
	}
	return tx.snapshotHeadConflict(kind, id, head.Version)
}

// readsLatestLocked reports whether the transaction reads entity id as last
// committed: it holds the entity's write lock, taken after a commit its
// snapshot does not hold (lockEntityForWrite).
func (tx *BadgerTransaction) readsLatestLocked(kind bufferedEntityKind, id string) bool {
	if tx.entityLocks.latest == 0 {
		return false
	}
	held := tx.entityLocks.find(entityWriteLockKey(kind, id))
	return held != nil && held.latest
}

// releaseEntityWriteLocksLocked releases the transaction's entity write
// locks. Without them the entities are read and validated at the snapshot
// again.
func (tx *BadgerTransaction) releaseEntityWriteLocksLocked() {
	if len(tx.entityLocks.held) != 0 {
		tx.entityLocks.schema.releaseEntityWriteLocks(tx.ID, tx.entityLocks.held)
	}
	tx.entityLocks = entityWriteLocks{}
}

// acquireEntityWriteLock takes the exclusive lock of one entity key for
// owner in the canonical commit-lock registry, waiting as wait says while
// another owner holds it (waitForUniqueConstraintLock). The commit of the
// owning transaction takes the key again without waiting.
func (sm *SchemaManager) acquireEntityWriteLock(ctx context.Context, owner string, key uniqueConstraintLockKey, wait *uniqueKeyLockWait) (uniqueConstraintLockRequest, error) {
	request := uniqueConstraintLockRequest{key: key}
	sm.uniqueConstraintCommitLocksMu.Lock()
	if sm.uniqueConstraintCommitLocks == nil {
		sm.uniqueConstraintCommitLocks = make(map[uniqueConstraintLockKey]*uniqueConstraintCommitLock)
	}
	if len(sm.uniqueConstraintCommitLocks) == 0 {
		sm.uniqueConstraintCommitOrder = 0
	}
	lock := sm.uniqueConstraintCommitLocks[key]
	if lock == nil {
		sm.uniqueConstraintCommitOrder++
		if sm.uniqueConstraintCommitOrder == 0 {
			panic("UNIQUE commit lock order overflow")
		}
		lock = uniqueConstraintLockPool.Get().(*uniqueConstraintCommitLock)
		lock.order = sm.uniqueConstraintCommitOrder
		sm.uniqueConstraintCommitLocks[key] = lock
	}
	lock.refs++
	request.lock = lock
	if lock.owner == "" && lock.readerCount() == 0 && !lock.recoveryPending {
		// Free: taken without a second registry round trip.
		lock.owner, lock.holds = owner, 1
		sm.uniqueConstraintCommitLocksMu.Unlock()
		return request, nil
	}
	sm.uniqueConstraintCommitLocksMu.Unlock()
	if err := sm.waitForUniqueConstraintLock(ctx, owner, request, wait); err != nil {
		sm.releaseUniqueConstraintLocks(owner, nil, []uniqueConstraintLockRequest{request})
		return uniqueConstraintLockRequest{}, err
	}
	return request, nil
}

// releaseEntityWriteLocks releases owner's entity write locks, newest first.
func (sm *SchemaManager) releaseEntityWriteLocks(owner string, held []entityWriteLock) {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	for at := len(held) - 1; at >= 0; at-- {
		sm.releaseUniqueConstraintHoldLocked(owner, held[at].request)
	}
	for at := range held {
		sm.dropUniqueConstraintLockReferenceLocked(held[at].request)
	}
}
