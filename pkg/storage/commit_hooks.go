package storage

import (
	"context"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Commit hooks: a database's code that runs at the commit of each of its
// transactions (APOC triggers). The storage layer knows nothing about what a
// hook does; it calls the one hook registered for the transaction's database
// at three points:
//
//   - BeforeCommit, once per commit, first thing in Commit: the transaction
//     is still active and holds no lock of its own (tx.mu is free) and none of
//     the commit's (the write barrier, the unique-constraint commit locks, the
//     change log), so the hook may read and write through the transaction like
//     any statement does. What it writes is part of the commit. An error rolls
//     the transaction back and is Commit's error.
//   - AfterCommit, after a successful commit, once Commit has released every
//     lock and published its notification.
//   - AfterRollback, after a transaction that changed something rolled back:
//     an explicit Rollback, or a Commit that failed.
//
// A hook can't end the transaction it runs for: Commit and Rollback called
// from BeforeCommit fail with a commit-hook re-entry error instead of
// re-entering the commit. BeforeCommit runs at most once per transaction, so
// what it writes is never handed to it again (APOC's beforeCommit is called
// once per commit).
//
// A database with a hook writes in transactions only: a direct engine write
// (CreateNode, UpdateNode, … outside a transaction) runs in its own
// transaction there (inCommitTransaction), so every write of the database
// reaches the hook, as change capture's writes do.

// CommitHookPhases are the points a hook wants to be called at; a phase it
// doesn't want costs nothing (no change set is built for it).
type CommitHookPhases uint8

const (
	// CommitHookBefore calls BeforeCommit.
	CommitHookBefore CommitHookPhases = 1 << iota
	// CommitHookAfter calls AfterCommit.
	CommitHookAfter
	// CommitHookRollback calls AfterRollback.
	CommitHookRollback
)

// CommitHook is a database's commit hook.
type CommitHook interface {
	// Phases returns the phases the hook is called at. It is read once per
	// transaction, at the start of its commit or rollback.
	Phases() CommitHookPhases
	// BeforeCommit runs in the committing transaction, before its commit;
	// ctx is the commit's (CommitContext's, else background). changes is
	// what the transaction changed before the hook ran.
	BeforeCommit(ctx context.Context, tx *BadgerTransaction, changes *ChangeSet) error
	// AfterCommit runs after a successful commit; changes includes what
	// BeforeCommit wrote, and the commit's sequence and time.
	AfterCommit(changes *ChangeSet)
	// AfterRollback runs after a transaction that changed something rolled
	// back; changes is what it had changed.
	AfterRollback(changes *ChangeSet)
}

// CommitHookEngine is implemented by engines that run commit hooks; the
// storage wrappers reach it through CommitHookEngineOf.
type CommitHookEngine interface {
	// SetCommitHook sets a database's hook; nil removes it.
	SetCommitHook(namespace string, hook CommitHook)
	// CommitHook returns a database's hook, nil for none.
	CommitHook(namespace string) CommitHook
}

// CommitHookEngineOf returns the hook-running engine under engine's wrappers
// (namespace, async, WAL, replication, tracing), if it has one: the
// BadgerEngine itself, also for a MemoryEngine, so every wrapper chain over
// one engine gives the same value.
func CommitHookEngineOf(engine Engine) (CommitHookEngine, bool) {
	for engine != nil {
		if memory, ok := engine.(*MemoryEngine); ok && memory.BadgerEngine != nil {
			return memory.BadgerEngine, true
		}
		if hooks, ok := engine.(CommitHookEngine); ok {
			return hooks, true
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil, false
		}
		inner := wrapper.GetInnerEngine()
		if inner == engine {
			return nil, false
		}
		engine = inner
	}
	return nil, false
}

// NodeChange is one node's change in a transaction: Before is its committed
// state (nil: the transaction created it), After its state at the commit
// (nil: the transaction deleted it). A node the transaction created and
// deleted is not in a change set. The nodes are the transaction's own: a
// hook must not modify them.
type NodeChange struct {
	ID     NodeID
	Before *Node
	After  *Node
}

// EdgeChange is NodeChange for a relationship.
type EdgeChange struct {
	ID     EdgeID
	Before *Edge
	After  *Edge
}

// ChangeSet is what a transaction changed, in the order it first changed
// each entity. IDs carry the database prefix.
type ChangeSet struct {
	Namespace string
	Nodes     []NodeChange
	Edges     []EdgeChange
	// Metadata is the transaction's metadata (tx.setMetaData, Bolt tx
	// metadata).
	Metadata map[string]any
	// CommitSequence and CommitTime are the commit's place in the
	// database's commit order and its time; zero before the commit.
	CommitSequence uint64
	CommitTime     time.Time
}

// SetCommitHook sets a database's commit hook; nil removes it. The engine
// keeps the hooks in a copy-on-write map, so a commit reads them with one
// atomic load and an engine without hooks pays nothing more.
func (b *BadgerEngine) SetCommitHook(namespace string, hook CommitHook) {
	b.commitHooksMu.Lock()
	defer b.commitHooksMu.Unlock()
	next := make(map[string]CommitHook)
	if current := b.commitHooks.Load(); current != nil {
		for name, existing := range *current {
			next[name] = existing
		}
	}
	if hook == nil {
		delete(next, namespace)
	} else {
		next[namespace] = hook
	}
	if len(next) == 0 {
		b.commitHooks.Store(nil)
		return
	}
	b.commitHooks.Store(&next)
}

// CommitHook returns a database's commit hook, nil for none.
func (b *BadgerEngine) CommitHook(namespace string) CommitHook {
	hooks := b.commitHooks.Load()
	if hooks == nil || namespace == "" {
		return nil
	}
	return (*hooks)[namespace]
}

// needsCommitTransaction reports whether a database's direct writes must run
// in a transaction: it captures changes, or has a commit hook.
func (b *BadgerEngine) needsCommitTransaction(namespace string) bool {
	return b.ChangeCapture(namespace) != ChangeCaptureOff || b.CommitHook(namespace) != nil
}

// txHookState is a transaction's progress through its commit hook.
type txHookState uint8

const (
	// txHookBeforeRunning: BeforeCommit is running; Commit and Rollback
	// refuse (commitHookReentryError).
	txHookBeforeRunning txHookState = 1 << iota
	// txHookBeforeDone: the hook's start-of-commit work is done; it never
	// runs again for this transaction.
	txHookBeforeDone
)

// commitHookReentryError is Commit's and Rollback's error when the commit
// hook running in the transaction calls them.
func commitHookReentryError() error {
	return localizedError(localization.StorageTransactionCommitHookReentry(), ErrCommitHookReentry)
}

// runBeforeCommitHook is the start of Commit: it calls the database's hook's
// BeforeCommit, without holding tx.mu or any commit lock, and keeps the
// final change set for AfterCommit / AfterRollback. It runs once per
// transaction.
func (tx *BadgerTransaction) runBeforeCommitHook() error {
	if tx.engine == nil || tx.engine.commitHooks.Load() == nil {
		return nil
	}
	tx.mu.Lock()
	if tx.hookState&txHookBeforeRunning != 0 {
		tx.mu.Unlock()
		return commitHookReentryError()
	}
	if tx.Status != TxStatusActive || tx.hookState != 0 || len(tx.operations) == 0 {
		tx.mu.Unlock()
		return nil
	}
	hook := tx.engine.CommitHook(tx.namespace)
	if hook == nil {
		tx.mu.Unlock()
		return nil
	}
	phases := hook.Phases()
	tx.hook, tx.hookPhases = hook, phases
	ctx := tx.commitCtx
	tx.hookState = txHookBeforeRunning
	var operations []Operation
	var metadata map[string]any
	if phases&CommitHookBefore != 0 {
		operations, metadata = tx.hookOperationsLocked()
	}
	tx.mu.Unlock()

	var err error
	if phases&CommitHookBefore != 0 {
		if ctx == nil {
			ctx = context.Background()
		}
		err = hook.BeforeCommit(ctx, tx, tx.engine.changeSet(tx.namespace, operations, metadata))
	}

	// The final change set, for AfterCommit, or for AfterRollback when the
	// hook's error or a later commit step rolls the transaction back.
	tx.mu.Lock()
	tx.hookState = txHookBeforeDone
	if phases&(CommitHookAfter|CommitHookRollback) != 0 && tx.Status == TxStatusActive {
		operations, metadata = tx.hookOperationsLocked()
		tx.mu.Unlock()
		changes := tx.engine.changeSet(tx.namespace, operations, metadata)
		tx.mu.Lock()
		tx.hookChanges = changes
	}
	tx.mu.Unlock()
	return err
}

// hookOperationsLocked copies the transaction's operations and metadata for
// a change set: the hook's own writes append to them.
func (tx *BadgerTransaction) hookOperationsLocked() ([]Operation, map[string]any) {
	return append([]Operation(nil), tx.operations...), copyChangeMetadata(tx.Metadata)
}

// finishCommitHook runs after Commit has released its locks: AfterCommit
// for a committed transaction, AfterRollback for one the commit rolled back.
func (tx *BadgerTransaction) finishCommitHook() {
	// Only Commit's own goroutine sets tx.hook (runBeforeCommitHook), so a
	// commit without a hook skips the lock.
	if tx.hook == nil {
		return
	}
	tx.mu.Lock()
	hook, phases, changes, status := tx.hook, tx.hookPhases, tx.hookChanges, tx.Status
	tx.hook, tx.hookChanges = nil, nil
	if changes != nil {
		changes.CommitSequence = tx.CommitVersion.CommitSequence
		changes.CommitTime = tx.CommitVersion.CommitTimestamp
	}
	tx.mu.Unlock()
	if hook == nil || changes == nil {
		return
	}
	switch {
	case status == TxStatusCommitted && phases&CommitHookAfter != 0:
		hook.AfterCommit(changes)
	case status == TxStatusRolledBack && phases&CommitHookRollback != 0:
		changes.CommitSequence, changes.CommitTime = 0, time.Time{}
		hook.AfterRollback(changes)
	}
}

// rollbackHookLocked returns, for a Rollback of a transaction that changed
// something, the hook to call AfterRollback on and what it changed.
func (tx *BadgerTransaction) rollbackHookLocked() (CommitHook, []Operation, map[string]any) {
	if tx.engine == nil || tx.engine.commitHooks.Load() == nil || len(tx.operations) == 0 {
		return nil, nil, nil
	}
	hook := tx.engine.CommitHook(tx.namespace)
	if hook == nil || hook.Phases()&CommitHookRollback == 0 {
		return nil, nil, nil
	}
	operations, metadata := tx.hookOperationsLocked()
	return hook, operations, metadata
}

// changeSet is what operations changed. A relationship a node delete
// removed (DeletedEdgeIDs) and that no operation of its own names is read
// as committed: it is in the change set as deleted.
func (b *BadgerEngine) changeSet(namespace string, operations []Operation, metadata map[string]any) *ChangeSet {
	states := newCommitStates(operations)
	changes := &ChangeSet{Namespace: namespace, Metadata: metadata}
	seenNodes := make(map[NodeID]bool)
	seenEdges := make(map[EdgeID]bool)
	addEdge := func(id EdgeID) {
		if seenEdges[id] {
			return
		}
		seenEdges[id] = true
		before, after := states.committedEdges[id], states.finalEdges[id]
		if before == nil && after == nil {
			return
		}
		changes.Edges = append(changes.Edges, EdgeChange{ID: id, Before: before, After: after})
	}
	for _, op := range operations {
		switch op.Type {
		case OpCreateNode, OpUpdateNode, OpDeleteNode:
			id := op.NodeID
			if op.Node != nil {
				id = op.Node.ID
			}
			if op.Type == OpDeleteNode {
				for _, edgeID := range op.DeletedEdgeIDs {
					if _, own := states.committedEdges[edgeID]; own {
						addEdge(edgeID)
						continue
					}
					if seenEdges[edgeID] {
						continue
					}
					seenEdges[edgeID] = true
					if edge, err := b.GetEdge(edgeID); err == nil && edge != nil {
						changes.Edges = append(changes.Edges, EdgeChange{ID: edgeID, Before: edge})
					}
				}
			}
			if seenNodes[id] {
				continue
			}
			seenNodes[id] = true
			before, after := states.committedNodes[id], states.finalNodes[id]
			if before == nil && after == nil {
				continue
			}
			changes.Nodes = append(changes.Nodes, NodeChange{ID: id, Before: before, After: after})
		case OpCreateEdge, OpUpdateEdge, OpDeleteEdge:
			id := op.EdgeID
			if op.Edge != nil {
				id = op.Edge.ID
			}
			addEdge(id)
		}
	}
	return changes
}
