package storage

import (
	"context"
	"errors"
	"sync"
)

// EntityCommitSubmitter is discovered before unwrapping a storage wrapper.
// A non-nil error means definitely not accepted; accepted callers receive a
// receipt whose terminal owner survives waiter cancellation.
type EntityCommitSubmitter interface {
	SubmitEntityCommit(context.Context, *PreparedEntityCommit, func(CommitOutcome)) (*CommitReceipt, error)
}

// PreparedEntityCommit transfers one validated native transaction to an
// accepted command. It retains native snapshot/admission ownership until the
// command chooses physical commit or definite discard. Caller wait cancellation
// is not a discard decision.
type PreparedEntityCommit struct {
	mode                 EntityCommitMode
	markRecoveryPending  func()
	hooksMu              sync.Mutex
	appliedHooks         []func()
	retainAuthority      func(context.Context) (func(), error)
	engine               *BadgerEngine
	terminal             CommitOutcome
	afterPublication     func()
	namespace, logicalID string
	operations           []Operation
	changes              []StoredEntityChange
	decide               chan bool
	finished             chan error
	once                 sync.Once
}

func (p *PreparedEntityCommit) Namespace() string { return p.namespace }
func (p *PreparedEntityCommit) LogicalID() string { return p.logicalID }
func (p *PreparedEntityCommit) Operations() []Operation {
	ops, _ := p.OperationsWithContext(context.Background())
	return ops
}

// OperationsWithContext snapshots command images during pre-acceptance work.
func (p *PreparedEntityCommit) OperationsWithContext(ctx context.Context) ([]Operation, error) {
	ops := make([]Operation, len(p.operations))
	for i := range p.operations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ops[i] = p.operations[i]
		ops[i].snapshotForNotification()
	}
	return ops, ctx.Err()
}

// CountDelta projects namespace-scoped physical counts without copying images.
// The prepared pairs remain private and immutable throughout admission.
func (p *PreparedEntityCommit) CountDelta() (nodes, edges int64) {
	nodes, edges, _ = p.CountDeltaWithContext(context.Background())
	return nodes, edges
}

// CountDeltaWithContext folds the same private stored images during admission.
func (p *PreparedEntityCommit) CountDeltaWithContext(ctx context.Context) (nodes, edges int64, err error) {
	return StoredEntityCountDeltaWithContext(ctx, p.namespace, p.changes)
}
func (p *PreparedEntityCommit) Changes() []StoredEntityChange {
	changes, _ := p.ChangesWithContext(context.Background())
	return changes
}

// ChangesWithContext copies an admission population without exposing a partial
// snapshot on cancellation. Accepted publication uses the same compatibility body.
func (p *PreparedEntityCommit) ChangesWithContext(ctx context.Context) ([]StoredEntityChange, error) {
	changes := make([]StoredEntityChange, len(p.changes))
	for i := range p.changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		changes[i] = p.changes[i]
		v := &changes[i]
		v.BeforeNode = CopyNode(v.BeforeNode)
		v.AfterNode = CopyNode(v.AfterNode)
		v.BeforeEdge = CopyEdge(v.BeforeEdge)
		v.AfterEdge = CopyEdge(v.AfterEdge)
	}
	return changes, ctx.Err()
}

func (p *PreparedEntityCommit) startCommit(afterPublication func()) error {
	selected := false
	p.once.Do(func() {
		selected = true
		p.afterPublication = func() {
			drainReleaseCallbacks(&p.hooksMu, &p.appliedHooks)
			if afterPublication != nil {
				afterPublication()
			}
		}

		p.decide <- true
	})
	if !selected {
		return ErrTransactionClosed
	}
	return nil
}
func (p *PreparedEntityCommit) Commit(afterPublication func()) error {
	if err := p.startCommit(afterPublication); err != nil {
		return err
	}
	return <-p.finished
}

func (p *PreparedEntityCommit) Abort() { p.once.Do(func() { p.decide <- false; <-p.finished }) }

var errPreparedEntityDiscarded = errors.New("prepared entity commit discarded")

// PrepareEntityCommit runs the existing native commit pipeline up to its
// prephysical boundary. Schema transactions retain their ordinary Commit path.
func (tx *BadgerTransaction) PrepareEntityCommit(ctx context.Context, logicalID string) (*PreparedEntityCommit, error) {
	if logicalID == "" {
		return nil, ErrInvalidID
	}
	applied, receiptErr := tx.PrepareLogicalCommitReceipt(logicalID)
	if receiptErr != nil {
		return nil, receiptErr
	}
	if applied {
		tx.mu.Lock()
		namespace := tx.namespace
		engine := tx.engine
		mode := tx.commitMode
		tx.mu.Unlock()
		_ = tx.Rollback()
		tx.clientDetached.Store(true)
		done := make(chan error, 1)
		done <- nil
		return &PreparedEntityCommit{engine: engine, mode: mode, namespace: namespace, logicalID: logicalID, decide: make(chan bool, 1), finished: done, terminal: CommitOutcome{State: CommitApplied}}, nil
	}
	tx.mu.Lock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		tx.mu.Unlock()
		return nil, err
	}
	tx.commitLogicalID = logicalID
	tx.clientDetached.Store(true)
	tx.mu.Unlock()
	ready := make(chan *PreparedEntityCommit, 1)
	finished := make(chan error, 1)
	go func() {
		var prepared *PreparedEntityCommit
		err := tx.commitContextWithPreparation(ctx, func() {
			if prepared != nil && prepared.afterPublication != nil {
				prepared.afterPublication()
			}
		}, func() error {
			if tx.schemaRuntime != nil || tx.knowledgeSchemaDirty {
				return ErrInvalidData
			}
			changes, err := tx.capturePreparedEntityChangesLocked()
			if err != nil {
				return err
			}
			operations, err := prepareEntityOperationsWithContext(ctx, tx.operations)
			if err != nil {
				return err
			}
			nodes, edges, err := tx.pendingEntityAuthorityKeysLocked(ctx)
			if err != nil {
				return err
			}
			schema, owner := tx.engine.GetSchemaForNamespace(tx.namespace), tx.ID
			prepared = &PreparedEntityCommit{engine: tx.engine, mode: tx.commitMode, namespace: tx.namespace, logicalID: logicalID, operations: operations, changes: changes, decide: make(chan bool, 1), finished: finished}
			prepared.retainAuthority = func(retainCtx context.Context) (func(), error) {
				return retainPreparedAuthorityWithContext(retainCtx, schema, owner, nodes, edges, operations)
			}
			prepared.markRecoveryPending = func() { schema.markCommitOwnerRecoveryPending(owner) }
			tx.commitReleaseAcceptedAuthority = func() { drainReleaseCallbacks(&prepared.hooksMu, &prepared.appliedHooks) }
			// Frontend cleanup must never wait behind an accepted command.
			// Ownership is already detached, so ordinary methods reject it;
			// only this private native continuation may use the staged state.
			tx.mu.Unlock()
			ready <- prepared
			commit := <-prepared.decide
			tx.mu.Lock()
			if !commit {
				return errPreparedEntityDiscarded
			}
			// Accepted work belongs to its terminal owner, no longer the waiter.
			tx.commitCtx = nil
			return nil
		})
		if prepared != nil {
			state := CommitRecoveryPending
			if tx.Status == TxStatusCommitted {
				state = CommitApplied
			} else if tx.Status == TxStatusRolledBack && tx.badgerDB.oracle.failure() == nil {
				state = CommitDiscarded
			}
			prepared.terminal = CommitOutcome{State: state, Err: err}
		}
		finished <- err
	}()
	select {
	case prepared := <-ready:
		if ctx != nil && ctx.Err() != nil {
			prepared.Abort()
			return nil, ctx.Err()
		}
		return prepared, nil
	case err := <-finished:
		return nil, err
	}
}

// RetainAcceptedAuthority transfers an additional reentrant hold to a durable
// command owner. Applied publication releases it before native listeners;
// recovery-pending commands retain it after an inner native rollback.
// The returned release is only for definite pre-record/log discard.
func (p *PreparedEntityCommit) RetainAcceptedAuthority() (func(), error) {
	return p.RetainAcceptedAuthorityWithContext(context.Background())
}

// RetainAcceptedAuthorityWithContext prepares an accepted owner's reentrant
// holds before journal admission. Definite rejection leaves no new hold.
func (p *PreparedEntityCommit) RetainAcceptedAuthorityWithContext(ctx context.Context) (func(), error) {
	if p.retainAuthority == nil {
		return func() {}, nil
	}
	release, err := p.retainAuthority(ctx)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	releaseOnce := func() { once.Do(release) }
	p.hooksMu.Lock()
	p.appliedHooks = append(p.appliedHooks, releaseOnce)
	p.hooksMu.Unlock()
	return releaseOnce, nil
}

// retainPreparedAuthorityWithContext is the single projection/acquisition body
// shared by cancellable admission and context-free accepted recovery holders.
func retainPreparedAuthorityWithContext(ctx context.Context, schema *SchemaManager, owner string, nodes []NodeID, edges []EdgeID, operations []Operation) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keys, err := entityCommitKeysWithContext(ctx, nodes, edges)
	if err != nil {
		return nil, err
	}
	releaseEntities, err := schema.acquireUniqueConstraintCommitLocks(ctx, owner, keys)
	if err != nil {
		return nil, err
	}
	var nodeImages []*Node
	var edgeImages []*Edge
	for _, op := range operations {
		if err := ctx.Err(); err != nil {
			releaseEntities()
			return nil, err
		}
		if op.Node != nil {
			nodeImages = append(nodeImages, op.Node)
		}
		if op.OldNode != nil {
			nodeImages = append(nodeImages, op.OldNode)
		}
		if op.Edge != nil {
			edgeImages = append(edgeImages, op.Edge)
		}
		if op.OldEdge != nil {
			edgeImages = append(edgeImages, op.OldEdge)
		}
	}
	valueKeys, err := schema.appendConstraintValueKeysWithContext(ctx, nil, nodeImages, edgeImages)
	if err != nil {
		releaseEntities()
		return nil, err
	}
	releaseValues, err := schema.acquireUniqueConstraintCommitLocks(ctx, owner, valueKeys)
	if err != nil {
		releaseEntities()
		return nil, err
	}
	return func() { releaseValues(); releaseEntities() }, nil
}

// MarkRecoveryPending preserves accepted authority while making ordinary
// waiters fail with the existing conflict class, allowing a safe repair cut.
func (p *PreparedEntityCommit) MarkRecoveryPending() {
	if p.markRecoveryPending != nil {
		p.markRecoveryPending()
	}
}

// drainReleaseCallbacks retires a release set under its actual owner mutex,
// then invokes every callback after unlocking. Prepared ownership deliberately
// uses its own mutex: accepted publication must never wait on client tx.mu.
func drainReleaseCallbacks[C []func() | map[uniqueConstraintLockKey]func()](mu *sync.Mutex, slot *C) {
	mu.Lock()
	callbacks := *slot
	var empty C
	*slot = empty
	mu.Unlock()
	switch callbacks := any(&callbacks).(type) {
	case *[]func():
		for _, release := range *callbacks {
			release()
		}
	case *map[uniqueConstraintLockKey]func():
		for _, release := range *callbacks {
			release()
		}
	}
}

func (p *PreparedEntityCommit) Mode() EntityCommitMode { return p.mode }

// prepareEntityOperationsWithContext freezes notification images before a
// prepared handle is returned. Accepted completion retains those owned images.
func prepareEntityOperationsWithContext(ctx context.Context, source []Operation) ([]Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, nil
	}
	out := make([]Operation, 0, len(source))
	for _, operation := range source {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		operation.snapshotForNotification()
		out = append(out, operation)
	}
	return out, nil
}
