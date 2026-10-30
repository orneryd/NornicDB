package storage

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
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
	// Author: NOR-288 lane (Codex), 2026-10-26. The retained authority owner
	// is reused by applications of this same accepted logical command.
	accepted             atomic.Bool
	application          acceptedEntityApplication
	authorityOwner       string
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

// Operations returns detached diagnostic images, including before-images and
// cascading-deletion IDs. Encoders use EntityTransactionRequest{Prepared: p}
// to read the immutable accepted snapshot without allocating this public copy.
func (p *PreparedEntityCommit) Operations() []Operation {
	ops := append([]Operation(nil), p.operations...)
	for i := range ops {
		op := &ops[i]
		op.Node = copyNode(op.Node)
		op.Edge = copyEdge(op.Edge)
		op.OldNode = copyNode(op.OldNode)
		op.OldEdge = copyEdge(op.OldEdge)
		detachPreparedPropertyMaps([]*Node{op.Node, op.OldNode}, []*Edge{op.Edge, op.OldEdge})
		if op.DeletedEdgeIDs != nil {
			ids := make([]EdgeID, len(op.DeletedEdgeIDs))
			copy(ids, op.DeletedEdgeIDs)
			op.DeletedEdgeIDs = ids
		}
	}
	return ops
}

// CountDelta projects namespace-scoped physical counts without copying images.
// The prepared pairs remain private and immutable throughout admission.
func (p *PreparedEntityCommit) CountDelta() (nodes, edges int64) {
	return StoredEntityCountDelta(p.namespace, p.changes)
}
func (p *PreparedEntityCommit) Changes() []StoredEntityChange {
	changes := append([]StoredEntityChange(nil), p.changes...)
	for i := range changes {
		v := &changes[i]
		v.BeforeNode = CopyNode(v.BeforeNode)
		v.AfterNode = CopyNode(v.AfterNode)
		v.BeforeEdge = CopyEdge(v.BeforeEdge)
		v.AfterEdge = CopyEdge(v.AfterEdge)
	}
	return changes
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

// EntityCommitRequest identifies an accepted commit and optional decoded changes.
// Operations are staged only when LogicalID has not already been acknowledged.
// An already-staged transaction leaves Operations nil; replay passes its existing
// operation slice without copying it or allocating a staging callback.
type EntityCommitRequest struct {
	LogicalID  string
	Operations []Operation
}

// PrepareEntityCommit observes the logical receipt once, stages new entity work,
// and runs the native commit pipeline up to its prephysical boundary. A staging
// error leaves cleanup with the caller. Schema transactions use ordinary Commit.
func (tx *BadgerTransaction) PrepareEntityCommit(ctx context.Context, request EntityCommitRequest) (*PreparedEntityCommit, error) {
	logicalID := request.LogicalID
	if logicalID == "" {
		return nil, ErrInvalidID
	}
	applied, receiptErr := func() (bool, error) {
		tx.mu.Lock()
		defer tx.mu.Unlock()
		if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
			return false, err
		}
		if tx.namespace == "" || logicalID == "" {
			return false, ErrInvalidData
		}
		// The engine owns receipt observation: it reads through this
		// transaction first, keeping the marker in this transaction's conflict
		// set, then the buffer generations and the physical marker under its
		// retirement exclusion. An absent receipt is staged under the key the
		// observation already built.
		observation, err := tx.engine.observeBufferedCommitReceipt(bufferedReceiptRead{
			namespace: tx.namespace,
			logicalID: logicalID,
			native:    tx.badgerTx,
		})
		if err != nil {
			return false, err
		}
		if observation.present {
			return true, nil
		}
		tx.bufferSet(observation.key, []byte{1})
		return false, nil
	}()
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
	for _, op := range request.Operations {
		if err := ApplyEntityChange(tx, op, func(node *Node) error { return tx.UpdateGraphNode(node, StoredNodeImages) }, func(id NodeID) error { return tx.DeleteGraphNode(id, CascadeEdges) }); err != nil {
			return nil, err
		}
	}
	tx.mu.Lock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
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
		err := tx.PublishCommit(ctx, CommitPublication{AfterPublication: func() {
			if prepared != nil && prepared.afterPublication != nil {
				prepared.afterPublication()
			}
		}, prepare: func() error {
			if tx.schemaRuntime != nil || tx.knowledgeSchemaDirty {
				return ErrInvalidData
			}
			changes, err := tx.engine.captureStoredEntityChanges(storedEntityCapture{staged: tx})
			if err != nil {
				return err
			}
			operations := append([]Operation(nil), tx.operations...)
			for i := range operations {
				operations[i].snapshotForNotification()
			}
			nodes, edges, endpoints := tx.pendingEntityAuthorityKeysLocked()
			schema, owner := tx.engine.GetSchemaForNamespace(tx.namespace), tx.ID
			prepared = &PreparedEntityCommit{engine: tx.engine, authorityOwner: tx.ID, mode: tx.commitMode, namespace: tx.namespace, logicalID: logicalID, operations: operations, changes: changes, decide: make(chan bool, 1), finished: finished}
			tx.preparedEntityCommit = prepared
			prepared.retainAuthority = func(ctx context.Context) (func(), error) {
				releaseEntities, err := schema.acquireUniqueConstraintCommitLocks(ctx, owner, entityCommitKeys(nodes, edges, endpoints))
				if err != nil {
					return nil, err
				}
				var nodeImages []*Node
				var edgeImages []*Edge
				for _, op := range operations {
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
				releaseValues, err := schema.acquireUniqueConstraintCommitLocks(ctx, owner, schema.appendConstraintValueKeys(nil, nodeImages, edgeImages))
				if err != nil {
					releaseEntities()
					return nil, err
				}
				return func() { releaseValues(); releaseEntities() }, nil
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
			tx.acceptedApplication = tx.acceptedApplication || prepared.accepted.Load()
			return nil
		}})
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
// ctx is the caller's work or request context: a contended authority wait
// observes it (Author: NOR-288 lane (Claude), 2026-10-30).
func (p *PreparedEntityCommit) RetainAcceptedAuthority(ctx context.Context) (func(), error) {
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
	p.accepted.Store(true)
	return releaseOnce, nil
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

// detachPreparedPropertyMaps completes the caller-owned diagnostic copy. Native
// storage copies intentionally retain property values; diagnostic consumers may
// mutate supported list/map values without changing accepted commit images.
// This is never used by the canonical prepared encoder or commit path.
func detachPreparedPropertyMaps(nodes []*Node, edges []*Edge) {
	detach := func(properties map[string]any) {
		for key, value := range properties {
			if value != nil {
				properties[key] = clonePreparedPropertyValue(value)
			}
		}
	}
	for _, node := range nodes {
		if node != nil {
			detach(node.Properties)
			detach(node.EmbedMeta)
		}
	}
	for _, edge := range edges {
		if edge != nil {
			detach(edge.Properties)
		}
	}
}

// clonePreparedPropertyValue preserves the concrete list/map types supported
// by prepared wire images and nil containers. Scalars and strings are immutable.
func clonePreparedPropertyValue(value any) any {
	switch value := value.(type) {
	case []byte:
		return slices.Clone(value)
	case []string:
		return slices.Clone(value)
	case []int:
		return slices.Clone(value)
	case []int32:
		return slices.Clone(value)
	case []int64:
		return slices.Clone(value)
	case []float32:
		return slices.Clone(value)
	case []float64:
		return slices.Clone(value)
	case []bool:
		return slices.Clone(value)
	case []any:
		if value == nil {
			return value
		}
		copied := make([]any, len(value))
		for index, item := range value {
			copied[index] = clonePreparedPropertyValue(item)
		}
		return copied
	case map[string]any:
		if value == nil {
			return value
		}
		copied := make(map[string]any, len(value))
		for key, item := range value {
			copied[key] = clonePreparedPropertyValue(item)
		}
		return copied
	default:
		return value
	}
}
