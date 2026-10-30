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

// Operations returns detached diagnostic images, including before-images and
// cascading-deletion IDs. Encoders use EntityTransactionRequest{Prepared: p}
// to read the immutable accepted snapshot without allocating this public copy.
func (p *PreparedEntityCommit) Operations() []Operation {
	ops, _ := p.OperationsWithContext(context.Background())
	return ops
}

// OperationsWithContext snapshots command images during pre-acceptance work.
func (p *PreparedEntityCommit) OperationsWithContext(ctx context.Context) ([]Operation, error) {
	return prepareEntityOperationsWithContext(ctx, p.operations)
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	changes := make([]StoredEntityChange, len(p.changes))
	for i := range p.changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		changes[i] = p.changes[i]
		v := &changes[i]
		var err error
		v.BeforeNode, err = copyNodeWithContext(ctx, v.BeforeNode)
		if err != nil {
			return nil, err
		}
		v.AfterNode, err = copyNodeWithContext(ctx, v.AfterNode)
		if err != nil {
			return nil, err
		}
		v.BeforeEdge, err = copyEdgeWithContext(ctx, v.BeforeEdge)
		if err != nil {
			return nil, err
		}
		v.AfterEdge, err = copyEdgeWithContext(ctx, v.AfterEdge)
		if err != nil {
			return nil, err
		}
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

// EntityCommitRequest identifies an accepted commit and optional decoded changes.
// Operations are staged only when LogicalID has not already been acknowledged.
// An already-staged transaction leaves Operations nil; replay passes its existing
// operation slice without copying it or allocating a staging callback.
type EntityCommitRequest struct {
	LogicalID       string
	Operations      []Operation
	clientNodeEdits bool
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
		if err := tx.ensureLifecycleActiveLocked(); err != nil {
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
		if err := ApplyEntityChangeWithContext(ctx, tx, op, func(node *Node) error {
			if request.clientNodeEdits {
				return ApplyNodeUpdateWithContext(ctx, tx, node)
			}
			return tx.UpdateGraphNodeWithContext(ctx, node, StoredNodeImages)
		}, func(id NodeID) error { return tx.DeleteGraphNodeWithContext(ctx, id, CascadeEdges) }); err != nil {
			return nil, err
		}
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
			operations, err := prepareEntityOperationsWithContext(ctx, tx.operations)
			if err != nil {
				return err
			}
			nodes, edges, endpoints, err := tx.pendingEntityAuthorityKeysLocked(ctx)
			if err != nil {
				return err
			}
			schema, owner := tx.engine.GetSchemaForNamespace(tx.namespace), tx.ID
			prepared = &PreparedEntityCommit{engine: tx.engine, mode: tx.commitMode, namespace: tx.namespace, logicalID: logicalID, operations: operations, changes: changes, decide: make(chan bool, 1), finished: finished}
			tx.preparedEntityCommit = prepared
			prepared.retainAuthority = func(retainCtx context.Context) (func(), error) {
				return retainPreparedAuthorityWithContext(retainCtx, schema, owner, nodes, edges, endpoints, operations)
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
func retainPreparedAuthorityWithContext(ctx context.Context, schema *SchemaManager, owner string, nodes []NodeID, edges []EdgeID, endpoints []NodeID, operations []Operation) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	keys, err := entityCommitKeysWithContext(ctx, nodes, edges, endpoints)
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

// detachPreparedPropertyMaps completes the caller-owned diagnostic copy. Native
// storage copies intentionally retain property values; diagnostic consumers may
// mutate supported list/map values without changing accepted commit images.
// This is never used by the canonical prepared encoder or commit path.
func detachPreparedPropertyMaps(nodes []*Node, edges []*Edge) {
	_ = detachPreparedPropertyMapsWithContext(context.Background(), nodes, edges)
}
func detachPreparedPropertyMapsWithContext(ctx context.Context, nodes []*Node, edges []*Edge) error {
	detach := func(properties map[string]any) error {
		for key, value := range properties {
			if err := ctx.Err(); err != nil {
				return err
			}
			if value != nil {
				copied, err := clonePreparedPropertyValueWithContext(ctx, value)
				if err != nil {
					return err
				}
				properties[key] = copied
			}
		}
		return nil
	}
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if node != nil {
			if err := detach(node.Properties); err != nil {
				return err
			}
			if err := detach(node.EmbedMeta); err != nil {
				return err
			}
		}
	}
	for _, edge := range edges {
		if err := ctx.Err(); err != nil {
			return err
		}
		if edge != nil {
			if err := detach(edge.Properties); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}

// clonePreparedPropertyValue preserves the concrete list/map types supported
// by prepared wire images and nil containers. Scalars and strings are immutable.
func clonePreparedPropertyValue(value any) any {
	out, _ := clonePreparedPropertyValueWithContext(context.Background(), value)
	return out
}
func clonePreparedPropertyValueWithContext(ctx context.Context, value any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch value := value.(type) {
	case []byte:
		return clonePreparedScalarsWithContext(ctx, value)
	case []string:
		return clonePreparedScalarsWithContext(ctx, value)
	case []int:
		return clonePreparedScalarsWithContext(ctx, value)
	case []int32:
		return clonePreparedScalarsWithContext(ctx, value)
	case []int64:
		return clonePreparedScalarsWithContext(ctx, value)
	case []float32:
		return clonePreparedScalarsWithContext(ctx, value)
	case []float64:
		return clonePreparedScalarsWithContext(ctx, value)
	case []bool:
		return clonePreparedScalarsWithContext(ctx, value)
	case []any:
		if value == nil {
			return value, nil
		}
		out := make([]any, len(value))
		for i, item := range value {
			v, err := clonePreparedPropertyValueWithContext(ctx, item)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, ctx.Err()
	case map[string]any:
		if value == nil {
			return value, nil
		}
		out := make(map[string]any, len(value))
		for key, item := range value {
			v, err := clonePreparedPropertyValueWithContext(ctx, item)
			if err != nil {
				return nil, err
			}
			out[key] = v
		}
		return out, ctx.Err()
	default:
		return value, nil
	}
}
func clonePreparedScalarsWithContext[T byte | string | int | int32 | int64 | float32 | float64 | bool](ctx context.Context, source []T) ([]T, error) {
	if source == nil {
		return nil, ctx.Err()
	}
	out := make([]T, len(source))
	for i, value := range source {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out[i] = value
	}
	return out, ctx.Err()
}

// prepareEntityOperationsWithContext freezes the same detached prepared images
// as Operations. Wire encoding consumes the immutable private snapshot directly.
func prepareEntityOperationsWithContext(ctx context.Context, source []Operation) ([]Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(source) == 0 {
		return nil, nil
	}
	ops := make([]Operation, len(source))
	for i, operation := range source {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		op := &ops[i]
		*op = operation
		var err error
		if op.Node, err = copyNodeWithContext(ctx, op.Node); err != nil {
			return nil, err
		}
		if op.Edge, err = copyEdgeWithContext(ctx, op.Edge); err != nil {
			return nil, err
		}
		if op.OldNode, err = copyNodeWithContext(ctx, op.OldNode); err != nil {
			return nil, err
		}
		if op.OldEdge, err = copyEdgeWithContext(ctx, op.OldEdge); err != nil {
			return nil, err
		}
		if err = detachPreparedPropertyMapsWithContext(ctx, []*Node{op.Node, op.OldNode}, []*Edge{op.Edge, op.OldEdge}); err != nil {
			return nil, err
		}
		if op.DeletedEdgeIDs != nil {
			ids := make([]EdgeID, len(op.DeletedEdgeIDs))
			for j, id := range op.DeletedEdgeIDs {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				ids[j] = id
			}
			op.DeletedEdgeIDs = ids
		}
	}
	return ops, ctx.Err()
}
