package storage

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// nodeMutationPreimagesLocked folds only staged mutations. Reads retain no
// observation state, and a create/delete starts a new incarnation of a claim.
func (tx *BadgerTransaction) nodeMutationPreimagesLocked(ctx context.Context) (map[NodeID]*Node, bool, error) {
	updates := 0
	for index := range tx.operations {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		operation := &tx.operations[index]
		if operation.Type == OpUpdateNode {
			updates++
		}
	}
	if updates == 0 {
		return nil, false, nil
	}
	// A pure update batch with one operation per pending identity already
	// carries its ordered first images. No fold map is needed in this case.
	if updates == len(tx.operations) && updates == len(tx.pendingNodes) {
		return nil, true, nil
	}
	before := make(map[NodeID]*Node, min(updates, len(tx.pendingNodes)))
	for index := range tx.operations {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		operation := &tx.operations[index]
		switch operation.Type {
		case OpCreateNode, OpDeleteNode:
			// Updates to an in-frame CREATE are coalesced into that create
			// by UpdateGraphNode, so they retain no committed preimage.
			delete(before, operation.NodeID)
		case OpUpdateNode:
			if _, seen := before[operation.NodeID]; !seen {
				before[operation.NodeID] = operation.nodeClaimPreimage()
			}
		}
	}
	return before, false, nil
}

// nodeClaimPreimage selects the caller's immutable observation when supplied,
// otherwise the stored predecessor captured by native staging.
func (operation *Operation) nodeClaimPreimage() *Node {
	if operation.nodeObservedBefore != nil {
		return operation.nodeObservedBefore
	}
	return operation.OldNode
}

// sameNodeUniqueClaim compares the semantic key, including label membership
// and null's absence of a claim, using the canonical constraint key projection.
// Non-reflexive values cannot reserve a key; decoded temporal values must use
// their index identity rather than Go's internal location pointer identity.
func sameNodeUniqueClaim(ctx context.Context, before, after *Node, label, property string) (bool, error) {
	var old, next interface{}
	if before != nil && hasLabel(before.Labels, label) {
		key, present, err := constraintLockValue(ctx, before, property)
		if err != nil {
			return false, err
		}
		if present && lockableUniqueConstraintKey(uniqueConstraintLockKey{value: key}) {
			old = key
		}
	}
	if after != nil && hasLabel(after.Labels, label) {
		key, present, err := constraintLockValue(ctx, after, property)
		if err != nil {
			return false, err
		}
		if present && lockableUniqueConstraintKey(uniqueConstraintLockKey{value: key}) {
			next = key
		}
	}
	return old == next, nil
}

// validateNodeClaimPreimagesLocked rejects a stale full image before it can
// overwrite an acknowledged key change. Entity authority is already held by
// PublishCommit. The current read bypasses unversioned caches and shares the
// existing buffered/storage read cut and projected body decoder.
func (tx *BadgerTransaction) validateNodeClaimPreimagesLocked(ctx context.Context) error {
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	if len(tx.pendingNodes) == 0 {
		return nil
	}
	constraints, err := schema.GetConstraints(ctx)
	if err != nil {
		return err
	}
	if len(constraints) == 0 {
		return nil
	}
	preimages, distinctUpdates, err := tx.nodeMutationPreimagesLocked(ctx)
	if err != nil {
		return err
	}
	if len(preimages) == 0 && !distinctUpdates {
		return nil
	}
	properties := make([]string, 0, len(constraints))
	include := make(map[string]struct{}, len(constraints))
	for index := range constraints {
		constraint := &constraints[index]
		if _, present := include[constraint.Property]; !present {
			include[constraint.Property] = struct{}{}
			properties = append(properties, constraint.Property)
		}
	}
	// One current read cut covers the whole mutation batch. Capture only
	// matching buffered bodies; the usual physical-only case allocates no
	// overlay map and opens one Badger read transaction rather than one per node.
	var buffered map[NodeID]*Node
	entered := false
	err = tx.engine.withReadView(ctx, func(buffer *WriteBehindBuffer) {
		if buffer == nil {
			return
		}
		capture := func(id NodeID) {
			state := buffer.entityStateLocked(bufferedNodeEntity, string(id), bufferedReadVisibility{engine: tx.engine, visibility: storedRead})
			if !state.found {
				return
			}
			if buffered == nil {
				buffered = make(map[NodeID]*Node)
			}
			if state.deleted {
				buffered[id] = nil
			} else {
				buffered[id] = state.node
			}
		}
		if distinctUpdates {
			for index := range tx.operations {
				operation := &tx.operations[index]
				capture(operation.NodeID)
			}
		} else {
			for id := range preimages {
				capture(id)
			}
		}
	}, func(view *badger.Txn) error {
		entered = true
		// Keep mutation order when several targets conflict, and consume each
		// first preimage once without allocating an ordered ID slice.
		for index := range tx.operations {
			operation := &tx.operations[index]
			id := operation.NodeID
			before := operation.nodeClaimPreimage()
			if !distinctUpdates {
				var exists bool
				before, exists = preimages[id]
				if !exists {
					continue
				}
				delete(preimages, id)
			}
			if before == nil || tx.pendingNodes[id] == nil {
				continue
			}
			current, found := buffered[id]
			if !found {
				item, err := view.Get(nodeKey(id))
				if err != nil && err != badger.ErrKeyNotFound {
					return &CommitFailureError{Cause: err}
				}
				if err == nil {
					current, err = tx.engine.readProjectedNodeItemInTxn(view, item, id, properties, include, nodeBodyBorrowed)
					if err != nil {
						return &CommitFailureError{Cause: err}
					}
				}
			}
			changed := current == nil
			for index := range constraints {
				constraint := &constraints[index]
				same, err := sameNodeUniqueClaim(ctx, before, current, constraint.Label, constraint.Property)
				if err != nil {
					return err
				}
				if !same {
					changed = true
					break
				}
			}
			if changed {
				if err := tx.classifyUpdateConflictAsUniqueViolation(ctx, id); err != nil {
					return err
				}
				return localizedError(localization.StorageTransactionNodeChanged(string(id)), ErrConflict)
			}
		}
		return nil
	}, readViewOptions{scope: tx.namespace})
	if err != nil && !entered {
		return &CommitFailureError{Cause: err}
	}
	return err
}
