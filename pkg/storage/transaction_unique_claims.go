package storage

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// mutationPreimagesLocked folds the first staged predecessor of each live
// incarnation. Pure distinct updates use their ordered operation images without
// a map; CREATE-only ingestion and ordinary reads retain no preimage state.
// Only an UPDATE target can carry a predecessor, so when CREATE/DELETE
// operations outnumber updates (a mixed import), CREATEs of identities no
// UPDATE targets add no map entries.
func mutationPreimagesLocked[ID comparable, Image any](ctx context.Context, operations []Operation, pending int, updateType OperationType, project func(*Operation) (ID, Image)) (map[ID]Image, bool, error) {
	if pending == 0 {
		return nil, false, nil
	}
	// A single staged identity needs neither a fold nor a bulk poller. Its
	// one admission check is already the final record check.
	if len(operations) == 1 && pending == 1 {
		return nil, operations[0].Type == updateType, ctx.Err()
	}
	createType, deleteType := OpCreateNode, OpDeleteNode
	if updateType == OpUpdateEdge {
		createType, deleteType = OpCreateEdge, OpDeleteEdge
	}
	updates, structural := 0, 0
	var cancellation bulkReadCancellation
	for i := range operations {
		if err := cancellation.check(ctx); err != nil {
			return nil, false, err
		}
		switch operations[i].Type {
		case updateType:
			updates++
		case createType, deleteType:
			structural++
		}
	}
	if updates == 0 {
		return nil, false, ctx.Err()
	}
	if updates == len(operations) && updates == pending {
		return nil, true, ctx.Err()
	}
	var targets map[ID]struct{}
	if structural > updates {
		targets = make(map[ID]struct{}, updates)
		for i := range operations {
			if err := cancellation.check(ctx); err != nil {
				return nil, false, err
			}
			if operations[i].Type == updateType {
				id, _ := project(&operations[i])
				targets[id] = struct{}{}
			}
		}
	}
	before := make(map[ID]Image, min(updates, pending))
	for i := range operations {
		if err := cancellation.check(ctx); err != nil {
			return nil, false, err
		}
		op := &operations[i]
		switch op.Type {
		case createType:
			id, _ := project(op)
			if targets != nil {
				if _, updated := targets[id]; !updated {
					continue
				}
			}
			var empty Image
			before[id] = empty
		case deleteType:
			id, _ := project(op)
			delete(before, id)
		case updateType:
			id, image := project(op)
			if _, seen := before[id]; !seen {
				before[id] = image
			}
		}
	}
	return before, false, ctx.Err()
}

// nodeClaimPreimage selects the caller's immutable observation when supplied,
// otherwise the stored predecessor captured by native staging.
func nodeClaimPreimage(operation *Operation) (NodeID, *Node) {
	if operation.nodeObservedBefore != nil {
		return operation.NodeID, operation.nodeObservedBefore
	}
	return operation.NodeID, operation.OldNode
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

// captureBufferedImage records the stored image a write-behind buffer holds
// for id (nil for a buffered delete), allocating images only on the first
// buffered hit. Node and relationship predecessor validation share it.
func captureBufferedImage[ID ~string, Image *Node | *Edge](buffer *WriteBehindBuffer, engine *BadgerEngine, kind bufferedEntityKind, id ID, images *map[ID]Image, image func(*bufferedEntityState) Image) {
	state := buffer.entityStateLocked(kind, string(id), bufferedReadVisibility{engine: engine, visibility: storedRead})
	if !state.found {
		return
	}
	if *images == nil {
		*images = make(map[ID]Image)
	}
	if state.deleted {
		(*images)[id] = nil
	} else {
		(*images)[id] = image(&state)
	}
}

func bufferedNodeImage(state *bufferedEntityState) *Node { return state.node }
func bufferedEdgeImage(state *bufferedEntityState) *Edge { return state.edge }

// validateMutationPreimagesLocked rejects a stale full image before it can
// overwrite an acknowledged predecessor or constraint key change. Entity
// authority is already held by PublishCommit. The current read bypasses unversioned caches and shares the
// existing buffered/storage read cut and projected body decoder.
func (tx *BadgerTransaction) validateMutationPreimagesLocked(ctx context.Context) error {
	// Node images need validation only with write-behind (whole bodies) or a
	// node unique constraint (claim keys); relationship images only with
	// write-behind. Decide that before folding any staged operation.
	validateBody := tx.engine.writeBehind != nil
	validateNodes := len(tx.pendingNodes) != 0 && (validateBody || tx.engine.GetSchemaForNamespace(tx.namespace).HasUniqueValueTracking())
	if !validateNodes && (!validateBody || len(tx.pendingEdges) == 0) {
		return nil
	}
	var preimages map[NodeID]*Node
	var distinctUpdates bool
	var err error
	if validateNodes {
		preimages, distinctUpdates, err = mutationPreimagesLocked(ctx, tx.operations, len(tx.pendingNodes), OpUpdateNode, nodeClaimPreimage)
		if err != nil {
			return err
		}
	}
	var edgePreimages map[EdgeID]*Edge
	var distinctEdgeUpdates bool
	if validateBody {
		edgePreimages, distinctEdgeUpdates, err = mutationPreimagesLocked(ctx, tx.operations, len(tx.pendingEdges), OpUpdateEdge, func(op *Operation) (EdgeID, *Edge) { return op.EdgeID, op.OldEdge })
	}
	if err != nil {
		return err
	}
	if len(preimages) == 0 && !distinctUpdates && len(edgePreimages) == 0 && !distinctEdgeUpdates {
		return nil
	}
	// Relationship-only updates have no node constraint projection. A create-only
	// batch has already returned without copying the schema's constraint slice.
	var constraints []UniqueConstraint
	if len(preimages) != 0 || distinctUpdates {
		constraints, err = tx.engine.GetSchemaForNamespace(tx.namespace).GetConstraints(ctx)
		if err != nil {
			return err
		}
	}
	if len(constraints) == 0 && !validateBody {
		return nil
	}
	var properties []string
	var include map[string]struct{}
	if !validateBody {
		properties = make([]string, 0, len(constraints))
		include = make(map[string]struct{}, len(constraints))
		for index := range constraints {
			constraint := &constraints[index]
			if _, present := include[constraint.Property]; !present {
				include[constraint.Property] = struct{}{}
				properties = append(properties, constraint.Property)
			}
		}
	}
	// A version is reserved before WAL/accounting preparation, so BEGIN may
	// include a peer's version without having seen its image. For write-behind
	// also compare the actual observed predecessor, not only constraint keys.
	// This uses the same batch read cut and first-mutation preimage owner.
	// One current read cut covers the whole mutation batch. Capture only
	// matching buffered bodies; the usual physical-only case allocates no
	// overlay map and opens one Badger read transaction rather than one per node.
	var buffered map[NodeID]*Node
	var bufferedEdges map[EdgeID]*Edge
	entered := false
	err = tx.engine.withReadView(ctx, func(buffer *WriteBehindBuffer) {
		if buffer == nil {
			return
		}
		capture := func(id NodeID) {
			captureBufferedImage(buffer, tx.engine, bufferedNodeEntity, id, &buffered, bufferedNodeImage)
		}
		captureEdge := func(id EdgeID) {
			captureBufferedImage(buffer, tx.engine, bufferedEdgeEntity, id, &bufferedEdges, bufferedEdgeImage)
		}
		if distinctEdgeUpdates {
			for index := range tx.operations {
				captureEdge(tx.operations[index].EdgeID)
			}
		} else {
			for id := range edgePreimages {
				captureEdge(id)
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
			_, before := nodeClaimPreimage(operation)
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
					if validateBody {
						// Preimage equality needs all properties and labels, not vectors.
						current, err = tx.engine.readNodeItemInTxn(view, item, id, nodeItemBodyOnly, false, nil)
					} else {
						current, err = tx.engine.readProjectedNodeItemInTxn(view, item, id, properties, include, nodeBodyBorrowed)
					}
					if err != nil {
						return &CommitFailureError{Cause: err}
					}
				}
			}
			changed := current == nil
			if !changed && validateBody {
				changed = !sameStoredValue(before.Properties, current.Properties) || !sameLabelSet(before.Labels, current.Labels)
			}
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
		for index := range tx.operations {
			if err := ctx.Err(); err != nil {
				return err
			}
			operation := &tx.operations[index]
			id := operation.EdgeID
			before := operation.OldEdge
			if !distinctEdgeUpdates {
				var exists bool
				before, exists = edgePreimages[id]
				if !exists {
					continue
				}
				delete(edgePreimages, id)
			}
			if before == nil || tx.pendingEdges[id] == nil {
				continue
			}
			current, found := bufferedEdges[id]
			if !found {
				var err error
				current, _, err = tx.engine.readEdgeBodyInTxn(view, id, storedRead)
				if err != nil && err != ErrNotFound {
					return &CommitFailureError{Cause: err}
				}
			}
			if !sameStoredEdgeIdentity(before, current) {
				return localizedError(localization.StorageTransactionEdgeChanged(string(id)), ErrConflict)
			}
		}
		return nil
	}, readViewOptions{scope: tx.namespace})
	if err != nil && !entered {
		return &CommitFailureError{Cause: err}
	}
	return err
}
