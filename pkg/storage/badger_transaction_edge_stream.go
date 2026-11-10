package storage

import (
	"context"
)

// StreamEdgesByType visits the relationships of edgeType visible to the
// transaction one at a time (EdgeTypeStreamer): the committed edges of its
// snapshot overlaid with its own pending edge writes and deletes, exactly as
// GetEdgesByType merges them, without collecting the type into a slice.
// Committed reads are scoped to the transaction's database. Visited edges are
// copies, so the callback may keep them.
// Admission follows IncidentEdges: the caller's context ends a wait for the
// transaction's lock, and a closed transaction reports ErrTransactionClosed
// first. It also ends the retake after each visitor callback, so a canceled
// stream stops when its visitor returns rather than waiting for that lock, and
// a transaction the client closed or detached while the visitor ran is reported
// the same way, before that retake waits.
func (tx *BadgerTransaction) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return tx.streamEdgesByType(ctx, edgeType, visibleRead, visit)
}

func (tx *BadgerTransaction) streamEdgesByType(ctx context.Context, edgeType string, v readVisibility, visit func(*Edge) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	if err := tx.clientOwnershipError(transactionRead); err != nil {
		return err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	frame := streamVisitFrame{held: true}
	defer func() {
		if frame.held {
			tx.mu.Unlock()
		}
	}()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	invokeVisit := func(edge *Edge) error {
		return streamVisitUnlocked(tx, ctx, &frame, edge, visit)
	}
	nowNanos := DecayScoringTime()
	matches := func(edge *Edge) bool {
		return edge != nil && (edgeType == "" || edge.Type == edgeType) && !tx.engine.filterEntityByDecay(v, edge, nowNanos)
	}

	pendingEdges, overlayErr := writeOwnedReadImagesLocked(ctx, tx.pendingEdges, tx.deletedEdges, &tx.entityLocks, edgeWriteLockKind, tx.engine.GetStoredEdge)
	if overlayErr != nil {
		return overlayErr
	}
	hasPending := len(pendingEdges) > 0 || len(tx.deletedEdges) > 0
	var seen map[EdgeID]struct{}
	if hasPending {
		seen = make(map[EdgeID]struct{}, len(pendingEdges))
	}
	emitCommitted := func(edge *Edge) error {
		if edge == nil {
			return nil
		}
		if hasPending {
			if _, deleted := tx.deletedEdges[edge.ID]; deleted {
				return nil
			}
			if pending, exists := pendingEdges[edge.ID]; exists {
				seen[edge.ID] = struct{}{}
				if matches(pending) {
					return invokeVisit(copyEdge(pending))
				}
				return nil
			}
		}
		return invokeVisit(copyEdge(edge))
	}

	scope := tx.labelScanScopeLocked()
	var err error
	if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the engine's latest
		// (overlaid) stream includes flushes that landed after the pinned
		// snapshot.
		err = tx.engine.streamEdgesByTypeInScope(ctx, scope, edgeType, v, emitCommitted)
	} else {
		err = tx.engine.streamEdgesByTypeAtSnapshotWithView(ctx, scope, edgeType, tx.readTS, tx.withSnapshotViewLocked, v, emitCommitted)
	}
	if err != nil {
		return err
	}
	if !hasPending {
		return nil
	}

	// Edges this transaction created (not in the committed stream). Collect
	// them under the lock first: the callback runs unlocked and may write.
	var created []*Edge
	for id, edge := range pendingEdges {
		if _, emitted := seen[id]; emitted || !matches(edge) {
			continue
		}
		created = append(created, copyEdge(edge))
	}
	for _, edge := range created {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := invokeVisit(edge); err != nil {
			return err
		}
	}
	return nil
}
