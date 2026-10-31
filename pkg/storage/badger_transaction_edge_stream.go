// Author: Codex, NOR-160 snapshot read ownership correction, 2026-10-26.
package storage

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/callerowned"
)

// StreamEdgesByType visits the relationships of edgeType visible to the
// transaction one at a time (EdgeTypeStreamer): the committed edges of its
// snapshot overlaid with its own pending edge writes and deletes, exactly as
// GetEdgesByType merges them, without collecting the type into a slice.
// Committed reads are scoped to the transaction's database. Visited edges are
// copies, so the callback may keep them.
func (tx *BadgerTransaction) StreamEdgesByType(ctx context.Context, edgeType string, visit func(*Edge) error) error {
	return tx.streamEdgesByType(ctx, edgeType, visibleRead, visit)
}

func (tx *BadgerTransaction) streamEdgesByType(ctx context.Context, edgeType string, v readVisibility, visit func(*Edge) error) error {
	if visit == nil {
		return ErrInvalidData
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	invokeVisit := func(edge *Edge) error {
		tx.mu.Unlock()
		err := visit(edge)
		tx.mu.Lock()
		return err
	}
	matches := func(edge *Edge) bool {
		return edge != nil && (edgeType == "" || edge.Type == edgeType)
	}

	hasPending := len(tx.pendingEdges) > 0 || len(tx.deletedEdges) > 0
	var seen map[EdgeID]struct{}
	if hasPending {
		seen = make(map[EdgeID]struct{}, len(tx.pendingEdges))
	}
	// Both the pinned MVCC stream and the live engine stream emit
	// caller-owned edges; only this transaction's pending edges are copied.
	emitCommitted := func(edge *Edge) error {
		if edge == nil {
			return nil
		}
		if hasPending {
			if _, deleted := tx.deletedEdges[edge.ID]; deleted {
				return nil
			}
			if pending, exists := tx.pendingEdges[edge.ID]; exists {
				seen[edge.ID] = struct{}{}
				if matches(pending) {
					return invokeVisit(copyEdge(pending))
				}
				return nil
			}
		}
		return invokeVisit(edge)
	}

	scope := tx.labelScanScopeLocked()
	var err error
	if tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		// Write-behind reads are read-committed: the engine's latest
		// (overlaid) stream includes flushes that landed after the pinned
		// snapshot.
		// The engine's images: an edge this transaction deleted or rewrote
		// is replaced before anything is delivered (NOR-160).
		err = tx.engine.streamEdgesByTypeDecided(ctx, scope, edgeType, v, func(edge *Edge) (bool, error) {
			if hasPending {
				if _, deleted := tx.deletedEdges[edge.ID]; deleted {
					return false, nil
				}
				if pending, exists := tx.pendingEdges[edge.ID]; exists {
					seen[edge.ID] = struct{}{}
					if matches(pending) {
						return false, invokeVisit(copyEdge(pending))
					}
					return false, nil
				}
			}
			return true, nil
		}, invokeVisit, nil)
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
	for id, edge := range tx.pendingEdges {
		if _, emitted := seen[id]; emitted || !matches(edge) {
			continue
		}
		created = append(created, edge)
	}
	created = callerowned.Each(created, copyEdge)
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
