package storage

import (
	"cmp"
	"context"
	"github.com/orneryd/nornicdb/internal/ctxsort"
)

// resolveConnectedDeletesLocked finishes the deletes of DeleteGraphNode with RejectConnected at
// commit: a node no relationship connects any more is deleted for good; one
// that is still connected fails the commit, as Neo4j's DeleteConnectedNode
// constraint does. Nodes are checked in ID order, so the error names the
// same node every time.
func (tx *BadgerTransaction) resolveConnectedDeletesLocked(ctx context.Context) error {
	if len(tx.connectedDeletes) == 0 {
		return nil
	}
	ids := make([]NodeID, 0, len(tx.connectedDeletes))
	for id := range tx.connectedDeletes {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := ctxsort.Sort(ctx, ids, cmp.Compare[NodeID], false); err != nil {
		return err
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Every relationship counts, hidden ones too: the delete would
		// otherwise remove them with the node (incidentEdgesLocked).
		remaining, err := tx.incidentEdgesLocked(ctx, id)
		if err != nil {
			return err
		}
		if len(remaining) > 0 {
			return &NodeStillConnectedError{NodeID: id, Namespace: tx.namespace}
		}
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		oldNode := tx.connectedDeletes[id]
		delete(tx.deletedNodes, id)
		result, err := tx.engine.executeNodeDeletion(ctx, tx.staged(), id, oldNode)
		if err != nil {
			return err
		}
		tx.recordNodeDeletionLocked(id, result)
	}
	tx.connectedDeletes = nil
	return nil
}
