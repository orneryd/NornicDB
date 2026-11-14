package storage

import (
	"context"
	"errors"
)

// UpsertGraphNode stages a client node image, creating an absent ID or updating
// its stored predecessor even when that predecessor is suppressed. Updates
// preserve the original creation timestamp and use ClientNodeEdits semantics.
// The transaction resolves the predecessor before staging, so an intervening
// change is subject to the normal observed-image conflict checks.
//
// boundary, when provided, receives the resolved operation before staging and
// must invoke apply synchronously exactly once on success. Returning an error aborts the
// caller's operation; callers must roll back the transaction on failure.
// This hook lets existing accounting policies surround the actual write.
//
// For example, a catalog writer can call tx.UpsertGraphNode(ctx, metadata, nil)
// and then prepare and publish the transaction through its normal commit owner.
func (tx *BadgerTransaction) UpsertGraphNode(ctx context.Context, node *Node, boundary func(Operation, func() error) error) error {
	if node == nil {
		return ErrInvalidData
	}
	previous, err := tx.GetStoredNode(ctx, node.ID)
	operation := Operation{Type: OpUpdateNode, Node: node}
	var apply func() error
	if errors.Is(err, ErrNotFound) {
		operation.Type = OpCreateNode
		apply = func() error { _, err := tx.CreateNode(ctx, node); return err }
	} else if err != nil {
		return err
	} else {
		node.CreatedAt = previous.CreatedAt
		operation.OldNode = previous
		apply = func() error {
			return tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: previous, After: node, Policy: ClientNodeEdits})
		}
	}
	if boundary != nil {
		return boundary(operation, apply)
	}
	return apply()
}
