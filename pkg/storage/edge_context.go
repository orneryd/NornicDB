package storage

import "context"

// UpdateEdgeWithContext dispatches preparation through the enforcing owner.
func UpdateEdgeWithContext(ctx context.Context, engine interface{ UpdateEdge(*Edge) error }, edge *Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if owner, ok := engine.(interface {
		UpdateEdgeWithContext(context.Context, *Edge) error
	}); ok {
		return owner.UpdateEdgeWithContext(ctx, edge)
	}
	return engine.UpdateEdge(edge)
}

// DeleteEdgeWithContext dispatches preparation through the enforcing owner.
func DeleteEdgeWithContext(ctx context.Context, engine interface{ DeleteEdge(EdgeID) error }, id EdgeID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if owner, ok := engine.(interface {
		DeleteEdgeWithContext(context.Context, EdgeID) error
	}); ok {
		return owner.DeleteEdgeWithContext(ctx, id)
	}
	return engine.DeleteEdge(id)
}

// BulkDeleteEdgesWithContext retains each engine's atomic batch semantics.
func BulkDeleteEdgesWithContext(ctx context.Context, engine interface{ BulkDeleteEdges([]EdgeID) error }, ids []EdgeID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if owner, ok := engine.(interface {
		BulkDeleteEdgesWithContext(context.Context, []EdgeID) error
	}); ok {
		return owner.BulkDeleteEdgesWithContext(ctx, ids)
	}
	return engine.BulkDeleteEdges(ids)
}

// CreateEdgeWithContext keeps edge staging/preparation in the caller's lifetime
// without unwrapping an enforcing engine. Legacy engines retain their own path.
func CreateEdgeWithContext(ctx context.Context, engine interface{ CreateEdge(*Edge) error }, edge *Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if owner, ok := engine.(interface {
		CreateEdgeWithContext(context.Context, *Edge) error
	}); ok {
		return owner.CreateEdgeWithContext(ctx, edge)
	}
	return engine.CreateEdge(edge)
}

// BulkCreateEdgesWithContext is the batch counterpart of CreateEdgeWithContext.
func BulkCreateEdgesWithContext(ctx context.Context, engine interface{ BulkCreateEdges([]*Edge) error }, edges []*Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if owner, ok := engine.(interface {
		BulkCreateEdgesWithContext(context.Context, []*Edge) error
	}); ok {
		return owner.BulkCreateEdgesWithContext(ctx, edges)
	}
	return engine.BulkCreateEdges(edges)
}

// CreateMintedEdgeWithContext preserves the caller's minted identity policy.
func CreateMintedEdgeWithContext(ctx context.Context, engine interface{ CreateMintedEdge(*Edge) error }, edge *Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if owner, ok := engine.(interface {
		CreateMintedEdgeWithContext(context.Context, *Edge) error
	}); ok {
		return owner.CreateMintedEdgeWithContext(ctx, edge)
	}
	return engine.CreateMintedEdge(edge)
}

// BulkCreateMintedEdgesWithContext preserves that policy for a staged batch.
func BulkCreateMintedEdgesWithContext(ctx context.Context, engine interface{ BulkCreateMintedEdges([]*Edge) error }, edges []*Edge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if owner, ok := engine.(interface {
		BulkCreateMintedEdgesWithContext(context.Context, []*Edge) error
	}); ok {
		return owner.BulkCreateMintedEdgesWithContext(ctx, edges)
	}
	return engine.BulkCreateMintedEdges(edges)
}
