package multidb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GetOutgoingEdgesWithContext forwards read preparation through the retained
// immediate engine; mutation enforcement and publication ownership stay local.
func (t *sizeTrackingEngine) GetOutgoingEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return storage.GetOutgoingEdgesWithContext(ctx, t.Engine, id)
}

// GetIncomingEdgesWithContext forwards the same incoming read lifetime.
func (t *sizeTrackingEngine) GetIncomingEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return storage.GetIncomingEdgesWithContext(ctx, t.Engine, id)
}

// GetAdjacentEdgesWithContext retains the inner combined reader's read cut.
func (t *sizeTrackingEngine) GetAdjacentEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	return storage.GetAdjacentEdgesWithContext(ctx, t.Engine, id)
}

// OwnsDirectionalReads binds the contextual forward to this actual decorator.
func (t *sizeTrackingEngine) OwnsDirectionalReads(engine storage.Engine) bool { return engine == t }
