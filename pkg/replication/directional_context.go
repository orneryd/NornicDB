package replication

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GetOutgoingEdgesWithContext forwards read preparation through the retained
// immediate engine; mutation enforcement and publication ownership stay local.
func (e *ReplicatedEngine) GetOutgoingEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return storage.GetOutgoingEdgesWithContext(ctx, e.Engine, id)
}

// GetIncomingEdgesWithContext forwards the same incoming read lifetime.
func (e *ReplicatedEngine) GetIncomingEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return storage.GetIncomingEdgesWithContext(ctx, e.Engine, id)
}

// GetAdjacentEdgesWithContext retains the inner combined reader's read cut.
func (e *ReplicatedEngine) GetAdjacentEdgesWithContext(ctx context.Context, id storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	return storage.GetAdjacentEdgesWithContext(ctx, e.Engine, id)
}

// OwnsDirectionalReads binds the contextual forward to this actual decorator.
func (e *ReplicatedEngine) OwnsDirectionalReads(engine storage.Engine) bool { return engine == e }
