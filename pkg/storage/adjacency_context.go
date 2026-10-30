package storage

import "context"

// ContextualDirectionalReadOwner explicitly binds contextual forwarding to
// the actual engine. A promoted method must not bypass a decorator's legacy
// read override. Implementations compare engine with their own receiver;
// embedding an implementation alone does not opt a decorator in.
type ContextualDirectionalReadOwner interface {
	OwnsDirectionalReads(engine Engine) bool
}

func ownsDirectionalContext(engine Engine) bool {
	owner, ok := engine.(ContextualDirectionalReadOwner)
	return ok && owner.OwnsDirectionalReads(engine)
}

// OwnsDirectionalReads binds these read methods to this concrete engine.
func (b *BadgerEngine) OwnsDirectionalReads(engine Engine) bool { return engine == b }

// OwnsDirectionalReads deliberately opts the embedded in-memory engine in.
func (m *MemoryEngine) OwnsDirectionalReads(engine Engine) bool { return engine == m }

// OwnsDirectionalReads binds namespace projection to the actual view.
func (n *NamespacedEngine) OwnsDirectionalReads(engine Engine) bool { return engine == n }

// OwnsDirectionalReads binds WAL's read forwarding to the actual decorator.
func (w *WALEngine) OwnsDirectionalReads(engine Engine) bool { return engine == w }

// GetOutgoingEdgesWithContext reads the same visible outgoing relationships
// through the retained directional dispatcher, including request preparation.
// Example: GetOutgoingEdgesWithContext(ctx, tenantStore, "person").
func GetOutgoingEdgesWithContext(ctx context.Context, engine Engine, nodeID NodeID) ([]*Edge, error) {
	out, _, err := readAdjacencyWithContext(ctx, engine, adjacencyOutgoing, nodeID, "", nil, visibleRead)
	return out, err
}

// GetIncomingEdgesWithContext is the incoming side of that same read owner.
// Example: GetIncomingEdgesWithContext(ctx, tenantStore, "person").
func GetIncomingEdgesWithContext(ctx context.Context, engine Engine, nodeID NodeID) ([]*Edge, error) {
	_, in, err := readAdjacencyWithContext(ctx, engine, adjacencyIncoming, nodeID, "", nil, visibleRead)
	return in, err
}

// GetAdjacentEdgesWithContext captures both directions through the same owner,
// retaining a combined provider's single read cut and partial-result policy.
// Example: GetAdjacentEdgesWithContext(ctx, tenantStore, "person").
func GetAdjacentEdgesWithContext(ctx context.Context, engine Engine, nodeID NodeID) ([]*Edge, []*Edge, error) {
	return readAdjacencyWithContext(ctx, engine, adjacencyBoth, nodeID, "", nil, visibleRead)
}
