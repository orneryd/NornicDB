package storage

import (
	"context"
	"strings"
)

// ScopedLabelNodeReader is an optional extension interface for label reads
// within one database. The label index is shared by every database on the
// server. Scoped Badger reads retain an in-scope streaming prefix, then use
// the existing namespace dictionary at the first foreign boundary without
// enumerating that foreign run. scope is the database's node ID prefix ("db:");
// "" reads every database, as the unscoped methods do.
//
// Engines that wrap another engine pass the scope down when the inner engine
// implements this interface and fall back to the unscoped read otherwise, so
// a result may still hold nodes outside the scope: callers keep filtering by
// database.
type ScopedLabelNodeReader interface {
	scopedLabelNodes
	scopedLabelFirstNode
	scopedProjectedLabelNodes
	scopedHistoricalLabelNodes
}

type scopedLabelNodes interface {
	GetNodesByLabelInScope(scope, label string) ([]*Node, error)
}
type scopedLabelFirstNode interface {
	GetFirstNodeByLabelInScope(scope, label string) (*Node, error)
}
type scopedProjectedLabelNodes interface {
	StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
}
type scopedHistoricalLabelNodes interface {
	GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error)
}

// ScopedLabelNodeIDReader is the ID-only counterpart to ScopedLabelNodeReader.
// Its scope uses the same physical database prefix, and false stops the visit.
type ScopedLabelNodeIDReader interface {
	ForEachNodeIDByLabelInScope(scope, label string, visit func(NodeID) bool) error
}

var (
	_ ScopedLabelNodeIDReader = (*BadgerEngine)(nil)
	_ ScopedLabelNodeIDReader = (*MemoryEngine)(nil)
	_ ScopedLabelNodeIDReader = (*WALEngine)(nil)
	_ ScopedLabelNodeIDReader = (*TracedEngine)(nil)
	_ LabelNodeIDLookupEngine = (*TracedEngine)(nil)
	_ ScopedLabelNodeReader   = (*TracedEngine)(nil)
)

// nodeIDInScope reports whether id belongs to the database scope ("" is every
// database).
func nodeIDInScope(id NodeID, scope string) bool {
	return scope == "" || strings.HasPrefix(string(id), scope)
}

// The helpers below are the scoped reads of an inner engine for the wrapping
// engines: the scoped method when the inner engine has it, the unscoped one
// otherwise.

func getNodesByLabelInScope(engine Engine, scope, label string) ([]*Node, error) {
	return getNodesByLabelInScopeWithContext(context.Background(), engine, scope, label)
}

// getNodesByLabelInScopeWithContext carries the caller through this read and its materialization.
func getNodesByLabelInScopeWithContext(ctx context.Context, engine Engine, scope, label string) ([]*Node, error) {
	return ReadWithCapability(engine, func(scoped scopedLabelNodes) ([]*Node, error) {
		return GetNodesByLabelInScopeWithContext(ctx, scoped, scope, label)
	}, func() ([]*Node, error) { return engine.GetNodesByLabel(label) })
}

func getFirstNodeByLabelInScope(engine Engine, scope, label string) (*Node, error) {
	return getFirstNodeByLabelInScopeWithContext(context.Background(), engine, scope, label)
}

// getFirstNodeByLabelInScopeWithContext carries the caller through this read and its materialization.
func getFirstNodeByLabelInScopeWithContext(ctx context.Context, engine Engine, scope, label string) (*Node, error) {
	return ReadWithCapability(engine, func(scoped scopedLabelFirstNode) (*Node, error) {
		return GetFirstNodeByLabelInScopeWithContext(ctx, scoped, scope, label)
	}, func() (*Node, error) { return engine.GetFirstNodeByLabel(label) })
}

func streamNodesByLabelProjectedInScope(engine Engine, scope, label string, properties []string, visit func(*Node) error) error {
	return streamNodesByLabelProjectedInScopeWithContext(context.Background(), engine, scope, label, properties, visit)
}

// streamNodesByLabelProjectedInScopeWithContext carries the caller through this read and its materialization.
func streamNodesByLabelProjectedInScopeWithContext(ctx context.Context, engine Engine, scope, label string, properties []string, visit func(*Node) error) error {
	return VisitWithCapability(engine, func(reader scopedProjectedLabelNodes, deliver func(*Node) error) error {
		return StreamNodesByLabelProjectedInScopeWithContext(ctx, reader, scope, label, properties, deliver)
	}, func(deliver func(*Node) error) error {
		return VisitWithCapability(engine, func(reader ProjectedLabelNodeReader, deliver func(*Node) error) error {
			return StreamNodesByLabelProjectedWithContext(ctx, reader, label, properties, deliver)
		}, func(next func(*Node) error) error {
			return (nodeReadOperation{engine, StreamNodesOptions{Prefix: scope, Label: label, Projection: properties, WithEmbeddings: properties == nil, ApplyDecayFilter: true}}).visit(ctx, next)
		}, deliver)
	}, func(node *Node) error {
		if node == nil || !nodeIDInScope(node.ID, scope) {
			return nil
		}
		return visit(node)
	})
}

func getNodesByLabelVisibleAtInScope(engine Engine, scope, label string, version MVCCVersion) ([]*Node, error) {
	return getNodesByLabelVisibleAtInScopeWithContext(context.Background(), engine, scope, label, version)
}

// getNodesByLabelVisibleAtInScopeWithContext carries the caller through this read and its materialization.
func getNodesByLabelVisibleAtInScopeWithContext(ctx context.Context, engine Engine, scope, label string, version MVCCVersion) ([]*Node, error) {
	return ReadWithCapability(engine, func(reader scopedHistoricalLabelNodes) ([]*Node, error) {
		return GetNodesByLabelVisibleAtInScopeWithContext(ctx, reader, scope, label, version)
	}, func() ([]*Node, error) {
		return ReadWithCapability(engine, func(reader historicalLabelNodes) ([]*Node, error) {
			return GetNodesByLabelVisibleAtWithContext(ctx, reader, label, version)
		}, nil)
	})
}

func forEachNodeIDByLabelInScope(engine Engine, scope, label string, visit func(NodeID) bool) error {
	return forEachNodeIDByLabelInScopeWithContext(context.Background(), engine, scope, label, visit)
}

// forEachNodeIDByLabelInScopeWithContext carries the caller through this read and its materialization.
func forEachNodeIDByLabelInScopeWithContext(ctx context.Context, engine Engine, scope, label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}
	inScope := func(id NodeID) bool { return !nodeIDInScope(id, scope) || visit(id) }
	return VisitWhileWithCapability(engine, func(reader ScopedLabelNodeIDReader, deliver func(NodeID) bool) error {
		return ForEachNodeIDByLabelInScopeWithContext(ctx, reader, scope, label, deliver)
	}, func(deliver func(NodeID) bool) error {
		return VisitWhileWithCapability(engine, func(reader LabelNodeIDLookupEngine, next func(NodeID) bool) error {
			return ForEachNodeIDByLabelWithContext(ctx, reader, label, next)
		}, func(next func(NodeID) bool) error {
			return (nodeReadOperation{engine, StreamNodesOptions{Prefix: scope, Label: label, Projection: []string{}, ApplyDecayFilter: true}}).visit(ctx, func(node *Node) error {
				if !next(node.ID) {
					return ErrIterationStopped
				}
				return nil
			})
		}, deliver)
	}, inScope)
}

// ForEachNodeIDByLabelInScope forwards scoped ID iteration through WAL.
func (w *WALEngine) ForEachNodeIDByLabelInScope(scope, label string, visit func(NodeID) bool) error {
	return w.ForEachNodeIDByLabelInScopeWithContext(context.Background(), scope, label, visit)
}

// ForEachNodeIDByLabelInScopeWithContext carries the caller through this read and its materialization.
func (w *WALEngine) ForEachNodeIDByLabelInScopeWithContext(ctx context.Context, scope, label string, visit func(NodeID) bool) error {
	return forEachNodeIDByLabelInScopeWithContext(ctx, w.engine, scope, label, visit)
}

// ForEachNodeIDByLabelInScope forwards scoped ID iteration through tracing.
func (t *TracedEngine) ForEachNodeIDByLabelInScope(scope, label string, visit func(NodeID) bool) error {
	return t.ForEachNodeIDByLabelInScopeWithContext(context.Background(), scope, label, visit)
}

// ForEachNodeIDByLabelInScopeWithContext carries the caller through this read and its materialization.
func (t *TracedEngine) ForEachNodeIDByLabelInScopeWithContext(ctx context.Context, scope, label string, visit func(NodeID) bool) error {
	return forEachNodeIDByLabelInScopeWithContext(ctx, t.Engine, scope, label, visit)
}

// ForEachNodeIDByLabel forwards ID iteration through tracing.
func (t *TracedEngine) ForEachNodeIDByLabel(label string, visit func(NodeID) bool) error {
	return t.ForEachNodeIDByLabelWithContext(context.Background(), label, visit)
}

// ForEachNodeIDByLabelWithContext carries the caller through this read and its materialization.
func (t *TracedEngine) ForEachNodeIDByLabelWithContext(ctx context.Context, label string, visit func(NodeID) bool) error {
	return ForEachNodeIDByLabelInScopeWithContext(ctx, t, "", label, visit)
}

// GetNodesByLabelInScope forwards the scoped label reader through tracing.
func (t *TracedEngine) GetNodesByLabelInScope(scope, label string) ([]*Node, error) {
	return t.GetNodesByLabelInScopeWithContext(context.Background(), scope, label)
}

// GetNodesByLabelInScopeWithContext carries the caller through this read and its materialization.
func (t *TracedEngine) GetNodesByLabelInScopeWithContext(ctx context.Context, scope, label string) ([]*Node, error) {
	return getNodesByLabelInScopeWithContext(ctx, t.Engine, scope, label)
}

// GetFirstNodeByLabelInScope forwards the scoped first reader through tracing.
func (t *TracedEngine) GetFirstNodeByLabelInScope(scope, label string) (*Node, error) {
	return t.GetFirstNodeByLabelInScopeWithContext(context.Background(), scope, label)
}

// GetFirstNodeByLabelInScopeWithContext carries the caller through this read and its materialization.
func (t *TracedEngine) GetFirstNodeByLabelInScopeWithContext(ctx context.Context, scope, label string) (*Node, error) {
	return getFirstNodeByLabelInScopeWithContext(ctx, t.Engine, scope, label)
}

// StreamNodesByLabelProjectedInScope forwards the scoped stream through tracing.
func (t *TracedEngine) StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error {
	return t.StreamNodesByLabelProjectedInScopeWithContext(context.Background(), scope, label, properties, visit)
}

// StreamNodesByLabelProjectedInScopeWithContext carries the caller through this read and its materialization.
func (t *TracedEngine) StreamNodesByLabelProjectedInScopeWithContext(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	return streamNodesByLabelProjectedInScopeWithContext(ctx, t.Engine, scope, label, properties, visit)
}

// GetNodesByLabelVisibleAtInScope forwards the scoped snapshot read through tracing.
func (t *TracedEngine) GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error) {
	return t.GetNodesByLabelVisibleAtInScopeWithContext(context.Background(), scope, label, version)
}

// GetNodesByLabelVisibleAtInScopeWithContext carries the caller through this read and its materialization.
func (t *TracedEngine) GetNodesByLabelVisibleAtInScopeWithContext(ctx context.Context, scope, label string, version MVCCVersion) ([]*Node, error) {
	return getNodesByLabelVisibleAtInScopeWithContext(ctx, t.Engine, scope, label, version)
}
