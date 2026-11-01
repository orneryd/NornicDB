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
	GetNodesByLabelInScope(ctx context.Context, scope, label string) ([]*Node, error)
	GetFirstNodeByLabelInScope(scope, label string) (*Node, error)
	StreamNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error
	GetNodesByLabelVisibleAtInScope(ctx context.Context, scope, label string, version MVCCVersion) ([]*Node, error)
}

// ScopedLabelNodeIDReader is the ID-only counterpart to ScopedLabelNodeReader.
// Its scope uses the same physical database prefix, and false stops the visit.
type ScopedLabelNodeIDReader interface {
	ForEachNodeIDByLabelInScope(ctx context.Context, scope, label string, visit func(NodeID) bool) error
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

func getNodesByLabelInScope(ctx context.Context, engine Engine, scope, label string) ([]*Node, error) {
	if scoped, ok := engine.(ScopedLabelNodeReader); ok {
		return scoped.GetNodesByLabelInScope(ctx, scope, label)
	}
	return engine.GetNodesByLabel(ctx, label)
}

func getFirstNodeByLabelInScope(engine Engine, scope, label string) (*Node, error) {
	if scoped, ok := engine.(ScopedLabelNodeReader); ok {
		return scoped.GetFirstNodeByLabelInScope(scope, label)
	}
	return engine.GetFirstNodeByLabel(label)
}

func streamNodesByLabelProjectedInScope(ctx context.Context, engine Engine, scope, label string, properties []string, visit func(*Node) error) error {
	if scoped, ok := engine.(ScopedLabelNodeReader); ok {
		return scoped.StreamNodesByLabelProjectedInScope(ctx, scope, label, properties, visit)
	}
	if reader, ok := engine.(ProjectedLabelNodeReader); ok {
		return reader.StreamNodesByLabelProjected(ctx, label, properties, visit)
	}
	return ErrNotImplemented
}

func getNodesByLabelVisibleAtInScope(ctx context.Context, engine Engine, scope, label string, version MVCCVersion) ([]*Node, error) {
	if scoped, ok := engine.(ScopedLabelNodeReader); ok {
		return scoped.GetNodesByLabelVisibleAtInScope(ctx, scope, label, version)
	}
	if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetNodesByLabelVisibleAt(ctx, label, version)
	}
	return nil, ErrNotImplemented
}

func forEachNodeIDByLabelInScope(ctx context.Context, engine Engine, scope, label string, visit func(NodeID) bool) error {
	if visit == nil {
		return nil
	}
	if scoped, ok := engine.(ScopedLabelNodeIDReader); ok {
		return scoped.ForEachNodeIDByLabelInScope(ctx, scope, label, visit)
	}
	inScope := func(id NodeID) bool {
		return !nodeIDInScope(id, scope) || visit(id)
	}
	if lookup, ok := engine.(LabelNodeIDLookupEngine); ok {
		return lookup.ForEachNodeIDByLabel(ctx, label, inScope)
	}
	nodes, err := getNodesByLabelInScope(ctx, engine, scope, label)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node != nil && !inScope(node.ID) {
			break
		}
	}
	return nil
}

// ForEachNodeIDByLabelInScope forwards scoped ID iteration through WAL.
func (w *WALEngine) ForEachNodeIDByLabelInScope(ctx context.Context, scope, label string, visit func(NodeID) bool) error {
	return forEachNodeIDByLabelInScope(ctx, w.engine, scope, label, visit)
}

// ForEachNodeIDByLabelInScope forwards scoped ID iteration through tracing.
func (t *TracedEngine) ForEachNodeIDByLabelInScope(ctx context.Context, scope, label string, visit func(NodeID) bool) error {
	return forEachNodeIDByLabelInScope(ctx, t.Engine, scope, label, visit)
}

// ForEachNodeIDByLabel forwards ID iteration through tracing.
func (t *TracedEngine) ForEachNodeIDByLabel(ctx context.Context, label string, visit func(NodeID) bool) error {
	return t.ForEachNodeIDByLabelInScope(ctx, "", label, visit)
}

// GetNodesByLabelInScope forwards the scoped label reader through tracing.
func (t *TracedEngine) GetNodesByLabelInScope(ctx context.Context, scope, label string) ([]*Node, error) {
	return getNodesByLabelInScope(ctx, t.Engine, scope, label)
}

// GetFirstNodeByLabelInScope forwards the scoped first reader through tracing.
func (t *TracedEngine) GetFirstNodeByLabelInScope(scope, label string) (*Node, error) {
	return getFirstNodeByLabelInScope(t.Engine, scope, label)
}

// StreamNodesByLabelProjectedInScope forwards the scoped stream through tracing.
func (t *TracedEngine) StreamNodesByLabelProjectedInScope(ctx context.Context, scope, label string, properties []string, visit func(*Node) error) error {
	return streamNodesByLabelProjectedInScope(ctx, t.Engine, scope, label, properties, visit)
}

// GetNodesByLabelVisibleAtInScope forwards the scoped snapshot read through tracing.
func (t *TracedEngine) GetNodesByLabelVisibleAtInScope(ctx context.Context, scope, label string, version MVCCVersion) ([]*Node, error) {
	return getNodesByLabelVisibleAtInScope(ctx, t.Engine, scope, label, version)
}
