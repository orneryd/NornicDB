package storage

import "strings"

// ScopedLabelNodeReader is an optional extension interface for label reads
// within one database. The label index is shared by every database on the
// server, so an unscoped label read loads and decodes the label's nodes in
// all of them; a scoped read skips another database's index entries before
// reading their nodes (#851). scope is the database's node ID prefix ("db:");
// "" reads every database, as the unscoped methods do.
//
// Engines that wrap another engine pass the scope down when the inner engine
// implements this interface and fall back to the unscoped read otherwise, so
// a result may still hold nodes outside the scope: callers keep filtering by
// database.
type ScopedLabelNodeReader interface {
	GetNodesByLabelInScope(scope, label string) ([]*Node, error)
	GetFirstNodeByLabelInScope(scope, label string) (*Node, error)
	StreamNodesByLabelProjectedInScope(scope, label string, properties []string, visit func(*Node) error) error
	GetNodesByLabelVisibleAtInScope(scope, label string, version MVCCVersion) ([]*Node, error)
}

// nodeIDInScope reports whether id belongs to the database scope ("" is every
// database).
func nodeIDInScope(id NodeID, scope string) bool {
	return scope == "" || strings.HasPrefix(string(id), scope)
}

// The helpers below are the scoped reads of an inner engine for the wrapping
// engines: the scoped method when the inner engine has it, the unscoped one
// otherwise.

func getNodesByLabelInScope(engine Engine, scope, label string) ([]*Node, error) {
	if scoped, ok := engine.(ScopedLabelNodeReader); ok {
		return scoped.GetNodesByLabelInScope(scope, label)
	}
	return engine.GetNodesByLabel(label)
}

func getFirstNodeByLabelInScope(engine Engine, scope, label string) (*Node, error) {
	if scoped, ok := engine.(ScopedLabelNodeReader); ok {
		return scoped.GetFirstNodeByLabelInScope(scope, label)
	}
	return engine.GetFirstNodeByLabel(label)
}

func streamNodesByLabelProjectedInScope(engine Engine, scope, label string, properties []string, visit func(*Node) error) error {
	if scoped, ok := engine.(ScopedLabelNodeReader); ok {
		return scoped.StreamNodesByLabelProjectedInScope(scope, label, properties, visit)
	}
	if reader, ok := engine.(ProjectedLabelNodeReader); ok {
		return reader.StreamNodesByLabelProjected(label, properties, visit)
	}
	return ErrNotImplemented
}

func getNodesByLabelVisibleAtInScope(engine Engine, scope, label string, version MVCCVersion) ([]*Node, error) {
	if scoped, ok := engine.(ScopedLabelNodeReader); ok {
		return scoped.GetNodesByLabelVisibleAtInScope(scope, label, version)
	}
	if provider, ok := engine.(MVCCIndexedVisibilityEngine); ok {
		return provider.GetNodesByLabelVisibleAt(label, version)
	}
	return nil, ErrNotImplemented
}
