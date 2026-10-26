package storage

// ObservedNodeUpdater accepts before/after images from the same transaction.
// Namespace and transaction adapters own both translations when implemented.
type ObservedNodeUpdater interface {
	UpdateNodeObserved(before, after *Node) error
}
type ObservedEdgeUpdater interface {
	UpdateEdgeObserved(before, after *Edge) error
}

// UpdateObservedNode dispatches only on the original operation owner. Absence
// uses that same engine's ordinary atomic update. An executed error (including
// ErrNotImplemented) is never replayed or retried on a child.
func UpdateObservedNode(engine Engine, before, after *Node) error {
	if writer, ok := OwnedCapability[ObservedNodeUpdater](engine); ok {
		return writer.UpdateNodeObserved(before, after)
	}
	return engine.UpdateNode(after)
}
func UpdateObservedEdge(engine Engine, before, after *Edge) error {
	if writer, ok := OwnedCapability[ObservedEdgeUpdater](engine); ok {
		return writer.UpdateEdgeObserved(before, after)
	}
	return engine.UpdateEdge(after)
}
