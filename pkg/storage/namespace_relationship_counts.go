package storage

import "fmt"

// RelationshipCountTier selects one of the persisted relationship counter tiers.
type RelationshipCountTier uint8

const (
	// RelationshipCountByType counts a relationship type in one database.
	RelationshipCountByType RelationshipCountTier = iota
	// RelationshipCountByStartLabel also constrains the physical start label.
	RelationshipCountByStartLabel
	// RelationshipCountByEndLabel also constrains the physical end label.
	RelationshipCountByEndLabel
)

// NamespaceRelationshipCount dispatches to the physical namespace counter
// contract. A missing capability is an error: the engine's global counters
// cannot answer for a database, and scanning would discard the count-store
// point-read guarantee. Transparent decorators forward here without unwrapping
// transaction, composite or other views that own different read semantics.
func NamespaceRelationshipCount(engine Engine, namespace, edgeType, label string, tier RelationshipCountTier) (int64, error) {
	counter, ok := engine.(NamespaceEdgeTypeStatsProvider)
	if !ok {
		return 0, fmt.Errorf("%w: namespace relationship counters unavailable on %T", ErrNotImplemented, engine)
	}
	switch tier {
	case RelationshipCountByType:
		return counter.EdgeCountByTypeInNamespace(namespace, edgeType)
	case RelationshipCountByStartLabel:
		return counter.EdgeCountByStartLabelInNamespace(namespace, label, edgeType)
	case RelationshipCountByEndLabel:
		return counter.EdgeCountByEndLabelInNamespace(namespace, label, edgeType)
	default:
		return 0, ErrInvalidData
	}
}

// EdgeCountByTypeInNamespace preserves the namespace relationship counter.
func (w *WALEngine) EdgeCountByTypeInNamespace(namespace, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(w.engine, namespace, edgeType, "", RelationshipCountByType)
}

// EdgeCountByStartLabelInNamespace preserves the physical start-label counter.
func (w *WALEngine) EdgeCountByStartLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(w.engine, namespace, edgeType, label, RelationshipCountByStartLabel)
}

// EdgeCountByEndLabelInNamespace preserves the physical end-label counter.
func (w *WALEngine) EdgeCountByEndLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(w.engine, namespace, edgeType, label, RelationshipCountByEndLabel)
}

// EdgeCountByTypeInNamespace preserves the namespace relationship counter.
func (n *NamespacedEngine) EdgeCountByTypeInNamespace(namespace, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(n.inner, namespace, edgeType, "", RelationshipCountByType)
}

// EdgeCountByStartLabelInNamespace preserves the physical start-label counter.
func (n *NamespacedEngine) EdgeCountByStartLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(n.inner, namespace, edgeType, label, RelationshipCountByStartLabel)
}

// EdgeCountByEndLabelInNamespace preserves the physical end-label counter.
func (n *NamespacedEngine) EdgeCountByEndLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(n.inner, namespace, edgeType, label, RelationshipCountByEndLabel)
}

// EdgeCountByTypeInNamespace preserves the namespace relationship counter.
func (t *TracedEngine) EdgeCountByTypeInNamespace(namespace, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(t.Engine, namespace, edgeType, "", RelationshipCountByType)
}

// EdgeCountByStartLabelInNamespace preserves the physical start-label counter.
func (t *TracedEngine) EdgeCountByStartLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(t.Engine, namespace, edgeType, label, RelationshipCountByStartLabel)
}

// EdgeCountByEndLabelInNamespace preserves the physical end-label counter.
func (t *TracedEngine) EdgeCountByEndLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	return NamespaceRelationshipCount(t.Engine, namespace, edgeType, label, RelationshipCountByEndLabel)
}
