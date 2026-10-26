package multidb

import "github.com/orneryd/nornicdb/pkg/storage"

var _ storage.NamespaceEdgeTypeStatsProvider = (*sizeTrackingEngine)(nil)

// EdgeCountByTypeInNamespace preserves the namespace relationship counter.
func (s *sizeTrackingEngine) EdgeCountByTypeInNamespace(namespace, edgeType string) (int64, error) {
	return storage.NamespaceRelationshipCount(s.Engine, namespace, edgeType, "", storage.RelationshipCountByType)
}

// EdgeCountByStartLabelInNamespace preserves the physical start-label counter.
func (s *sizeTrackingEngine) EdgeCountByStartLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	return storage.NamespaceRelationshipCount(s.Engine, namespace, edgeType, label, storage.RelationshipCountByStartLabel)
}

// EdgeCountByEndLabelInNamespace preserves the physical end-label counter.
func (s *sizeTrackingEngine) EdgeCountByEndLabelInNamespace(namespace, label, edgeType string) (int64, error) {
	return storage.NamespaceRelationshipCount(s.Engine, namespace, edgeType, label, storage.RelationshipCountByEndLabel)
}
