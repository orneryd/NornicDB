package replication

import "context"

import "github.com/orneryd/nornicdb/pkg/storage"

var _ storage.NamespaceEdgeTypeStatsProvider = (*ReplicatedEngine)(nil)

// EdgeCountByTypeInNamespace preserves the namespace relationship counter.
func (r *ReplicatedEngine) EdgeCountByTypeInNamespace(ctx context.Context, namespace, edgeType string) (int64, error) {
	return storage.NamespaceRelationshipCount(ctx, r.Engine, namespace, edgeType, "", storage.RelationshipCountByType)
}

// EdgeCountByStartLabelInNamespace preserves the physical start-label counter.
func (r *ReplicatedEngine) EdgeCountByStartLabelInNamespace(ctx context.Context, namespace, label, edgeType string) (int64, error) {
	return storage.NamespaceRelationshipCount(ctx, r.Engine, namespace, edgeType, label, storage.RelationshipCountByStartLabel)
}

// EdgeCountByEndLabelInNamespace preserves the physical end-label counter.
func (r *ReplicatedEngine) EdgeCountByEndLabelInNamespace(ctx context.Context, namespace, label, edgeType string) (int64, error) {
	return storage.NamespaceRelationshipCount(ctx, r.Engine, namespace, edgeType, label, storage.RelationshipCountByEndLabel)
}
