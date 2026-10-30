package storage

import "github.com/orneryd/nornicdb/pkg/knowledgepolicy"

// GetEntityMeta returns the metadata needed by the access flusher to resolve
// ON ACCESS policies and property visibility for either nodes or edges.
func (b *BadgerEngine) GetEntityMeta(entityID string) (knowledgepolicy.EntityMeta, error) {
	if node, err := b.GetNode(NodeID(entityID)); err == nil && node != nil {
		propKeys := make([]string, 0, len(node.Properties))
		for key := range node.Properties {
			propKeys = append(propKeys, key)
		}
		createdAt := node.CreatedAt.UnixNano()
		versionAt := createdAt
		if !node.UpdatedAt.IsZero() {
			versionAt = node.UpdatedAt.UnixNano()
		}
		return knowledgepolicy.EntityMeta{
			Scope:          knowledgepolicy.ScopeNode,
			Labels:         append([]string(nil), node.Labels...),
			PropertyKeys:   propKeys,
			CreatedAtNanos: createdAt,
			VersionAtNanos: versionAt,
		}, nil
	}

	edge, err := b.GetEdge(EdgeID(entityID))
	if err != nil {
		return knowledgepolicy.EntityMeta{}, err
	}
	propKeys := make([]string, 0, len(edge.Properties))
	for key := range edge.Properties {
		propKeys = append(propKeys, key)
	}
	createdAt := edge.CreatedAt.UnixNano()
	versionAt := createdAt
	if !edge.UpdatedAt.IsZero() {
		versionAt = edge.UpdatedAt.UnixNano()
	}
	return knowledgepolicy.EntityMeta{
		Scope:          knowledgepolicy.ScopeEdge,
		EdgeType:       edge.Type,
		PropertyKeys:   propKeys,
		CreatedAtNanos: createdAt,
		VersionAtNanos: versionAt,
	}, nil
}
