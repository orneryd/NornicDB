package storage

import (
	"context"
	"errors"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// GetEntityMeta returns the metadata needed by the access flusher to resolve
// ON ACCESS policies and property visibility for either nodes or edges.
func (b *BadgerEngine) GetEntityMeta(ctx context.Context, entityID string) (knowledgepolicy.EntityMeta, error) {
	node, err := b.GetStoredNode(ctx, NodeID(entityID))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return knowledgepolicy.EntityMeta{}, err
	}
	if err == nil && node != nil {
		propKeys := make([]string, 0, len(node.Properties))
		for key := range node.Properties {
			if err := ctx.Err(); err != nil {
				return knowledgepolicy.EntityMeta{}, err
			}
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

	edge, err := b.GetStoredEdge(ctx, EdgeID(entityID))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return knowledgepolicy.EntityMeta{}, errors.Join(knowledgepolicy.ErrEntityMetaNotFound, err)
		}
		return knowledgepolicy.EntityMeta{}, err
	}
	propKeys := make([]string, 0, len(edge.Properties))
	for key := range edge.Properties {
		if err := ctx.Err(); err != nil {
			return knowledgepolicy.EntityMeta{}, err
		}
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
