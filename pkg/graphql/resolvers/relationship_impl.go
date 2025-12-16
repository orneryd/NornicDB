package resolvers

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
)

// RelationshipStartNode returns the start node
func (r *relationshipResolver) relationshipStartNode(ctx context.Context, obj *models.Relationship) (*models.Node, error) {
	node, err := r.DB.GetNode(ctx, obj.StartNodeID)
	if err != nil {
		return nil, err
	}
	return dbNodeToModel(node), nil
}

// RelationshipEndNode returns the end node
func (r *relationshipResolver) relationshipEndNode(ctx context.Context, obj *models.Relationship) (*models.Node, error) {
	node, err := r.DB.GetNode(ctx, obj.EndNodeID)
	if err != nil {
		return nil, err
	}
	return dbNodeToModel(node), nil
}
