package resolvers

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
)

// NodeRelationships returns relationships for a node
func (r *nodeResolver) nodeRelationships(ctx context.Context, obj *models.Node, types []string, direction *models.RelationshipDirection, limit *int) ([]models.Relationship, error) {
	edges, err := r.getEdgesForNode(ctx, obj.ID)
	if err != nil {
		return nil, err
	}

	lim := 100
	if limit != nil {
		lim = *limit
	}

	result := make([]models.Relationship, 0, len(edges))
	for _, edge := range edges {
		if len(types) > 0 {
			found := false
			for _, t := range types {
				if edge.Type == t {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if direction != nil {
			switch *direction {
			case models.RelationshipDirectionOutgoing:
				if edge.Source != obj.ID {
					continue
				}
			case models.RelationshipDirectionIncoming:
				if edge.Target != obj.ID {
					continue
				}
			}
		}

		result = append(result, *dbEdgeToModel(edge))
		if len(result) >= lim {
			break
		}
	}
	return result, nil
}

// NodeOutgoing returns outgoing relationships
func (r *nodeResolver) nodeOutgoing(ctx context.Context, obj *models.Node, types []string, limit *int) ([]models.Relationship, error) {
	dir := models.RelationshipDirectionOutgoing
	return r.nodeRelationships(ctx, obj, types, &dir, limit)
}

// NodeIncoming returns incoming relationships
func (r *nodeResolver) nodeIncoming(ctx context.Context, obj *models.Node, types []string, limit *int) ([]models.Relationship, error) {
	dir := models.RelationshipDirectionIncoming
	return r.nodeRelationships(ctx, obj, types, &dir, limit)
}

// NodeNeighbors returns neighboring nodes
func (r *nodeResolver) nodeNeighbors(ctx context.Context, obj *models.Node, direction *models.RelationshipDirection, relationshipTypes []string, labels []string, limit *int) ([]models.Node, error) {
	edges, err := r.getEdgesForNode(ctx, obj.ID)
	if err != nil {
		return nil, err
	}

	lim := 100
	if limit != nil {
		lim = *limit
	}

	seen := make(map[string]bool)
	result := make([]models.Node, 0)

	for _, edge := range edges {
		if len(relationshipTypes) > 0 {
			found := false
			for _, t := range relationshipTypes {
				if edge.Type == t {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		var neighborID string
		if edge.Source == obj.ID {
			if direction != nil && *direction == models.RelationshipDirectionIncoming {
				continue
			}
			neighborID = edge.Target
		} else {
			if direction != nil && *direction == models.RelationshipDirectionOutgoing {
				continue
			}
			neighborID = edge.Source
		}

		if seen[neighborID] {
			continue
		}
		seen[neighborID] = true

		neighbor, err := r.getNode(ctx, neighborID)
		if err != nil {
			continue
		}

		if len(labels) > 0 {
			hasLabel := false
			for _, reqLabel := range labels {
				for _, nodeLabel := range neighbor.Labels {
					if nodeLabel == reqLabel {
						hasLabel = true
						break
					}
				}
				if hasLabel {
					break
				}
			}
			if !hasLabel {
				continue
			}
		}

		result = append(result, *dbNodeToModel(neighbor))
		if len(result) >= lim {
			break
		}
	}
	return result, nil
}

// NodeSimilar finds similar nodes
func (r *nodeResolver) nodeSimilar(ctx context.Context, obj *models.Node, limit *int, threshold *float64) ([]models.SimilarNode, error) {
	lim := 10
	if limit != nil {
		lim = *limit
	}
	// threshold is ignored - FindSimilar doesn't support it

	results, err := r.DB.FindSimilar(ctx, obj.ID, lim)
	if err != nil {
		return nil, err
	}

	similar := make([]models.SimilarNode, 0, len(results))
	for _, res := range results {
		if res.Node == nil {
			continue
		}
		similar = append(similar, models.SimilarNode{
			Node:       dbNodeToModel(res.Node),
			Similarity: res.Score,
		})
	}
	return similar, nil
}
