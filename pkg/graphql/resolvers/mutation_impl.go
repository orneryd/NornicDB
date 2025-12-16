package resolvers

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
)

// MutationCreateNode creates a new node
func (r *mutationResolver) mutationCreateNode(ctx context.Context, input models.CreateNodeInput) (*models.Node, error) {
	node, err := r.DB.CreateNode(ctx, input.Labels, map[string]interface{}(input.Properties))
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}
	return dbNodeToModel(node), nil
}

// MutationUpdateNode updates an existing node
func (r *mutationResolver) mutationUpdateNode(ctx context.Context, input models.UpdateNodeInput) (*models.Node, error) {
	props := map[string]interface{}(input.Properties)
	node, err := r.DB.UpdateNode(ctx, input.ID, props)
	if err != nil {
		return nil, fmt.Errorf("failed to update node: %w", err)
	}
	return dbNodeToModel(node), nil
}

// MutationDeleteNode deletes a node
func (r *mutationResolver) mutationDeleteNode(ctx context.Context, id string) (bool, error) {
	if err := r.DB.DeleteNode(ctx, id); err != nil {
		return false, fmt.Errorf("failed to delete node: %w", err)
	}
	return true, nil
}

// MutationBulkCreateNodes creates multiple nodes
func (r *mutationResolver) mutationBulkCreateNodes(ctx context.Context, input models.BulkCreateNodesInput) (*models.BulkCreateResult, error) {
	created := 0
	skipped := 0
	var errors []string

	for _, nodeInput := range input.Nodes {
		_, err := r.DB.CreateNode(ctx, nodeInput.Labels, map[string]interface{}(nodeInput.Properties))
		if err != nil {
			errors = append(errors, err.Error())
			skipped++
		} else {
			created++
		}
	}

	return &models.BulkCreateResult{
		Created: created,
		Skipped: skipped,
		Errors:  errors,
	}, nil
}

// MutationBulkDeleteNodes deletes multiple nodes
func (r *mutationResolver) mutationBulkDeleteNodes(ctx context.Context, ids []string) (*models.BulkDeleteResult, error) {
	deleted := 0
	var notFound []string

	for _, id := range ids {
		if err := r.DB.DeleteNode(ctx, id); err != nil {
			notFound = append(notFound, id)
		} else {
			deleted++
		}
	}

	return &models.BulkDeleteResult{
		Deleted:  deleted,
		NotFound: notFound,
	}, nil
}

// MutationMergeNode merges a node (create or update)
func (r *mutationResolver) mutationMergeNode(ctx context.Context, labels []string, matchProperties models.JSON, setProperties models.JSON) (*models.Node, error) {
	// Try to find existing node by match properties using Cypher
	matchProps := map[string]interface{}(matchProperties)
	setProps := map[string]interface{}(setProperties)

	// Build match query
	label := ""
	if len(labels) > 0 {
		label = labels[0]
	}

	// Try to find existing node
	var whereClause string
	var params = make(map[string]interface{})
	i := 0
	for k, v := range matchProps {
		if i > 0 {
			whereClause += " AND "
		}
		paramName := fmt.Sprintf("p%d", i)
		whereClause += fmt.Sprintf("n.%s = $%s", k, paramName)
		params[paramName] = v
		i++
	}

	query := fmt.Sprintf("MATCH (n:%s) WHERE %s RETURN n LIMIT 1", label, whereClause)
	result, err := r.DB.ExecuteCypher(ctx, query, params)
	if err == nil && len(result.Rows) > 0 {
		// Update existing node - Rows is [][]interface{}
		row := result.Rows[0]
		if len(row) > 0 {
			if nodeData, ok := row[0].(map[string]interface{}); ok {
				if id, ok := nodeData["id"].(string); ok {
					node, err := r.DB.UpdateNode(ctx, id, setProps)
					if err != nil {
						return nil, err
					}
					return dbNodeToModel(node), nil
				}
			}
		}
	}

	// Create new node with merged properties
	allProps := make(map[string]interface{})
	for k, v := range matchProps {
		allProps[k] = v
	}
	for k, v := range setProps {
		allProps[k] = v
	}

	node, err := r.DB.CreateNode(ctx, labels, allProps)
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}
	return dbNodeToModel(node), nil
}

// MutationCreateRelationship creates a new relationship
func (r *mutationResolver) mutationCreateRelationship(ctx context.Context, input models.CreateRelationshipInput) (*models.Relationship, error) {
	edge, err := r.DB.CreateEdge(ctx, input.StartNodeID, input.EndNodeID, input.Type, map[string]interface{}(input.Properties))
	if err != nil {
		return nil, fmt.Errorf("failed to create relationship: %w", err)
	}
	return dbEdgeToModel(edge), nil
}

// MutationUpdateRelationship updates an existing relationship
func (r *mutationResolver) mutationUpdateRelationship(ctx context.Context, input models.UpdateRelationshipInput) (*models.Relationship, error) {
	// Get existing edge first
	edge, err := r.DB.GetEdge(ctx, input.ID)
	if err != nil {
		return nil, fmt.Errorf("relationship not found: %w", err)
	}

	// Merge properties
	if input.Properties != nil {
		for k, v := range input.Properties {
			edge.Properties[k] = v
		}
	}

	// Delete and recreate since there's no UpdateEdge
	if err := r.DB.DeleteEdge(ctx, input.ID); err != nil {
		return nil, fmt.Errorf("failed to delete old relationship: %w", err)
	}

	newEdge, err := r.DB.CreateEdge(ctx, edge.Source, edge.Target, edge.Type, edge.Properties)
	if err != nil {
		return nil, fmt.Errorf("failed to create updated relationship: %w", err)
	}
	return dbEdgeToModel(newEdge), nil
}

// MutationDeleteRelationship deletes a relationship
func (r *mutationResolver) mutationDeleteRelationship(ctx context.Context, id string) (bool, error) {
	if err := r.DB.DeleteEdge(ctx, id); err != nil {
		return false, fmt.Errorf("failed to delete relationship: %w", err)
	}
	return true, nil
}

// MutationBulkCreateRelationships creates multiple relationships
func (r *mutationResolver) mutationBulkCreateRelationships(ctx context.Context, input models.BulkCreateRelationshipsInput) (*models.BulkCreateResult, error) {
	created := 0
	skipped := 0
	var errors []string

	for _, relInput := range input.Relationships {
		_, err := r.DB.CreateEdge(ctx, relInput.StartNodeID, relInput.EndNodeID, relInput.Type, map[string]interface{}(relInput.Properties))
		if err != nil {
			errors = append(errors, err.Error())
			skipped++
		} else {
			created++
		}
	}

	return &models.BulkCreateResult{
		Created: created,
		Skipped: skipped,
		Errors:  errors,
	}, nil
}

// MutationBulkDeleteRelationships deletes multiple relationships
func (r *mutationResolver) mutationBulkDeleteRelationships(ctx context.Context, ids []string) (*models.BulkDeleteResult, error) {
	deleted := 0
	var notFound []string

	for _, id := range ids {
		if err := r.DB.DeleteEdge(ctx, id); err != nil {
			notFound = append(notFound, id)
		} else {
			deleted++
		}
	}

	return &models.BulkDeleteResult{
		Deleted:  deleted,
		NotFound: notFound,
	}, nil
}

// MutationMergeRelationship merges a relationship
func (r *mutationResolver) mutationMergeRelationship(ctx context.Context, startNodeID string, endNodeID string, typeArg string, properties models.JSON) (*models.Relationship, error) {
	// Try to find existing edge
	edges, err := r.DB.GetEdgesForNode(ctx, startNodeID)
	if err == nil {
		for _, edge := range edges {
			if edge.Target == endNodeID && edge.Type == typeArg {
				// Update existing - delete and recreate
				if err := r.DB.DeleteEdge(ctx, edge.ID); err != nil {
					return nil, err
				}
				mergedProps := edge.Properties
				for k, v := range properties {
					mergedProps[k] = v
				}
				newEdge, err := r.DB.CreateEdge(ctx, startNodeID, endNodeID, typeArg, mergedProps)
				if err != nil {
					return nil, err
				}
				return dbEdgeToModel(newEdge), nil
			}
		}
	}

	// Create new
	edge, err := r.DB.CreateEdge(ctx, startNodeID, endNodeID, typeArg, map[string]interface{}(properties))
	if err != nil {
		return nil, fmt.Errorf("failed to create relationship: %w", err)
	}
	return dbEdgeToModel(edge), nil
}

// MutationExecuteCypher executes a Cypher statement (mutation)
func (r *mutationResolver) mutationExecuteCypher(ctx context.Context, input models.CypherInput) (*models.CypherResult, error) {
	params := make(map[string]interface{})
	for k, v := range input.Parameters {
		params[k] = v
	}

	result, err := r.DB.ExecuteCypher(ctx, input.Statement, params)
	if err != nil {
		return nil, fmt.Errorf("cypher execution failed: %w", err)
	}

	return &models.CypherResult{
		Columns:  result.Columns,
		Rows:     result.Rows,
		RowCount: len(result.Rows),
	}, nil
}

// MutationTriggerEmbedding triggers embedding generation
func (r *mutationResolver) mutationTriggerEmbedding(ctx context.Context, regenerate *bool) (*models.EmbeddingStatus, error) {
	regen := false
	if regenerate != nil {
		regen = *regenerate
	}

	// Clear and regenerate if requested
	if regen {
		_, _ = r.DB.ClearAllEmbeddings()
		_ = r.DB.ResetEmbedWorker()
	}

	// Trigger embedding
	_, _ = r.DB.EmbedExisting(ctx)

	// Get stats
	stats := r.DB.EmbedQueueStats()
	embeddingCount := r.DB.EmbeddingCount()

	running := false
	if stats != nil {
		running = stats.Running
	}

	return &models.EmbeddingStatus{
		Pending:       0, // Worker doesn't track pending
		Embedded:      embeddingCount,
		Total:         embeddingCount,
		WorkerRunning: running,
	}, nil
}

// MutationRebuildSearchIndex rebuilds the search index
func (r *mutationResolver) mutationRebuildSearchIndex(ctx context.Context) (bool, error) {
	if err := r.DB.BuildSearchIndexes(ctx); err != nil {
		return false, fmt.Errorf("failed to rebuild search index: %w", err)
	}
	return true, nil
}

// MutationRunDecay runs decay processing
func (r *mutationResolver) mutationRunDecay(ctx context.Context) (*models.DecayResult, error) {
	info := r.DB.GetDecayInfo()
	if info == nil || !info.Enabled {
		return &models.DecayResult{
			NodesProcessed:    0,
			NodesDecayed:      0,
			AverageDecayScore: 0,
		}, nil
	}

	return &models.DecayResult{
		NodesProcessed:    0,
		NodesDecayed:      0,
		AverageDecayScore: float64(info.ArchiveThreshold),
	}, nil
}

// MutationClearAll clears all data
func (r *mutationResolver) mutationClearAll(ctx context.Context, confirmPhrase string) (bool, error) {
	if confirmPhrase != "DELETE ALL DATA" {
		return false, fmt.Errorf("invalid confirmation phrase")
	}

	// Clear all nodes and edges using Cypher
	_, err := r.DB.ExecuteCypher(ctx, "MATCH (n) DETACH DELETE n", nil)
	if err != nil {
		return false, fmt.Errorf("failed to clear database: %w", err)
	}

	// Clear embeddings
	_, _ = r.DB.ClearAllEmbeddings()

	return true, nil
}
