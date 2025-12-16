package resolvers

import (
	"context"
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/graphql/models"
)

// QueryNode fetches a single node by ID
func (r *queryResolver) queryNode(ctx context.Context, id string) (*models.Node, error) {
	node, err := r.DB.GetNode(ctx, id)
	if err != nil {
		return nil, nil // Return nil for not found
	}
	return dbNodeToModel(node), nil
}

// QueryNodes fetches multiple nodes by IDs
func (r *queryResolver) queryNodes(ctx context.Context, ids []string) ([]models.Node, error) {
	result := make([]models.Node, 0, len(ids))
	for _, id := range ids {
		node, err := r.DB.GetNode(ctx, id)
		if err != nil {
			continue
		}
		result = append(result, *dbNodeToModel(node))
	}
	return result, nil
}

// QueryAllNodes fetches all nodes with optional label filter
func (r *queryResolver) queryAllNodes(ctx context.Context, labels []string, limit *int, offset *int) ([]models.Node, error) {
	lim := 100
	off := 0
	if limit != nil {
		lim = *limit
	}
	if offset != nil {
		off = *offset
	}

	label := ""
	if len(labels) > 0 {
		label = labels[0]
	}

	nodes, err := r.DB.ListNodes(ctx, label, lim+off, 0)
	if err != nil {
		return nil, err
	}

	start := off
	end := off + lim
	if start > len(nodes) {
		start = len(nodes)
	}
	if end > len(nodes) {
		end = len(nodes)
	}

	result := make([]models.Node, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, *dbNodeToModel(nodes[i]))
	}
	return result, nil
}

// QueryNodesByLabel fetches nodes by label
func (r *queryResolver) queryNodesByLabel(ctx context.Context, label string, limit *int, offset *int) ([]models.Node, error) {
	return r.queryAllNodes(ctx, []string{label}, limit, offset)
}

// QueryNodeCount returns the count of nodes
func (r *queryResolver) queryNodeCount(ctx context.Context, label *string) (int, error) {
	if label != nil {
		nodes, err := r.DB.ListNodes(ctx, *label, 10000, 0)
		if err != nil {
			return 0, err
		}
		return len(nodes), nil
	}
	stats := r.DB.Stats()
	return int(stats.NodeCount), nil
}

// QueryRelationship fetches a single relationship by ID
func (r *queryResolver) queryRelationship(ctx context.Context, id string) (*models.Relationship, error) {
	edge, err := r.DB.GetEdge(ctx, id)
	if err != nil {
		return nil, nil
	}
	return dbEdgeToModel(edge), nil
}

// QueryAllRelationships fetches all relationships with optional type filter
func (r *queryResolver) queryAllRelationships(ctx context.Context, types []string, limit *int, offset *int) ([]models.Relationship, error) {
	lim := 100
	off := 0
	if limit != nil {
		lim = *limit
	}
	if offset != nil {
		off = *offset
	}

	relType := ""
	if len(types) > 0 {
		relType = types[0]
	}

	edges, err := r.DB.ListEdges(ctx, relType, lim+off, 0)
	if err != nil {
		return nil, err
	}

	start := off
	end := off + lim
	if start > len(edges) {
		start = len(edges)
	}
	if end > len(edges) {
		end = len(edges)
	}

	result := make([]models.Relationship, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, *dbEdgeToModel(edges[i]))
	}
	return result, nil
}

// QueryRelationshipsByType fetches relationships by type
func (r *queryResolver) queryRelationshipsByType(ctx context.Context, typeArg string, limit *int, offset *int) ([]models.Relationship, error) {
	return r.queryAllRelationships(ctx, []string{typeArg}, limit, offset)
}

// QueryRelationshipsBetween fetches relationships between two nodes
func (r *queryResolver) queryRelationshipsBetween(ctx context.Context, startNodeID string, endNodeID string) ([]models.Relationship, error) {
	edges, err := r.DB.GetEdgesForNode(ctx, startNodeID)
	if err != nil {
		return nil, err
	}

	result := make([]models.Relationship, 0)
	for _, edge := range edges {
		if edge.Target == endNodeID || edge.Source == endNodeID {
			result = append(result, *dbEdgeToModel(edge))
		}
	}
	return result, nil
}

// QueryRelationshipCount returns the count of relationships
func (r *queryResolver) queryRelationshipCount(ctx context.Context, typeArg *string) (int, error) {
	if typeArg != nil {
		edges, err := r.DB.ListEdges(ctx, *typeArg, 10000, 0)
		if err != nil {
			return 0, err
		}
		return len(edges), nil
	}
	stats := r.DB.Stats()
	return int(stats.EdgeCount), nil
}

// QuerySearch performs a search query
func (r *queryResolver) querySearch(ctx context.Context, query string, options *models.SearchOptions) (*models.SearchResponse, error) {
	start := time.Now()

	limit := 10
	if options != nil && options.Limit != nil {
		limit = *options.Limit
	}

	var labels []string
	if options != nil && len(options.Labels) > 0 {
		labels = options.Labels
	}

	results, err := r.DB.Search(ctx, query, labels, limit)
	if err != nil {
		return nil, err
	}

	searchResults := make([]*models.SearchResult, 0, len(results))
	for _, res := range results {
		rrfScore := res.RRFScore
		vectorRank := res.VectorRank
		bm25Rank := res.BM25Rank
		foundBy := []string{}
		if vectorRank > 0 {
			foundBy = append(foundBy, "VECTOR")
		}
		if bm25Rank > 0 {
			foundBy = append(foundBy, "BM25")
		}
		searchResults = append(searchResults, &models.SearchResult{
			Node:       dbNodeToModel(res.Node),
			Score:      res.Score,
			RRFScore:   &rrfScore,
			VectorRank: &vectorRank,
			BM25Rank:   &bm25Rank,
			FoundBy:    foundBy,
		})
	}

	return &models.SearchResponse{
		Results:         searchResults,
		TotalCount:      len(searchResults),
		ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
	}, nil
}

// QuerySimilar finds similar nodes
func (r *queryResolver) querySimilar(ctx context.Context, nodeID string, limit *int, threshold *float64) ([]models.SimilarNode, error) {
	lim := 10
	if limit != nil {
		lim = *limit
	}
	// threshold is ignored - FindSimilar doesn't support it

	results, err := r.DB.FindSimilar(ctx, nodeID, lim)
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

// QuerySearchByProperty searches nodes by property
func (r *queryResolver) querySearchByProperty(ctx context.Context, key string, value models.JSON, labels []string, limit *int) ([]models.Node, error) {
	cypher := fmt.Sprintf("MATCH (n) WHERE n.%s = $value RETURN n LIMIT $limit", key)
	if len(labels) > 0 {
		cypher = fmt.Sprintf("MATCH (n:%s) WHERE n.%s = $value RETURN n LIMIT $limit", labels[0], key)
	}

	lim := 100
	if limit != nil {
		lim = *limit
	}

	result, err := r.DB.ExecuteCypher(ctx, cypher, map[string]interface{}{
		"value": value,
		"limit": lim,
	})
	if err != nil {
		return nil, err
	}

	nodes := make([]models.Node, 0)
	for _, row := range result.Rows {
		if len(row) > 0 {
			if nodeMap, ok := row[0].(map[string]interface{}); ok {
				if id, ok := nodeMap["id"].(string); ok {
					node, err := r.DB.GetNode(ctx, id)
					if err == nil {
						nodes = append(nodes, *dbNodeToModel(node))
					}
				}
			}
		}
	}
	return nodes, nil
}

// QueryCypher executes a Cypher query
func (r *queryResolver) queryCypher(ctx context.Context, input models.CypherInput) (*models.CypherResult, error) {
	params := make(map[string]interface{})
	for k, v := range input.Parameters {
		params[k] = v
	}

	result, err := r.DB.ExecuteCypher(ctx, input.Statement, params)
	if err != nil {
		return nil, err
	}

	return &models.CypherResult{
		Columns:  result.Columns,
		Rows:     result.Rows,
		RowCount: len(result.Rows),
	}, nil
}

// QueryStats returns database statistics
func (r *queryResolver) queryStats(ctx context.Context) (*models.DatabaseStats, error) {
	stats := r.DB.Stats()

	labels, _ := r.DB.GetLabels(ctx)
	labelStats := make([]*models.LabelStats, 0, len(labels))
	for _, label := range labels {
		// Use Cypher COUNT for accurate label counts
		result, err := r.DB.ExecuteCypher(ctx, fmt.Sprintf("MATCH (n:%s) RETURN count(n) as cnt", label), nil)
		count := 0
		if err == nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			if cnt, ok := result.Rows[0][0].(int64); ok {
				count = int(cnt)
			} else if cnt, ok := result.Rows[0][0].(int); ok {
				count = cnt
			}
		}
		labelStats = append(labelStats, &models.LabelStats{Label: label, Count: count})
	}

	relTypes, _ := r.DB.GetRelationshipTypes(ctx)
	typeStats := make([]*models.RelationshipTypeStats, 0, len(relTypes))
	for _, typ := range relTypes {
		// Use Cypher COUNT for accurate relationship counts
		result, err := r.DB.ExecuteCypher(ctx, fmt.Sprintf("MATCH ()-[r:%s]->() RETURN count(r) as cnt", typ), nil)
		count := 0
		if err == nil && len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
			if cnt, ok := result.Rows[0][0].(int64); ok {
				count = int(cnt)
			} else if cnt, ok := result.Rows[0][0].(int); ok {
				count = cnt
			}
		}
		typeStats = append(typeStats, &models.RelationshipTypeStats{Type: typ, Count: count})
	}

	searchStats := r.DB.GetSearchStats()
	embeddedCount := 0
	if searchStats != nil {
		embeddedCount = searchStats.EmbeddingCount
	}

	return &models.DatabaseStats{
		NodeCount:         int(stats.NodeCount),
		RelationshipCount: int(stats.EdgeCount),
		Labels:            labelStats,
		RelationshipTypes: typeStats,
		EmbeddedNodeCount: embeddedCount,
		UptimeSeconds:     time.Since(r.StartTime).Seconds(),
	}, nil
}

// QuerySchema returns the graph schema
func (r *queryResolver) querySchema(ctx context.Context) (*models.GraphSchema, error) {
	labels, _ := r.DB.GetLabels(ctx)
	relTypes, _ := r.DB.GetRelationshipTypes(ctx)

	return &models.GraphSchema{
		NodeLabels:               labels,
		RelationshipTypes:        relTypes,
		NodePropertyKeys:         []string{},
		RelationshipPropertyKeys: []string{},
		Constraints:              []*models.SchemaConstraint{},
	}, nil
}

// QueryLabels returns all labels
func (r *queryResolver) queryLabels(ctx context.Context) ([]string, error) {
	return r.DB.GetLabels(ctx)
}

// QueryRelationshipTypes returns all relationship types
func (r *queryResolver) queryRelationshipTypes(ctx context.Context) ([]string, error) {
	return r.DB.GetRelationshipTypes(ctx)
}

// QueryShortestPath finds shortest path between nodes
func (r *queryResolver) queryShortestPath(ctx context.Context, startNodeID string, endNodeID string, maxDepth *int, relationshipTypes []string) ([]models.Node, error) {
	depth := 10
	if maxDepth != nil {
		depth = *maxDepth
	}

	visited := make(map[string]string)
	queue := []string{startNodeID}
	visited[startNodeID] = ""

	for len(queue) > 0 && depth > 0 {
		depth--
		nextQueue := []string{}

		for _, nodeID := range queue {
			if nodeID == endNodeID {
				path := []models.Node{}
				current := endNodeID
				for current != "" {
					node, err := r.DB.GetNode(ctx, current)
					if err != nil {
						break
					}
					path = append([]models.Node{*dbNodeToModel(node)}, path...)
					current = visited[current]
				}
				return path, nil
			}

			edges, err := r.DB.GetEdgesForNode(ctx, nodeID)
			if err != nil {
				continue
			}

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
				if edge.Source == nodeID {
					neighborID = edge.Target
				} else {
					neighborID = edge.Source
				}

				if _, seen := visited[neighborID]; !seen {
					visited[neighborID] = nodeID
					nextQueue = append(nextQueue, neighborID)
				}
			}
		}
		queue = nextQueue
	}

	return []models.Node{}, nil
}

// QueryAllPaths finds all paths between nodes
func (r *queryResolver) queryAllPaths(ctx context.Context, startNodeID string, endNodeID string, maxDepth *int, limit *int) ([][]models.Node, error) {
	path, err := r.queryShortestPath(ctx, startNodeID, endNodeID, maxDepth, nil)
	if err != nil {
		return nil, err
	}
	if len(path) == 0 {
		return [][]models.Node{}, nil
	}
	return [][]models.Node{path}, nil
}

// QueryNeighborhood returns the neighborhood subgraph
func (r *queryResolver) queryNeighborhood(ctx context.Context, nodeID string, depth *int, relationshipTypes []string, labels []string, limit *int) (*models.Subgraph, error) {
	maxDepth := 2
	if depth != nil {
		maxDepth = *depth
	}

	lim := 100
	if limit != nil {
		lim = *limit
	}

	visited := make(map[string]bool)
	seenEdges := make(map[string]bool)
	collectedNodes := make([]*models.Node, 0)
	collectedEdges := make([]*models.Relationship, 0)

	queue := []string{nodeID}

	for d := 0; d <= maxDepth && len(queue) > 0; d++ {
		nextQueue := []string{}

		for _, nid := range queue {
			if visited[nid] || len(collectedNodes) >= lim {
				continue
			}
			visited[nid] = true

			node, err := r.DB.GetNode(ctx, nid)
			if err != nil {
				continue
			}

			if len(labels) > 0 && nid != nodeID {
				hasLabel := false
				for _, reqLabel := range labels {
					for _, nodeLabel := range node.Labels {
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

			collectedNodes = append(collectedNodes, dbNodeToModel(node))

			edges, err := r.DB.GetEdgesForNode(ctx, nid)
			if err != nil {
				continue
			}

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

				if !seenEdges[edge.ID] {
					seenEdges[edge.ID] = true
					collectedEdges = append(collectedEdges, dbEdgeToModel(edge))
				}

				var neighborID string
				if edge.Source == nid {
					neighborID = edge.Target
				} else {
					neighborID = edge.Source
				}
				if !visited[neighborID] {
					nextQueue = append(nextQueue, neighborID)
				}
			}
		}
		queue = nextQueue
	}

	return &models.Subgraph{
		Nodes:         collectedNodes,
		Relationships: collectedEdges,
	}, nil
}
