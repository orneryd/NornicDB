package resolvers

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// Helper methods that route all operations through namespaced storage when dbManager is set.
// These ensure all nodes/edges are created in the correct database namespace.

// createNode creates a node using namespaced storage.
func (r *Resolver) createNode(ctx context.Context, labels []string, properties map[string]interface{}) (*nornicdb.Node, error) {
	return r.createNodeViaCypher(ctx, labels, properties)
}

// updateNode updates a node using namespaced storage.
func (r *Resolver) updateNode(ctx context.Context, id string, properties map[string]interface{}) (*nornicdb.Node, error) {
	return r.updateNodeViaCypher(ctx, id, properties)
}

// deleteNode deletes a node using namespaced storage.
func (r *Resolver) deleteNode(ctx context.Context, id string) error {
	return r.deleteNodeViaCypher(ctx, id)
}

// getNode gets a node using namespaced storage.
func (r *Resolver) getNode(ctx context.Context, id string) (*nornicdb.Node, error) {
	return r.getNodeViaCypher(ctx, id)
}

// listNodes lists nodes using namespaced storage.
func (r *Resolver) listNodes(ctx context.Context, label string, limit, offset int) ([]*nornicdb.Node, error) {
	return r.listNodesViaCypher(ctx, label, limit, offset)
}

// createEdge creates an edge using namespaced storage.
func (r *Resolver) createEdge(ctx context.Context, source, target, edgeType string, properties map[string]interface{}) (*nornicdb.GraphEdge, error) {
	return r.createEdgeViaCypher(ctx, source, target, edgeType, properties)
}

// deleteEdge deletes an edge using namespaced storage.
func (r *Resolver) deleteEdge(ctx context.Context, id string) error {
	return r.deleteEdgeViaCypher(ctx, id)
}

// getEdge gets an edge using namespaced storage.
func (r *Resolver) getEdge(ctx context.Context, id string) (*nornicdb.GraphEdge, error) {
	return r.getEdgeViaCypher(ctx, id)
}

// getEdgesForNode gets edges for a node using namespaced storage.
func (r *Resolver) getEdgesForNode(ctx context.Context, nodeID string) ([]*nornicdb.GraphEdge, error) {
	return r.getEdgesForNodeViaCypher(ctx, nodeID)
}

// listEdges lists edges using namespaced storage.
func (r *Resolver) listEdges(ctx context.Context, relType string, limit, offset int) ([]*nornicdb.GraphEdge, error) {
	return r.listEdgesViaCypher(ctx, relType, limit, offset)
}

// getLabels gets labels using namespaced storage.
func (r *Resolver) getLabels(ctx context.Context) ([]string, error) {
	return r.getLabelsViaCypher(ctx)
}

// getRelationshipTypes gets relationship types using namespaced storage.
func (r *Resolver) getRelationshipTypes(ctx context.Context) ([]string, error) {
	return r.getRelationshipTypesViaCypher(ctx)
}

// stats gets stats using namespaced storage.
func (r *Resolver) stats() nornicdb.DBStats {
	return r.statsViaCypher()
}

// Cypher-based implementations for namespaced storage

func (r *Resolver) createNodeViaCypher(ctx context.Context, labels []string, properties map[string]interface{}) (*nornicdb.Node, error) {
	labelsStr := buildLabelsString(labels)
	propsStr, params := buildPropertiesString(properties)

	query := fmt.Sprintf("CREATE (n%s %s) RETURN n", labelsStr, propsStr)
	result, err := r.executeCypher(ctx, query, params, "")
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return nil, fmt.Errorf("failed to create node")
	}

	return extractNodeFromResult(result.Rows[0][0])
}

func (r *Resolver) updateNodeViaCypher(ctx context.Context, id string, properties map[string]interface{}) (*nornicdb.Node, error) {
	// Build SET clause
	setParts := make([]string, 0, len(properties))
	params := make(map[string]interface{})
	params["nodeId"] = id
	i := 0
	for k, v := range properties {
		paramName := fmt.Sprintf("p%d", i)
		setParts = append(setParts, fmt.Sprintf("n.%s = $%s", k, paramName))
		params[paramName] = v
		i++
	}

	if len(setParts) == 0 {
		// No properties to update, just return the node
		return r.getNodeViaCypher(ctx, id)
	}

	setClause := strings.Join(setParts, ", ")
	// Use id(n) function for matching internal storage ID, or fallback to n.id property
	query := fmt.Sprintf("MATCH (n) WHERE id(n) = $nodeId OR n.id = $nodeId SET %s RETURN n", setClause)
	result, err := r.executeCypher(ctx, query, params, "")
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return nil, fmt.Errorf("node not found")
	}

	return extractNodeFromResult(result.Rows[0][0])
}

func (r *Resolver) deleteNodeViaCypher(ctx context.Context, id string) error {
	// First check if node exists
	node, err := r.getNodeViaCypher(ctx, id)
	if err != nil {
		return err // Return error if node not found
	}
	if node == nil {
		return fmt.Errorf("node not found")
	}

	// Use id(n) function for matching internal storage ID, or fallback to n.id property
	query := "MATCH (n) WHERE id(n) = $id OR n.id = $id DETACH DELETE n"
	params := map[string]interface{}{"id": id}
	_, err = r.executeCypher(ctx, query, params, "")
	return err
}

func (r *Resolver) getNodeViaCypher(ctx context.Context, id string) (*nornicdb.Node, error) {
	// Use id(n) function for matching internal storage ID, or fallback to n.id property
	query := "MATCH (n) WHERE id(n) = $id OR n.id = $id RETURN n LIMIT 1"
	params := map[string]interface{}{"id": id}
	result, err := r.executeCypher(ctx, query, params, "")
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return nil, fmt.Errorf("node not found")
	}

	return extractNodeFromResult(result.Rows[0][0])
}

func (r *Resolver) listNodesViaCypher(ctx context.Context, label string, limit, offset int) ([]*nornicdb.Node, error) {
	labelClause := ""
	if label != "" {
		labelClause = ":" + label
	}

	// Use id(n) function for ordering (more reliable than user id property)
	query := fmt.Sprintf("MATCH (n%s) RETURN n ORDER BY id(n) SKIP $offset LIMIT $limit", labelClause)
	params := map[string]interface{}{
		"offset": offset,
		"limit":  limit,
	}

	result, err := r.executeCypher(ctx, query, params, "")
	if err != nil {
		return nil, err
	}

	nodes := make([]*nornicdb.Node, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			if node, err := extractNodeFromResult(row[0]); err == nil {
				nodes = append(nodes, node)
			}
		}
	}

	return nodes, nil
}

func (r *Resolver) createEdgeViaCypher(ctx context.Context, source, target, edgeType string, properties map[string]interface{}) (*nornicdb.GraphEdge, error) {
	propsStr, params := buildPropertiesString(properties)
	params["source"] = source
	params["target"] = target

	// Use id(n) function for matching internal storage ID, or fallback to n.id property
	// Note: id() returns unprefixed IDs when using NamespacedEngine, so source/target should be unprefixed
	query := fmt.Sprintf("MATCH (a), (b) WHERE (id(a) = $source OR a.id = $source) AND (id(b) = $target OR b.id = $target) CREATE (a)-[r:%s %s]->(b) RETURN r, id(a) as source, id(b) as target", edgeType, propsStr)
	result, err := r.executeCypher(ctx, query, params, "")
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		// Try to get more info about why the match failed
		// Check if nodes exist
		checkQuery := "MATCH (n) WHERE id(n) = $source OR n.id = $source RETURN count(n) as cnt"
		checkResult, _ := r.executeCypher(ctx, checkQuery, map[string]interface{}{"source": source}, "")
		sourceExists := false
		if checkResult != nil && len(checkResult.Rows) > 0 {
			if cnt, ok := checkResult.Rows[0][0].(int64); ok && cnt > 0 {
				sourceExists = true
			}
		}
		return nil, fmt.Errorf("failed to create relationship: source node %s exists=%v, target=%s", source, sourceExists, target)
	}

	return extractEdgeFromResult(result.Rows[0])
}

func (r *Resolver) deleteEdgeViaCypher(ctx context.Context, id string) error {
	// Use id(r) function for matching internal storage ID, or fallback to r.id property
	query := "MATCH ()-[r]->() WHERE id(r) = $id OR r.id = $id DELETE r"
	params := map[string]interface{}{"id": id}
	_, err := r.executeCypher(ctx, query, params, "")
	return err
}

func (r *Resolver) getEdgeViaCypher(ctx context.Context, id string) (*nornicdb.GraphEdge, error) {
	// Use id(r) function for matching internal storage ID, or fallback to r.id property
	query := "MATCH (a)-[r]->(b) WHERE id(r) = $id OR r.id = $id RETURN r, id(a) as source, id(b) as target LIMIT 1"
	params := map[string]interface{}{"id": id}
	result, err := r.executeCypher(ctx, query, params, "")
	if err != nil {
		return nil, err
	}

	if len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
		return nil, fmt.Errorf("edge not found")
	}

	// Fallback to id if _nodeId not available
	row := result.Rows[0]
	if len(row) >= 3 {
		if sourceID, ok := row[1].(string); !ok || sourceID == "" {
			if nodeMap, ok := row[1].(map[string]interface{}); ok {
				if id, ok := nodeMap["id"].(string); ok {
					row[1] = id
				} else if id, ok := nodeMap["_nodeId"].(string); ok {
					row[1] = id
				}
			}
		}
		if targetID, ok := row[2].(string); !ok || targetID == "" {
			if nodeMap, ok := row[2].(map[string]interface{}); ok {
				if id, ok := nodeMap["id"].(string); ok {
					row[2] = id
				} else if id, ok := nodeMap["_nodeId"].(string); ok {
					row[2] = id
				}
			}
		}
	}

	return extractEdgeFromResult(result.Rows[0])
}

func (r *Resolver) getEdgesForNodeViaCypher(ctx context.Context, nodeID string) ([]*nornicdb.GraphEdge, error) {
	// Use id() function for matching internal storage ID, or fallback to n.id property
	// Query outgoing edges: match edges where start node matches
	outgoingQuery := "MATCH (n)-[r]->(m) WHERE (id(n) = $nodeId OR n.id = $nodeId) RETURN r, id(n) as source, id(m) as target"
	params := map[string]interface{}{"nodeId": nodeID}
	outgoingResult, err := r.executeCypher(ctx, outgoingQuery, params, "")
	if err != nil {
		return nil, err
	}

	// Query incoming edges: match edges where end node matches
	incomingQuery := "MATCH (n)<-[r]-(m) WHERE (id(n) = $nodeId OR n.id = $nodeId) RETURN r, id(m) as source, id(n) as target"
	incomingResult, err := r.executeCypher(ctx, incomingQuery, params, "")
	if err != nil {
		return nil, err
	}

	edges := make([]*nornicdb.GraphEdge, 0, len(outgoingResult.Rows)+len(incomingResult.Rows))

	// Process outgoing edges
	for _, row := range outgoingResult.Rows {
		if len(row) >= 3 {
			// id() function returns string directly, so row[1] and row[2] should already be strings
			// But handle case where they might be node objects
			if sourceID, ok := row[1].(string); !ok || sourceID == "" {
				if nodeMap, ok := row[1].(map[string]interface{}); ok {
					if id, ok := nodeMap["id"].(string); ok {
						row[1] = id
					} else if id, ok := nodeMap["_nodeId"].(string); ok {
						row[1] = id
					}
				} else if storageNode, ok := row[1].(*storage.Node); ok {
					row[1] = string(storageNode.ID)
				}
			}
			if targetID, ok := row[2].(string); !ok || targetID == "" {
				if nodeMap, ok := row[2].(map[string]interface{}); ok {
					if id, ok := nodeMap["id"].(string); ok {
						row[2] = id
					} else if id, ok := nodeMap["_nodeId"].(string); ok {
						row[2] = id
					}
				} else if storageNode, ok := row[2].(*storage.Node); ok {
					row[2] = string(storageNode.ID)
				}
			}
			if edge, err := extractEdgeFromResult(row); err == nil {
				edges = append(edges, edge)
			}
		}
	}

	// Process incoming edges
	for _, row := range incomingResult.Rows {
		if len(row) >= 3 {
			// id() function returns string directly, so row[1] and row[2] should already be strings
			// But handle case where they might be node objects
			if sourceID, ok := row[1].(string); !ok || sourceID == "" {
				if nodeMap, ok := row[1].(map[string]interface{}); ok {
					if id, ok := nodeMap["id"].(string); ok {
						row[1] = id
					} else if id, ok := nodeMap["_nodeId"].(string); ok {
						row[1] = id
					}
				} else if storageNode, ok := row[1].(*storage.Node); ok {
					row[1] = string(storageNode.ID)
				}
			}
			if targetID, ok := row[2].(string); !ok || targetID == "" {
				if nodeMap, ok := row[2].(map[string]interface{}); ok {
					if id, ok := nodeMap["id"].(string); ok {
						row[2] = id
					} else if id, ok := nodeMap["_nodeId"].(string); ok {
						row[2] = id
					}
				} else if storageNode, ok := row[2].(*storage.Node); ok {
					row[2] = string(storageNode.ID)
				}
			}
			if edge, err := extractEdgeFromResult(row); err == nil {
				edges = append(edges, edge)
			}
		}
	}

	return edges, nil
}

func (r *Resolver) listEdgesViaCypher(ctx context.Context, relType string, limit, offset int) ([]*nornicdb.GraphEdge, error) {
	typeClause := ""
	if relType != "" {
		typeClause = ":" + relType
	}

	// Use MATCH pattern to get source/target nodes explicitly
	query := fmt.Sprintf("MATCH (a)-[r%s]->(b) RETURN r, id(a) as source, id(b) as target ORDER BY id(r) SKIP $offset LIMIT $limit", typeClause)
	params := map[string]interface{}{
		"offset": offset,
		"limit":  limit,
	}

	result, err := r.executeCypher(ctx, query, params, "")
	if err != nil {
		return nil, err
	}

	edges := make([]*nornicdb.GraphEdge, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) >= 3 {
			// id() function returns string directly, so row[1] and row[2] should already be strings
			// But handle case where they might be node objects
			if sourceID, ok := row[1].(string); !ok || sourceID == "" {
				if nodeMap, ok := row[1].(map[string]interface{}); ok {
					if id, ok := nodeMap["id"].(string); ok {
						row[1] = id
					} else if id, ok := nodeMap["_nodeId"].(string); ok {
						row[1] = id
					}
				} else if storageNode, ok := row[1].(*storage.Node); ok {
					row[1] = string(storageNode.ID)
				}
			}
			if targetID, ok := row[2].(string); !ok || targetID == "" {
				if nodeMap, ok := row[2].(map[string]interface{}); ok {
					if id, ok := nodeMap["id"].(string); ok {
						row[2] = id
					} else if id, ok := nodeMap["_nodeId"].(string); ok {
						row[2] = id
					}
				} else if storageNode, ok := row[2].(*storage.Node); ok {
					row[2] = string(storageNode.ID)
				}
			}
			if edge, err := extractEdgeFromResult(row); err == nil {
				edges = append(edges, edge)
			}
		}
	}

	return edges, nil
}

func (r *Resolver) getLabelsViaCypher(ctx context.Context) ([]string, error) {
	query := "CALL db.labels() YIELD label RETURN label"
	result, err := r.executeCypher(ctx, query, nil, "")
	if err != nil {
		// Fallback to MATCH if CALL doesn't work
		query = "MATCH (n) RETURN DISTINCT labels(n) as labels"
		result, err = r.executeCypher(ctx, query, nil, "")
		if err != nil {
			return nil, err
		}
		// Extract labels from result
		labelSet := make(map[string]bool)
		for _, row := range result.Rows {
			if len(row) > 0 {
				if labels, ok := row[0].([]interface{}); ok {
					for _, l := range labels {
						if label, ok := l.(string); ok {
							labelSet[label] = true
						}
					}
				}
			}
		}
		labels := make([]string, 0, len(labelSet))
		for l := range labelSet {
			labels = append(labels, l)
		}
		return labels, nil
	}

	labels := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			if label, ok := row[0].(string); ok {
				labels = append(labels, label)
			}
		}
	}
	return labels, nil
}

func (r *Resolver) getRelationshipTypesViaCypher(ctx context.Context) ([]string, error) {
	query := "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"
	result, err := r.executeCypher(ctx, query, nil, "")
	if err != nil {
		// Fallback to MATCH if CALL doesn't work
		query = "MATCH ()-[r]->() RETURN DISTINCT type(r) as type"
		result, err = r.executeCypher(ctx, query, nil, "")
		if err != nil {
			return nil, err
		}
	}

	types := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		if len(row) > 0 {
			if relType, ok := row[0].(string); ok {
				types = append(types, relType)
			}
		}
	}
	return types, nil
}

func (r *Resolver) statsViaCypher() nornicdb.DBStats {
	// Get node count
	nodeQuery := "MATCH (n) RETURN count(n) as count"
	nodeResult, _ := r.executeCypher(context.Background(), nodeQuery, nil, "")
	nodeCount := int64(0)
	if len(nodeResult.Rows) > 0 && len(nodeResult.Rows[0]) > 0 {
		if cnt, ok := nodeResult.Rows[0][0].(int64); ok {
			nodeCount = cnt
		}
	}

	// Get edge count
	edgeQuery := "MATCH ()-[r]->() RETURN count(r) as count"
	edgeResult, _ := r.executeCypher(context.Background(), edgeQuery, nil, "")
	edgeCount := int64(0)
	if len(edgeResult.Rows) > 0 && len(edgeResult.Rows[0]) > 0 {
		if cnt, ok := edgeResult.Rows[0][0].(int64); ok {
			edgeCount = cnt
		}
	}

	return nornicdb.DBStats{
		NodeCount: nodeCount,
		EdgeCount: edgeCount,
	}
}

// Helper functions

func buildLabelsString(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	return ":" + strings.Join(labels, ":")
}

func buildPropertiesString(properties map[string]interface{}) (string, map[string]interface{}) {
	if len(properties) == 0 {
		return "{}", make(map[string]interface{})
	}

	propsStr := "{"
	params := make(map[string]interface{})
	first := true
	i := 0
	for k, v := range properties {
		if !first {
			propsStr += ", "
		}
		first = false
		paramName := fmt.Sprintf("p%d", i)
		propsStr += fmt.Sprintf("%s: $%s", k, paramName)
		params[paramName] = v
		i++
	}
	propsStr += "}"

	return propsStr, params
}

// extractNodeFromResult extracts a node from a Cypher query result.
// This function is exported for use in tests.
func extractNodeFromResult(val interface{}) (*nornicdb.Node, error) {
	// Handle both map and *storage.Node formats
	var nodeMap map[string]interface{}

	if nm, ok := val.(map[string]interface{}); ok {
		nodeMap = nm
	} else if storageNode, ok := val.(*storage.Node); ok {
		// Convert storage.Node to map format
		nodeMap = map[string]interface{}{
			"_nodeId":    string(storageNode.ID),
			"id":         string(storageNode.ID),
			"labels":     storageNode.Labels,
			"properties": storageNode.Properties,
		}
	} else {
		return nil, fmt.Errorf("unexpected node format: %T", val)
	}

	node := &nornicdb.Node{}

	// Prefer _nodeId (internal storage ID), then id, then elementId
	var rawID string
	if id, ok := nodeMap["_nodeId"].(string); ok && id != "" {
		rawID = id
	} else if id, ok := nodeMap["id"].(string); ok && id != "" {
		rawID = id
	} else if id, ok := nodeMap["elementId"].(string); ok && id != "" {
		// Extract ID from elementId format (4:nornicdb:uuid -> uuid)
		parts := strings.Split(id, ":")
		if len(parts) >= 3 {
			rawID = parts[2]
		} else {
			rawID = id
		}
	} else {
		return nil, fmt.Errorf("node missing ID field")
	}

	// IDs are already unprefixed by GetNode/GetEdge in NamespacedEngine
	// No need to unprefix here - just use the ID as-is
	node.ID = rawID

	if labels, ok := nodeMap["labels"].([]string); ok {
		node.Labels = labels
	} else if labels, ok := nodeMap["labels"].([]interface{}); ok {
		node.Labels = make([]string, 0, len(labels))
		for _, l := range labels {
			if s, ok := l.(string); ok {
				node.Labels = append(node.Labels, s)
			}
		}
	}

	if props, ok := nodeMap["properties"].(map[string]interface{}); ok {
		node.Properties = props
	} else {
		// Extract properties from top-level keys (Neo4j compatibility)
		node.Properties = make(map[string]interface{})
		for k, v := range nodeMap {
			if k != "_nodeId" && k != "id" && k != "elementId" && k != "labels" && k != "properties" && k != "embedding" {
				node.Properties[k] = v
			}
		}
	}

	return node, nil
}

// extractEdgeFromResult extracts an edge from a Cypher query result.
// This function is exported for use in tests.
func extractEdgeFromResult(row []interface{}) (*nornicdb.GraphEdge, error) {
	if len(row) < 1 {
		return nil, fmt.Errorf("insufficient edge data")
	}

	edge := &nornicdb.GraphEdge{}

	// Handle *storage.Edge directly (most common case from CREATE queries)
	if storageEdge, ok := row[0].(*storage.Edge); ok {
		edge.ID = string(storageEdge.ID)
		edge.Type = storageEdge.Type
		edge.Properties = storageEdge.Properties

		// Extract source and target from row if provided (from id() function calls)
		if len(row) >= 3 {
			if source, ok := row[1].(string); ok && source != "" {
				edge.Source = source
			} else if storageNode, ok := row[1].(*storage.Node); ok {
				edge.Source = string(storageNode.ID)
			} else if nodeMap, ok := row[1].(map[string]interface{}); ok {
				if id, ok := nodeMap["_nodeId"].(string); ok {
					edge.Source = id
				} else if id, ok := nodeMap["id"].(string); ok {
					edge.Source = id
				}
			}
			if target, ok := row[2].(string); ok && target != "" {
				edge.Target = target
			} else if storageNode, ok := row[2].(*storage.Node); ok {
				edge.Target = string(storageNode.ID)
			} else if nodeMap, ok := row[2].(map[string]interface{}); ok {
				if id, ok := nodeMap["_nodeId"].(string); ok {
					edge.Target = id
				} else if id, ok := nodeMap["id"].(string); ok {
					edge.Target = id
				}
			}
		} else {
			// Fallback to edge's StartNode/EndNode if not in row
			edge.Source = string(storageEdge.StartNode)
			edge.Target = string(storageEdge.EndNode)
		}

		return edge, nil
	}

	// Handle map format (from edgeToMap)
	var edgeMap map[string]interface{}
	if em, ok := row[0].(map[string]interface{}); ok {
		edgeMap = em
	} else {
		return nil, fmt.Errorf("unexpected edge format: %T", row[0])
	}

	// Extract ID - prefer _edgeId, then id, then elementId
	if id, ok := edgeMap["_edgeId"].(string); ok && id != "" {
		edge.ID = id
	} else if id, ok := edgeMap["id"].(string); ok && id != "" {
		edge.ID = id
	} else if id, ok := edgeMap["elementId"].(string); ok && id != "" {
		parts := strings.Split(id, ":")
		if len(parts) >= 3 {
			edge.ID = parts[2]
		} else {
			edge.ID = id
		}
	} else {
		return nil, fmt.Errorf("edge missing ID field")
	}

	// Extract source and target from row (if provided)
	if len(row) >= 3 {
		if source, ok := row[1].(string); ok && source != "" {
			edge.Source = source
		} else if storageNode, ok := row[1].(*storage.Node); ok {
			edge.Source = string(storageNode.ID)
		} else if nodeMap, ok := row[1].(map[string]interface{}); ok {
			if id, ok := nodeMap["_nodeId"].(string); ok {
				edge.Source = id
			} else if id, ok := nodeMap["id"].(string); ok {
				edge.Source = id
			}
		}
		if target, ok := row[2].(string); ok && target != "" {
			edge.Target = target
		} else if storageNode, ok := row[2].(*storage.Node); ok {
			edge.Target = string(storageNode.ID)
		} else if nodeMap, ok := row[2].(map[string]interface{}); ok {
			if id, ok := nodeMap["_nodeId"].(string); ok {
				edge.Target = id
			} else if id, ok := nodeMap["id"].(string); ok {
				edge.Target = id
			}
		}
	} else {
		// Fallback to edgeMap's startNode/endNode
		if startNode, ok := edgeMap["startNode"].(string); ok {
			edge.Source = startNode
		}
		if endNode, ok := edgeMap["endNode"].(string); ok {
			edge.Target = endNode
		}
	}

	// Extract type
	if relType, ok := edgeMap["type"].(string); ok {
		edge.Type = relType
	}

	// Extract properties
	if props, ok := edgeMap["properties"].(map[string]interface{}); ok {
		edge.Properties = props
	} else {
		edge.Properties = make(map[string]interface{})
	}

	return edge, nil
}
