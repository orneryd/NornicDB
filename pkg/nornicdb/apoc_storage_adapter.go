// Package nornicdb provides an APOC storage adapter for the NornicDB storage engine.
//
// This adapter bridges pkg/storage.Engine to apoc/storage.Storage, allowing
// APOC plugins to access the database storage for procedures that need graph
// traversal, pathfinding, and other operations.
package nornicdb

import (
	"strconv"

	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// APOCStorageAdapter wraps a storage.Engine to implement apoc/storage.Storage.
// This allows APOC plugins to access the NornicDB storage layer.
type APOCStorageAdapter struct {
	engine storage.Engine
}

// NewAPOCStorageAdapter creates a new adapter wrapping the given storage engine.
func NewAPOCStorageAdapter(engine storage.Engine) *APOCStorageAdapter {
	return &APOCStorageAdapter{engine: engine}
}

// GetNode retrieves a node by ID.
func (a *APOCStorageAdapter) GetNode(id int64) (*apocstorage.Node, error) {
	nodeID := storage.NodeID(strconv.FormatInt(id, 10))
	node, err := a.engine.GetNode(nodeID)
	if err != nil {
		return nil, apocstorage.ErrNodeNotFound
	}
	return a.convertNode(node), nil
}

// CreateNode creates a new node with the given labels and properties.
func (a *APOCStorageAdapter) CreateNode(labels []string, properties map[string]interface{}) (*apocstorage.Node, error) {
	node := &storage.Node{
		Labels:     labels,
		Properties: properties,
	}
	_, err := a.engine.CreateNode(node)
	if err != nil {
		return nil, err
	}
	return a.convertNode(node), nil
}

// UpdateNode updates the properties of an existing node.
func (a *APOCStorageAdapter) UpdateNode(id int64, properties map[string]interface{}) error {
	nodeID := storage.NodeID(strconv.FormatInt(id, 10))
	node, err := a.engine.GetNode(nodeID)
	if err != nil {
		return apocstorage.ErrNodeNotFound
	}
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	for k, v := range properties {
		node.Properties[k] = v
	}
	return a.engine.UpdateNode(node)
}

// DeleteNode removes a node by ID.
func (a *APOCStorageAdapter) DeleteNode(id int64) error {
	nodeID := storage.NodeID(strconv.FormatInt(id, 10))
	return a.engine.DeleteNode(nodeID)
}

// AddLabels adds labels to a node.
func (a *APOCStorageAdapter) AddLabels(id int64, labels []string) error {
	nodeID := storage.NodeID(strconv.FormatInt(id, 10))
	node, err := a.engine.GetNode(nodeID)
	if err != nil {
		return apocstorage.ErrNodeNotFound
	}
	labelSet := make(map[string]bool)
	for _, l := range node.Labels {
		labelSet[l] = true
	}
	for _, l := range labels {
		if !labelSet[l] {
			node.Labels = append(node.Labels, l)
		}
	}
	return a.engine.UpdateNode(node)
}

// RemoveLabels removes labels from a node.
func (a *APOCStorageAdapter) RemoveLabels(id int64, labels []string) error {
	nodeID := storage.NodeID(strconv.FormatInt(id, 10))
	node, err := a.engine.GetNode(nodeID)
	if err != nil {
		return apocstorage.ErrNodeNotFound
	}
	labelSet := make(map[string]bool)
	for _, l := range labels {
		labelSet[l] = true
	}
	newLabels := make([]string, 0)
	for _, l := range node.Labels {
		if !labelSet[l] {
			newLabels = append(newLabels, l)
		}
	}
	node.Labels = newLabels
	return a.engine.UpdateNode(node)
}

// GetRelationship retrieves a relationship by ID.
func (a *APOCStorageAdapter) GetRelationship(id int64) (*apocstorage.Relationship, error) {
	edgeID := storage.EdgeID(strconv.FormatInt(id, 10))
	edge, err := a.engine.GetEdge(edgeID)
	if err != nil {
		return nil, apocstorage.ErrRelationshipNotFound
	}
	return a.convertRelationship(edge), nil
}

// CreateRelationship creates a new relationship.
func (a *APOCStorageAdapter) CreateRelationship(startID, endID int64, relType string, properties map[string]interface{}) (*apocstorage.Relationship, error) {
	edge := &storage.Edge{
		StartNode:  storage.NodeID(strconv.FormatInt(startID, 10)),
		EndNode:    storage.NodeID(strconv.FormatInt(endID, 10)),
		Type:       relType,
		Properties: properties,
	}
	err := a.engine.CreateEdge(edge)
	if err != nil {
		return nil, err
	}
	return a.convertRelationship(edge), nil
}

// UpdateRelationship updates the properties of an existing relationship.
func (a *APOCStorageAdapter) UpdateRelationship(id int64, properties map[string]interface{}) error {
	edgeID := storage.EdgeID(strconv.FormatInt(id, 10))
	edge, err := a.engine.GetEdge(edgeID)
	if err != nil {
		return apocstorage.ErrRelationshipNotFound
	}
	if edge.Properties == nil {
		edge.Properties = make(map[string]interface{})
	}
	for k, v := range properties {
		edge.Properties[k] = v
	}
	return a.engine.UpdateEdge(edge)
}

// DeleteRelationship removes a relationship by ID.
func (a *APOCStorageAdapter) DeleteRelationship(id int64) error {
	edgeID := storage.EdgeID(strconv.FormatInt(id, 10))
	return a.engine.DeleteEdge(edgeID)
}

// GetNodeRelationships gets relationships for a node filtered by type and direction.
func (a *APOCStorageAdapter) GetNodeRelationships(nodeID int64, relType string, direction apocstorage.Direction) ([]*apocstorage.Relationship, error) {
	nID := storage.NodeID(strconv.FormatInt(nodeID, 10))
	var result []*apocstorage.Relationship

	if direction == apocstorage.DirectionOutgoing || direction == apocstorage.DirectionBoth {
		outgoing, err := a.engine.GetOutgoingEdges(nID)
		if err == nil {
			for _, edge := range outgoing {
				if relType == "" || edge.Type == relType {
					result = append(result, a.convertRelationship(edge))
				}
			}
		}
	}

	if direction == apocstorage.DirectionIncoming || direction == apocstorage.DirectionBoth {
		incoming, err := a.engine.GetIncomingEdges(nID)
		if err == nil {
			for _, edge := range incoming {
				if relType == "" || edge.Type == relType {
					result = append(result, a.convertRelationship(edge))
				}
			}
		}
	}

	return result, nil
}

// GetNodeNeighbors gets neighbor nodes for a node.
func (a *APOCStorageAdapter) GetNodeNeighbors(nodeID int64, relType string, direction apocstorage.Direction) ([]*apocstorage.Node, error) {
	rels, err := a.GetNodeRelationships(nodeID, relType, direction)
	if err != nil {
		return nil, err
	}

	seen := make(map[int64]bool)
	var result []*apocstorage.Node

	for _, rel := range rels {
		var neighborID int64
		if rel.StartNode == nodeID {
			neighborID = rel.EndNode
		} else {
			neighborID = rel.StartNode
		}

		if !seen[neighborID] {
			seen[neighborID] = true
			neighbor, err := a.GetNode(neighborID)
			if err == nil {
				result = append(result, neighbor)
			}
		}
	}

	return result, nil
}

// GetNodeDegree returns the degree of a node.
func (a *APOCStorageAdapter) GetNodeDegree(nodeID int64, relType string, direction apocstorage.Direction) (int, error) {
	rels, err := a.GetNodeRelationships(nodeID, relType, direction)
	if err != nil {
		return 0, err
	}
	return len(rels), nil
}

// FindShortestPath finds the shortest path between two nodes.
func (a *APOCStorageAdapter) FindShortestPath(startID, endID int64, relType string, maxHops int) (*apocstorage.Path, error) {
	if startID == endID {
		node, err := a.GetNode(startID)
		if err != nil {
			return nil, err
		}
		return &apocstorage.Path{
			Nodes:         []*apocstorage.Node{node},
			Relationships: []*apocstorage.Relationship{},
			Length:        0,
		}, nil
	}

	// BFS for shortest path
	type queueItem struct {
		nodeID int64
		path   []*apocstorage.Node
		rels   []*apocstorage.Relationship
	}

	visited := make(map[int64]bool)
	queue := []queueItem{{nodeID: startID, path: nil, rels: nil}}

	startNode, err := a.GetNode(startID)
	if err != nil {
		return nil, err
	}
	queue[0].path = []*apocstorage.Node{startNode}
	visited[startID] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if len(current.path)-1 >= maxHops {
			continue
		}

		rels, _ := a.GetNodeRelationships(current.nodeID, relType, apocstorage.DirectionBoth)
		for _, rel := range rels {
			var neighborID int64
			if rel.StartNode == current.nodeID {
				neighborID = rel.EndNode
			} else {
				neighborID = rel.StartNode
			}

			if !visited[neighborID] {
				visited[neighborID] = true

				neighbor, err := a.GetNode(neighborID)
				if err != nil {
					continue
				}

				newPath := append([]*apocstorage.Node{}, current.path...)
				newPath = append(newPath, neighbor)
				newRels := append([]*apocstorage.Relationship{}, current.rels...)
				newRels = append(newRels, rel)

				if neighborID == endID {
					return &apocstorage.Path{
						Nodes:         newPath,
						Relationships: newRels,
						Length:        len(newRels),
					}, nil
				}

				queue = append(queue, queueItem{
					nodeID: neighborID,
					path:   newPath,
					rels:   newRels,
				})
			}
		}
	}

	return nil, apocstorage.ErrPathNotFound
}

// FindAllPaths finds all paths between two nodes up to maxHops.
func (a *APOCStorageAdapter) FindAllPaths(startID, endID int64, relType string, maxHops int) ([]*apocstorage.Path, error) {
	var paths []*apocstorage.Path
	visited := make(map[int64]bool)

	var dfs func(currentID int64, path []*apocstorage.Node, rels []*apocstorage.Relationship)
	dfs = func(currentID int64, path []*apocstorage.Node, rels []*apocstorage.Relationship) {
		if currentID == endID {
			pathCopy := &apocstorage.Path{
				Nodes:         append([]*apocstorage.Node{}, path...),
				Relationships: append([]*apocstorage.Relationship{}, rels...),
				Length:        len(rels),
			}
			paths = append(paths, pathCopy)
			return
		}

		if len(rels) >= maxHops {
			return
		}

		visited[currentID] = true

		nodeRels, _ := a.GetNodeRelationships(currentID, relType, apocstorage.DirectionBoth)
		for _, rel := range nodeRels {
			var neighborID int64
			if rel.StartNode == currentID {
				neighborID = rel.EndNode
			} else {
				neighborID = rel.StartNode
			}

			if !visited[neighborID] {
				neighbor, err := a.GetNode(neighborID)
				if err != nil {
					continue
				}

				dfs(neighborID, append(path, neighbor), append(rels, rel))
			}
		}

		visited[currentID] = false
	}

	startNode, err := a.GetNode(startID)
	if err != nil {
		return nil, err
	}

	dfs(startID, []*apocstorage.Node{startNode}, nil)
	return paths, nil
}

// BFS performs breadth-first search from a node.
func (a *APOCStorageAdapter) BFS(startID int64, relType string, maxDepth int, visitor func(*apocstorage.Node) bool) error {
	type queueItem struct {
		nodeID int64
		depth  int
	}

	visited := make(map[int64]bool)
	queue := []queueItem{{nodeID: startID, depth: 0}}
	visited[startID] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		node, err := a.GetNode(current.nodeID)
		if err != nil {
			continue
		}

		if !visitor(node) {
			return nil
		}

		if current.depth >= maxDepth {
			continue
		}

		neighbors, _ := a.GetNodeNeighbors(current.nodeID, relType, apocstorage.DirectionBoth)
		for _, neighbor := range neighbors {
			if !visited[neighbor.ID] {
				visited[neighbor.ID] = true
				queue = append(queue, queueItem{nodeID: neighbor.ID, depth: current.depth + 1})
			}
		}
	}

	return nil
}

// DFS performs depth-first search from a node.
func (a *APOCStorageAdapter) DFS(startID int64, relType string, maxDepth int, visitor func(*apocstorage.Node) bool) error {
	visited := make(map[int64]bool)

	var dfs func(nodeID int64, depth int) bool
	dfs = func(nodeID int64, depth int) bool {
		visited[nodeID] = true

		node, err := a.GetNode(nodeID)
		if err != nil {
			return true
		}

		if !visitor(node) {
			return false
		}

		if depth >= maxDepth {
			return true
		}

		neighbors, _ := a.GetNodeNeighbors(nodeID, relType, apocstorage.DirectionBoth)
		for _, neighbor := range neighbors {
			if !visited[neighbor.ID] {
				if !dfs(neighbor.ID, depth+1) {
					return false
				}
			}
		}

		return true
	}

	dfs(startID, 0)
	return nil
}

// Helper functions

func (a *APOCStorageAdapter) convertNode(node *storage.Node) *apocstorage.Node {
	id, _ := strconv.ParseInt(string(node.ID), 10, 64)
	return &apocstorage.Node{
		ID:         id,
		Labels:     node.Labels,
		Properties: node.Properties,
	}
}

func (a *APOCStorageAdapter) convertRelationship(edge *storage.Edge) *apocstorage.Relationship {
	id, _ := strconv.ParseInt(string(edge.ID), 10, 64)
	startID, _ := strconv.ParseInt(string(edge.StartNode), 10, 64)
	endID, _ := strconv.ParseInt(string(edge.EndNode), 10, 64)
	return &apocstorage.Relationship{
		ID:         id,
		Type:       edge.Type,
		StartNode:  startID,
		EndNode:    endID,
		Properties: edge.Properties,
	}
}
