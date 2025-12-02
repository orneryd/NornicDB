// Package storage provides the storage interface for APOC functions.
//
// This package defines the interface that APOC functions use to interact
// with the underlying graph database storage engine.
package storage

// Storage defines the interface for graph database operations.
type Storage interface {
	// Node operations
	GetNode(id int64) (*Node, error)
	CreateNode(labels []string, properties map[string]interface{}) (*Node, error)
	UpdateNode(id int64, properties map[string]interface{}) error
	DeleteNode(id int64) error
	AddLabels(id int64, labels []string) error
	RemoveLabels(id int64, labels []string) error
	
	// Relationship operations
	GetRelationship(id int64) (*Relationship, error)
	CreateRelationship(startID, endID int64, relType string, properties map[string]interface{}) (*Relationship, error)
	UpdateRelationship(id int64, properties map[string]interface{}) error
	DeleteRelationship(id int64) error
	
	// Query operations
	GetNodeRelationships(nodeID int64, relType string, direction Direction) ([]*Relationship, error)
	GetNodeNeighbors(nodeID int64, relType string, direction Direction) ([]*Node, error)
	GetNodeDegree(nodeID int64, relType string, direction Direction) (int, error)
	
	// Path operations
	FindShortestPath(startID, endID int64, relType string, maxHops int) (*Path, error)
	FindAllPaths(startID, endID int64, relType string, maxHops int) ([]*Path, error)
	
	// Traversal operations
	BFS(startID int64, relType string, maxDepth int, visitor func(*Node) bool) error
	DFS(startID int64, relType string, maxDepth int, visitor func(*Node) bool) error
}

// Direction represents relationship direction.
type Direction int

const (
	DirectionBoth Direction = iota
	DirectionOutgoing
	DirectionIncoming
)

// Node represents a graph node.
type Node struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

// Relationship represents a graph relationship.
type Relationship struct {
	ID         int64
	Type       string
	StartNode  int64
	EndNode    int64
	Properties map[string]interface{}
}

// Path represents a path through the graph.
type Path struct {
	Nodes         []*Node
	Relationships []*Relationship
	Length        int
}

// InMemoryStorage provides an in-memory implementation for testing/development.
type InMemoryStorage struct {
	nodes         map[int64]*Node
	relationships map[int64]*Relationship
	nodeRels      map[int64][]*Relationship
	nextNodeID    int64
	nextRelID     int64
}

// NewInMemoryStorage creates a new in-memory storage.
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		nodes:         make(map[int64]*Node),
		relationships: make(map[int64]*Relationship),
		nodeRels:      make(map[int64][]*Relationship),
		nextNodeID:    1,
		nextRelID:     1,
	}
}

func (s *InMemoryStorage) GetNode(id int64) (*Node, error) {
	if node, ok := s.nodes[id]; ok {
		return node, nil
	}
	return nil, ErrNodeNotFound
}

func (s *InMemoryStorage) CreateNode(labels []string, properties map[string]interface{}) (*Node, error) {
	node := &Node{
		ID:         s.nextNodeID,
		Labels:     labels,
		Properties: properties,
	}
	s.nodes[node.ID] = node
	s.nextNodeID++
	return node, nil
}

func (s *InMemoryStorage) UpdateNode(id int64, properties map[string]interface{}) error {
	node, err := s.GetNode(id)
	if err != nil {
		return err
	}
	for k, v := range properties {
		node.Properties[k] = v
	}
	return nil
}

func (s *InMemoryStorage) DeleteNode(id int64) error {
	if _, ok := s.nodes[id]; !ok {
		return ErrNodeNotFound
	}
	delete(s.nodes, id)
	delete(s.nodeRels, id)
	return nil
}

func (s *InMemoryStorage) AddLabels(id int64, labels []string) error {
	node, err := s.GetNode(id)
	if err != nil {
		return err
	}
	for _, label := range labels {
		if !hasLabel(node, label) {
			node.Labels = append(node.Labels, label)
		}
	}
	return nil
}

func (s *InMemoryStorage) RemoveLabels(id int64, labels []string) error {
	node, err := s.GetNode(id)
	if err != nil {
		return err
	}
	labelSet := make(map[string]bool)
	for _, label := range labels {
		labelSet[label] = true
	}
	newLabels := make([]string, 0)
	for _, label := range node.Labels {
		if !labelSet[label] {
			newLabels = append(newLabels, label)
		}
	}
	node.Labels = newLabels
	return nil
}

func (s *InMemoryStorage) GetRelationship(id int64) (*Relationship, error) {
	if rel, ok := s.relationships[id]; ok {
		return rel, nil
	}
	return nil, ErrRelationshipNotFound
}

func (s *InMemoryStorage) CreateRelationship(startID, endID int64, relType string, properties map[string]interface{}) (*Relationship, error) {
	if _, err := s.GetNode(startID); err != nil {
		return nil, err
	}
	if _, err := s.GetNode(endID); err != nil {
		return nil, err
	}
	
	rel := &Relationship{
		ID:         s.nextRelID,
		Type:       relType,
		StartNode:  startID,
		EndNode:    endID,
		Properties: properties,
	}
	s.relationships[rel.ID] = rel
	s.nodeRels[startID] = append(s.nodeRels[startID], rel)
	s.nodeRels[endID] = append(s.nodeRels[endID], rel)
	s.nextRelID++
	return rel, nil
}

func (s *InMemoryStorage) UpdateRelationship(id int64, properties map[string]interface{}) error {
	rel, err := s.GetRelationship(id)
	if err != nil {
		return err
	}
	for k, v := range properties {
		rel.Properties[k] = v
	}
	return nil
}

func (s *InMemoryStorage) DeleteRelationship(id int64) error {
	rel, err := s.GetRelationship(id)
	if err != nil {
		return err
	}
	
	// Remove from node relationships
	s.nodeRels[rel.StartNode] = removeRel(s.nodeRels[rel.StartNode], id)
	s.nodeRels[rel.EndNode] = removeRel(s.nodeRels[rel.EndNode], id)
	
	delete(s.relationships, id)
	return nil
}

func (s *InMemoryStorage) GetNodeRelationships(nodeID int64, relType string, direction Direction) ([]*Relationship, error) {
	if _, err := s.GetNode(nodeID); err != nil {
		return nil, err
	}
	
	rels := s.nodeRels[nodeID]
	result := make([]*Relationship, 0)
	
	for _, rel := range rels {
		// Filter by type
		if relType != "" && rel.Type != relType {
			continue
		}
		
		// Filter by direction
		switch direction {
		case DirectionOutgoing:
			if rel.StartNode == nodeID {
				result = append(result, rel)
			}
		case DirectionIncoming:
			if rel.EndNode == nodeID {
				result = append(result, rel)
			}
		case DirectionBoth:
			result = append(result, rel)
		}
	}
	
	return result, nil
}

func (s *InMemoryStorage) GetNodeNeighbors(nodeID int64, relType string, direction Direction) ([]*Node, error) {
	rels, err := s.GetNodeRelationships(nodeID, relType, direction)
	if err != nil {
		return nil, err
	}
	
	neighbors := make([]*Node, 0)
	seen := make(map[int64]bool)
	
	for _, rel := range rels {
		var neighborID int64
		if rel.StartNode == nodeID {
			neighborID = rel.EndNode
		} else {
			neighborID = rel.StartNode
		}
		
		if !seen[neighborID] {
			if neighbor, err := s.GetNode(neighborID); err == nil {
				neighbors = append(neighbors, neighbor)
				seen[neighborID] = true
			}
		}
	}
	
	return neighbors, nil
}

func (s *InMemoryStorage) GetNodeDegree(nodeID int64, relType string, direction Direction) (int, error) {
	rels, err := s.GetNodeRelationships(nodeID, relType, direction)
	if err != nil {
		return 0, err
	}
	return len(rels), nil
}

func (s *InMemoryStorage) FindShortestPath(startID, endID int64, relType string, maxHops int) (*Path, error) {
	if startID == endID {
		start, err := s.GetNode(startID)
		if err != nil {
			return nil, err
		}
		return &Path{
			Nodes:         []*Node{start},
			Relationships: []*Relationship{},
			Length:        0,
		}, nil
	}
	
	visited := make(map[int64]bool)
	parent := make(map[int64]*pathParent)
	queue := []int64{startID}
	visited[startID] = true
	
	for len(queue) > 0 && len(parent) < maxHops*100 {
		currentID := queue[0]
		queue = queue[1:]
		
		if currentID == endID {
			return s.reconstructPath(startID, endID, parent)
		}
		
		depth := 0
		if p, ok := parent[currentID]; ok {
			depth = p.depth + 1
		}
		
		if depth >= maxHops {
			continue
		}
		
		neighbors, _ := s.GetNodeNeighbors(currentID, relType, DirectionBoth)
		for _, neighbor := range neighbors {
			if !visited[neighbor.ID] {
				visited[neighbor.ID] = true
				
				// Find the relationship
				rels, _ := s.GetNodeRelationships(currentID, relType, DirectionBoth)
				for _, rel := range rels {
					if (rel.StartNode == currentID && rel.EndNode == neighbor.ID) ||
						(rel.EndNode == currentID && rel.StartNode == neighbor.ID) {
						parent[neighbor.ID] = &pathParent{
							nodeID: currentID,
							rel:    rel,
							depth:  depth,
						}
						break
					}
				}
				
				queue = append(queue, neighbor.ID)
			}
		}
	}
	
	return nil, ErrPathNotFound
}

func (s *InMemoryStorage) FindAllPaths(startID, endID int64, relType string, maxHops int) ([]*Path, error) {
	paths := make([]*Path, 0)
	visited := make(map[int64]bool)
	
	var dfs func(currentID int64, path *Path, depth int)
	dfs = func(currentID int64, path *Path, depth int) {
		if currentID == endID {
			// Found a path
			pathCopy := &Path{
				Nodes:         append([]*Node{}, path.Nodes...),
				Relationships: append([]*Relationship{}, path.Relationships...),
				Length:        path.Length,
			}
			paths = append(paths, pathCopy)
			return
		}
		
		if depth >= maxHops {
			return
		}
		
		visited[currentID] = true
		
		neighbors, _ := s.GetNodeNeighbors(currentID, relType, DirectionBoth)
		for _, neighbor := range neighbors {
			if !visited[neighbor.ID] {
				// Find relationship
				rels, _ := s.GetNodeRelationships(currentID, relType, DirectionBoth)
				for _, rel := range rels {
					if (rel.StartNode == currentID && rel.EndNode == neighbor.ID) ||
						(rel.EndNode == currentID && rel.StartNode == neighbor.ID) {
						
						path.Nodes = append(path.Nodes, neighbor)
						path.Relationships = append(path.Relationships, rel)
						path.Length++
						
						dfs(neighbor.ID, path, depth+1)
						
						path.Nodes = path.Nodes[:len(path.Nodes)-1]
						path.Relationships = path.Relationships[:len(path.Relationships)-1]
						path.Length--
						break
					}
				}
			}
		}
		
		visited[currentID] = false
	}
	
	start, err := s.GetNode(startID)
	if err != nil {
		return nil, err
	}
	
	initialPath := &Path{
		Nodes:         []*Node{start},
		Relationships: []*Relationship{},
		Length:        0,
	}
	
	dfs(startID, initialPath, 0)
	
	return paths, nil
}

func (s *InMemoryStorage) BFS(startID int64, relType string, maxDepth int, visitor func(*Node) bool) error {
	visited := make(map[int64]bool)
	queue := []nodeDepth{{nodeID: startID, depth: 0}}
	visited[startID] = true
	
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		
		node, err := s.GetNode(current.nodeID)
		if err != nil {
			continue
		}
		
		if !visitor(node) {
			return nil
		}
		
		if current.depth >= maxDepth {
			continue
		}
		
		neighbors, _ := s.GetNodeNeighbors(current.nodeID, relType, DirectionBoth)
		for _, neighbor := range neighbors {
			if !visited[neighbor.ID] {
				visited[neighbor.ID] = true
				queue = append(queue, nodeDepth{nodeID: neighbor.ID, depth: current.depth + 1})
			}
		}
	}
	
	return nil
}

func (s *InMemoryStorage) DFS(startID int64, relType string, maxDepth int, visitor func(*Node) bool) error {
	visited := make(map[int64]bool)
	
	var dfs func(nodeID int64, depth int) bool
	dfs = func(nodeID int64, depth int) bool {
		visited[nodeID] = true
		
		node, err := s.GetNode(nodeID)
		if err != nil {
			return true
		}
		
		if !visitor(node) {
			return false
		}
		
		if depth >= maxDepth {
			return true
		}
		
		neighbors, _ := s.GetNodeNeighbors(nodeID, relType, DirectionBoth)
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

// Helper types and functions

type pathParent struct {
	nodeID int64
	rel    *Relationship
	depth  int
}

type nodeDepth struct {
	nodeID int64
	depth  int
}

func (s *InMemoryStorage) reconstructPath(startID, endID int64, parent map[int64]*pathParent) (*Path, error) {
	nodes := make([]*Node, 0)
	rels := make([]*Relationship, 0)
	
	currentID := endID
	for currentID != startID {
		node, err := s.GetNode(currentID)
		if err != nil {
			return nil, err
		}
		nodes = append([]*Node{node}, nodes...)
		
		if p, ok := parent[currentID]; ok {
			rels = append([]*Relationship{p.rel}, rels...)
			currentID = p.nodeID
		} else {
			return nil, ErrPathNotFound
		}
	}
	
	start, err := s.GetNode(startID)
	if err != nil {
		return nil, err
	}
	nodes = append([]*Node{start}, nodes...)
	
	return &Path{
		Nodes:         nodes,
		Relationships: rels,
		Length:        len(rels),
	}, nil
}

func hasLabel(node *Node, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}

func removeRel(rels []*Relationship, id int64) []*Relationship {
	result := make([]*Relationship, 0)
	for _, rel := range rels {
		if rel.ID != id {
			result = append(result, rel)
		}
	}
	return result
}

// Errors
var (
	ErrNodeNotFound         = &StorageError{Message: "node not found"}
	ErrRelationshipNotFound = &StorageError{Message: "relationship not found"}
	ErrPathNotFound         = &StorageError{Message: "path not found"}
)

type StorageError struct {
	Message string
}

func (e *StorageError) Error() string {
	return e.Message
}
