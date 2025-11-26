// Package storage provides storage implementations.
// MemoryEngine is a thread-safe in-memory storage for testing and small datasets.
package storage

import (
	"sync"
)

// MemoryEngine is an in-memory implementation of Engine.
// It's useful for:
// - Unit testing (no disk I/O)
// - Loading Neo4j exports into memory
// - Small datasets that fit in RAM
type MemoryEngine struct {
	mu    sync.RWMutex
	nodes map[NodeID]*Node
	edges map[EdgeID]*Edge

	// Indexes for efficient lookups
	nodesByLabel  map[string]map[NodeID]struct{}
	outgoingEdges map[NodeID]map[EdgeID]struct{}
	incomingEdges map[NodeID]map[EdgeID]struct{}

	closed bool
}

// NewMemoryEngine creates a new in-memory storage engine.
func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{
		nodes:         make(map[NodeID]*Node),
		edges:         make(map[EdgeID]*Edge),
		nodesByLabel:  make(map[string]map[NodeID]struct{}),
		outgoingEdges: make(map[NodeID]map[EdgeID]struct{}),
		incomingEdges: make(map[NodeID]map[EdgeID]struct{}),
	}
}

// CreateNode creates a new node.
func (m *MemoryEngine) CreateNode(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	if _, exists := m.nodes[node.ID]; exists {
		return ErrAlreadyExists
	}

	// Deep copy to prevent external mutation
	stored := m.copyNode(node)
	m.nodes[node.ID] = stored

	// Update label index
	for _, label := range node.Labels {
		if m.nodesByLabel[label] == nil {
			m.nodesByLabel[label] = make(map[NodeID]struct{})
		}
		m.nodesByLabel[label][node.ID] = struct{}{}
	}

	return nil
}

// GetNode retrieves a node by ID.
func (m *MemoryEngine) GetNode(id NodeID) (*Node, error) {
	if id == "" {
		return nil, ErrInvalidID
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	node, exists := m.nodes[id]
	if !exists {
		return nil, ErrNotFound
	}

	return m.copyNode(node), nil
}

// UpdateNode updates an existing node.
func (m *MemoryEngine) UpdateNode(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	existing, exists := m.nodes[node.ID]
	if !exists {
		return ErrNotFound
	}

	// Remove from old label indexes
	for _, label := range existing.Labels {
		if m.nodesByLabel[label] != nil {
			delete(m.nodesByLabel[label], node.ID)
		}
	}

	// Store updated node
	stored := m.copyNode(node)
	m.nodes[node.ID] = stored

	// Update label index
	for _, label := range node.Labels {
		if m.nodesByLabel[label] == nil {
			m.nodesByLabel[label] = make(map[NodeID]struct{})
		}
		m.nodesByLabel[label][node.ID] = struct{}{}
	}

	return nil
}

// DeleteNode removes a node and all its edges.
func (m *MemoryEngine) DeleteNode(id NodeID) error {
	if id == "" {
		return ErrInvalidID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	node, exists := m.nodes[id]
	if !exists {
		return ErrNotFound
	}

	// Remove from label indexes
	for _, label := range node.Labels {
		if m.nodesByLabel[label] != nil {
			delete(m.nodesByLabel[label], id)
		}
	}

	// Delete all outgoing edges
	if outgoing := m.outgoingEdges[id]; outgoing != nil {
		for edgeID := range outgoing {
			edge := m.edges[edgeID]
			if edge != nil {
				// Remove from target's incoming
				if incoming := m.incomingEdges[edge.EndNode]; incoming != nil {
					delete(incoming, edgeID)
				}
			}
			delete(m.edges, edgeID)
		}
		delete(m.outgoingEdges, id)
	}

	// Delete all incoming edges
	if incoming := m.incomingEdges[id]; incoming != nil {
		for edgeID := range incoming {
			edge := m.edges[edgeID]
			if edge != nil {
				// Remove from source's outgoing
				if outgoing := m.outgoingEdges[edge.StartNode]; outgoing != nil {
					delete(outgoing, edgeID)
				}
			}
			delete(m.edges, edgeID)
		}
		delete(m.incomingEdges, id)
	}

	delete(m.nodes, id)
	return nil
}

// CreateEdge creates a new edge.
func (m *MemoryEngine) CreateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	if _, exists := m.edges[edge.ID]; exists {
		return ErrAlreadyExists
	}

	// Verify start and end nodes exist
	if _, exists := m.nodes[edge.StartNode]; !exists {
		return ErrNotFound
	}
	if _, exists := m.nodes[edge.EndNode]; !exists {
		return ErrNotFound
	}

	// Store edge
	stored := m.copyEdge(edge)
	m.edges[edge.ID] = stored

	// Update indexes
	if m.outgoingEdges[edge.StartNode] == nil {
		m.outgoingEdges[edge.StartNode] = make(map[EdgeID]struct{})
	}
	m.outgoingEdges[edge.StartNode][edge.ID] = struct{}{}

	if m.incomingEdges[edge.EndNode] == nil {
		m.incomingEdges[edge.EndNode] = make(map[EdgeID]struct{})
	}
	m.incomingEdges[edge.EndNode][edge.ID] = struct{}{}

	return nil
}

// GetEdge retrieves an edge by ID.
func (m *MemoryEngine) GetEdge(id EdgeID) (*Edge, error) {
	if id == "" {
		return nil, ErrInvalidID
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	edge, exists := m.edges[id]
	if !exists {
		return nil, ErrNotFound
	}

	return m.copyEdge(edge), nil
}

// UpdateEdge updates an existing edge.
func (m *MemoryEngine) UpdateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	existing, exists := m.edges[edge.ID]
	if !exists {
		return ErrNotFound
	}

	// If endpoints changed, update indexes
	if existing.StartNode != edge.StartNode || existing.EndNode != edge.EndNode {
		// Remove from old indexes
		if m.outgoingEdges[existing.StartNode] != nil {
			delete(m.outgoingEdges[existing.StartNode], edge.ID)
		}
		if m.incomingEdges[existing.EndNode] != nil {
			delete(m.incomingEdges[existing.EndNode], edge.ID)
		}

		// Verify new endpoints exist
		if _, exists := m.nodes[edge.StartNode]; !exists {
			return ErrNotFound
		}
		if _, exists := m.nodes[edge.EndNode]; !exists {
			return ErrNotFound
		}

		// Add to new indexes
		if m.outgoingEdges[edge.StartNode] == nil {
			m.outgoingEdges[edge.StartNode] = make(map[EdgeID]struct{})
		}
		m.outgoingEdges[edge.StartNode][edge.ID] = struct{}{}

		if m.incomingEdges[edge.EndNode] == nil {
			m.incomingEdges[edge.EndNode] = make(map[EdgeID]struct{})
		}
		m.incomingEdges[edge.EndNode][edge.ID] = struct{}{}
	}

	stored := m.copyEdge(edge)
	m.edges[edge.ID] = stored

	return nil
}

// DeleteEdge removes an edge.
func (m *MemoryEngine) DeleteEdge(id EdgeID) error {
	if id == "" {
		return ErrInvalidID
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	edge, exists := m.edges[id]
	if !exists {
		return ErrNotFound
	}

	// Remove from indexes
	if m.outgoingEdges[edge.StartNode] != nil {
		delete(m.outgoingEdges[edge.StartNode], id)
	}
	if m.incomingEdges[edge.EndNode] != nil {
		delete(m.incomingEdges[edge.EndNode], id)
	}

	delete(m.edges, id)
	return nil
}

// GetNodesByLabel returns all nodes with the given label.
func (m *MemoryEngine) GetNodesByLabel(label string) ([]*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	nodeIDs := m.nodesByLabel[label]
	if nodeIDs == nil {
		return []*Node{}, nil
	}

	nodes := make([]*Node, 0, len(nodeIDs))
	for id := range nodeIDs {
		if node := m.nodes[id]; node != nil {
			nodes = append(nodes, m.copyNode(node))
		}
	}

	return nodes, nil
}

// GetAllNodes returns all nodes in the storage.
func (m *MemoryEngine) GetAllNodes() []*Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return []*Node{}
	}

	nodes := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, m.copyNode(node))
	}

	return nodes
}

// GetEdgeBetween returns an edge between two nodes with the given type.
// Returns nil if no edge exists.
func (m *MemoryEngine) GetEdgeBetween(source, target NodeID, edgeType string) *Edge {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil
	}

	// Get outgoing edges from source
	edgeIDs := m.outgoingEdges[source]
	if edgeIDs == nil {
		return nil
	}

	// Find edge to target with matching type
	for edgeID := range edgeIDs {
		edge := m.edges[edgeID]
		if edge != nil && edge.EndNode == target {
			if edgeType == "" || edge.Type == edgeType {
				return m.copyEdge(edge)
			}
		}
	}

	return nil
}

// GetOutgoingEdges returns all edges starting from the given node.
func (m *MemoryEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	edgeIDs := m.outgoingEdges[nodeID]
	if edgeIDs == nil {
		return []*Edge{}, nil
	}

	edges := make([]*Edge, 0, len(edgeIDs))
	for id := range edgeIDs {
		if edge := m.edges[id]; edge != nil {
			edges = append(edges, m.copyEdge(edge))
		}
	}

	return edges, nil
}

// GetIncomingEdges returns all edges ending at the given node.
func (m *MemoryEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	edgeIDs := m.incomingEdges[nodeID]
	if edgeIDs == nil {
		return []*Edge{}, nil
	}

	edges := make([]*Edge, 0, len(edgeIDs))
	for id := range edgeIDs {
		if edge := m.edges[id]; edge != nil {
			edges = append(edges, m.copyEdge(edge))
		}
	}

	return edges, nil
}

// GetEdgesBetween returns all edges between two nodes.
func (m *MemoryEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	edgeIDs := m.outgoingEdges[startID]
	if edgeIDs == nil {
		return []*Edge{}, nil
	}

	edges := make([]*Edge, 0)
	for id := range edgeIDs {
		if edge := m.edges[id]; edge != nil && edge.EndNode == endID {
			edges = append(edges, m.copyEdge(edge))
		}
	}

	return edges, nil
}

// BulkCreateNodes creates multiple nodes efficiently.
func (m *MemoryEngine) BulkCreateNodes(nodes []*Node) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	// Validate all nodes first
	for _, node := range nodes {
		if node == nil {
			return ErrInvalidData
		}
		if node.ID == "" {
			return ErrInvalidID
		}
		if _, exists := m.nodes[node.ID]; exists {
			return ErrAlreadyExists
		}
	}

	// Insert all nodes
	for _, node := range nodes {
		stored := m.copyNode(node)
		m.nodes[node.ID] = stored

		for _, label := range node.Labels {
			if m.nodesByLabel[label] == nil {
				m.nodesByLabel[label] = make(map[NodeID]struct{})
			}
			m.nodesByLabel[label][node.ID] = struct{}{}
		}
	}

	return nil
}

// BulkCreateEdges creates multiple edges efficiently.
func (m *MemoryEngine) BulkCreateEdges(edges []*Edge) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	// Validate all edges first
	for _, edge := range edges {
		if edge == nil {
			return ErrInvalidData
		}
		if edge.ID == "" {
			return ErrInvalidID
		}
		if _, exists := m.edges[edge.ID]; exists {
			return ErrAlreadyExists
		}
		if _, exists := m.nodes[edge.StartNode]; !exists {
			return ErrNotFound
		}
		if _, exists := m.nodes[edge.EndNode]; !exists {
			return ErrNotFound
		}
	}

	// Insert all edges
	for _, edge := range edges {
		stored := m.copyEdge(edge)
		m.edges[edge.ID] = stored

		if m.outgoingEdges[edge.StartNode] == nil {
			m.outgoingEdges[edge.StartNode] = make(map[EdgeID]struct{})
		}
		m.outgoingEdges[edge.StartNode][edge.ID] = struct{}{}

		if m.incomingEdges[edge.EndNode] == nil {
			m.incomingEdges[edge.EndNode] = make(map[EdgeID]struct{})
		}
		m.incomingEdges[edge.EndNode][edge.ID] = struct{}{}
	}

	return nil
}

// Close closes the storage engine.
func (m *MemoryEngine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.nodes = nil
	m.edges = nil
	m.nodesByLabel = nil
	m.outgoingEdges = nil
	m.incomingEdges = nil

	return nil
}

// NodeCount returns the number of nodes.
func (m *MemoryEngine) NodeCount() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return 0, ErrStorageClosed
	}

	return int64(len(m.nodes)), nil
}

// EdgeCount returns the number of edges.
func (m *MemoryEngine) EdgeCount() (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return 0, ErrStorageClosed
	}

	return int64(len(m.edges)), nil
}

// copyNode creates a deep copy of a node.
func (m *MemoryEngine) copyNode(n *Node) *Node {
	if n == nil {
		return nil
	}

	copied := &Node{
		ID:           n.ID,
		Labels:       make([]string, len(n.Labels)),
		Properties:   make(map[string]any),
		CreatedAt:    n.CreatedAt,
		UpdatedAt:    n.UpdatedAt,
		DecayScore:   n.DecayScore,
		LastAccessed: n.LastAccessed,
		AccessCount:  n.AccessCount,
	}

	copy(copied.Labels, n.Labels)
	for k, v := range n.Properties {
		copied.Properties[k] = v
	}

	if n.Embedding != nil {
		copied.Embedding = make([]float32, len(n.Embedding))
		copy(copied.Embedding, n.Embedding)
	}

	return copied
}

// copyEdge creates a deep copy of an edge.
func (m *MemoryEngine) copyEdge(e *Edge) *Edge {
	if e == nil {
		return nil
	}

	copied := &Edge{
		ID:            e.ID,
		StartNode:     e.StartNode,
		EndNode:       e.EndNode,
		Type:          e.Type,
		Properties:    make(map[string]any),
		CreatedAt:     e.CreatedAt,
		Confidence:    e.Confidence,
		AutoGenerated: e.AutoGenerated,
	}

	for k, v := range e.Properties {
		copied.Properties[k] = v
	}

	return copied
}

// Verify MemoryEngine implements Engine interface
var _ Engine = (*MemoryEngine)(nil)
