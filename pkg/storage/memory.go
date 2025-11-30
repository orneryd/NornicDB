// Package storage provides storage engine implementations for NornicDB.
//
// The storage package defines the Engine interface and provides multiple implementations:
//   - MemoryEngine: In-memory storage for testing and small datasets
//   - BadgerEngine: Persistent disk-based storage (coming soon)
//
// All storage engines are thread-safe and support concurrent operations.
//
// Example Usage:
//
//	// Create in-memory storage
//	engine := storage.NewMemoryEngine()
//	defer engine.Close()
//
//	// Create a node
//	node := &storage.Node{
//		ID:     "user-001",
//		Labels: []string{"User"},
//		Properties: map[string]any{
//			"name": "Alice",
//			"email": "alice@example.com",
//		},
//		CreatedAt: time.Now(),
//	}
//
//	if err := engine.CreateNode(node); err != nil {
//		log.Fatal(err)
//	}
//
//	// Create an edge
//	edge := &storage.Edge{
//		ID:        "follows-001",
//		StartNode: "user-001",
//		EndNode:   "user-002",
//		Type:      "FOLLOWS",
//		CreatedAt: time.Now(),
//	}
//
//	if err := engine.CreateEdge(edge); err != nil {
//		log.Fatal(err)
//	}
//
//	// Query nodes by label
//	users, _ := engine.GetNodesByLabel("User")
//	fmt.Printf("Found %d users\n", len(users))
package storage

import (
	"fmt"
	"strings"
	"sync"
)

// normalizeLabel converts a label to lowercase for case-insensitive matching.
// This makes NornicDB compatible with Neo4j's case-insensitive label handling.
func normalizeLabel(label string) string {
	return strings.ToLower(label)
}

// MemoryEngine is a thread-safe in-memory graph storage implementation.
//
// Use Cases:
//   - Unit testing (no disk I/O, fast cleanup)
//   - Loading Neo4j exports into memory for analysis
//   - Small datasets that fit entirely in RAM
//   - Development and prototyping
//
// Features:
//   - Thread-safe: All operations use RWMutex for concurrent access
//   - Indexed: Maintains indexes for labels and edges for fast lookups
//   - Deep copies: Returns copies to prevent external mutation
//   - Bulk operations: Efficient batch insert for nodes and edges
//
// Performance Characteristics:
//   - Node lookup by ID: O(1)
//   - Node lookup by label: O(k) where k = nodes with that label
//   - Edge lookup: O(1)
//   - Outgoing/incoming edges: O(degree)
//   - Memory usage: ~200-500 bytes per node + properties
//
// Thread Safety:
//
//	All public methods are thread-safe. Multiple goroutines can safely
//	call any method concurrently.
//
// Example:
//
//	engine := storage.NewMemoryEngine()
//	defer engine.Close()
//
//	// Create nodes
//	nodes := []*storage.Node{
//		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}},
//		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"name": "Bob"}},
//	}
//	engine.BulkCreateNodes(nodes)
//
//	// Create edges
//	edges := []*storage.Edge{
//		{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS"},
//	}
//	engine.BulkCreateEdges(edges)
//
//	// Query
//	people, _ := engine.GetNodesByLabel("Person")
//	fmt.Printf("Found %d people\n", len(people))
//
//	outgoing, _ := engine.GetOutgoingEdges("n1")
//	fmt.Printf("Alice knows %d people\n", len(outgoing))
type MemoryEngine struct {
	mu    sync.RWMutex
	nodes map[NodeID]*Node
	edges map[EdgeID]*Edge

	// Indexes for efficient lookups
	nodesByLabel  map[string]map[NodeID]struct{}
	outgoingEdges map[NodeID]map[EdgeID]struct{}
	incomingEdges map[NodeID]map[EdgeID]struct{}

	// Schema management
	schema *SchemaManager

	closed bool
}

// NewMemoryEngine creates a new in-memory storage engine with empty indexes.
//
// This creates a fast, thread-safe, in-memory graph database perfect for testing,
// development, and small datasets. All data is stored in RAM and lost when the
// process exits. No disk I/O means extremely fast operations.
//
// Returns:
//   - *MemoryEngine ready for immediate concurrent use
//
// Example 1 - Basic Testing:
//
//	func TestMyGraph(t *testing.T) {
//		engine := storage.NewMemoryEngine()
//		defer engine.Close()
//		
//		// Fast, clean test environment
//		node := &storage.Node{
//			ID:     storage.NodeID("test-1"),
//			Labels: []string{"Test"},
//			Properties: map[string]any{"value": 42},
//		}
//		engine.CreateNode(node)
//		
//		// Verify
//		retrieved, _ := engine.GetNode(storage.NodeID("test-1"))
//		assert.Equal(t, 42, retrieved.Properties["value"])
//	}
//
// Example 2 - Loading Neo4j Export for Analysis:
//
//	// Load 10,000 nodes from Neo4j export into memory
//	engine := storage.NewMemoryEngine()
//	defer engine.Close()
//	
//	export := loadNeo4jExport("graph-export.json")
//	engine.BulkCreateNodes(export.Nodes)
//	engine.BulkCreateEdges(export.Edges)
//	
//	// Now analyze in memory (fast!)
//	users, _ := engine.GetNodesByLabel("User")
//	fmt.Printf("Loaded %d users\n", len(users))
//
// Example 3 - Temporary Processing Pipeline:
//
//	func processDocuments(docs []Document) *Graph {
//		engine := storage.NewMemoryEngine()
//		defer engine.Close()
//		
//		// Build graph in memory
//		for _, doc := range docs {
//			node := &storage.Node{
//				ID:     storage.NodeID(doc.ID),
//				Labels: []string{"Document"},
//				Properties: map[string]any{
//					"title":   doc.Title,
//					"content": doc.Content,
//				},
//			}
//			engine.CreateNode(node)
//		}
//		
//		// Detect relationships
//		detectRelationships(engine)
//		
//		// Export results
//		return exportGraph(engine)
//	}
//
// ELI12:
//
// Think of NewMemoryEngine like opening a new notebook:
//   - It's completely blank and ready to write in
//   - Everything you write stays in the notebook (RAM)
//   - When you close the notebook (program ends), everything disappears
//   - It's SUPER FAST because you're not saving to a hard drive
//
// Perfect for:
//   - Practice and learning (mess up all you want!)
//   - Testing your code (clean slate every test)
//   - Quick experiments (no setup needed)
//
// NOT good for:
//   - Saving important data (it disappears!)
//   - Huge datasets (runs out of memory)
//   - Production databases (use BadgerEngine instead)
//
// Memory Usage:
//   - ~200-500 bytes per node (depends on properties)
//   - ~100-200 bytes per edge
//   - 10,000 nodes ≈ 2-5 MB
//   - 100,000 nodes ≈ 20-50 MB
//
// Thread Safety:
//   Safe for concurrent use from multiple goroutines.
func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{
		nodes:         make(map[NodeID]*Node),
		edges:         make(map[EdgeID]*Edge),
		nodesByLabel:  make(map[string]map[NodeID]struct{}),
		outgoingEdges: make(map[NodeID]map[EdgeID]struct{}),
		incomingEdges: make(map[NodeID]map[EdgeID]struct{}),
		schema:        NewSchemaManager(),
	}
}

// CreateNode creates a new node in the storage.
//
// The node is deep-copied to prevent external mutations after storage.
// The ID must be unique - duplicate IDs return ErrAlreadyExists.
//
// Parameters:
//   - node: Node with unique ID, labels, and properties
//
// Returns:
//   - nil on success
//   - ErrInvalidData if node is nil
//   - ErrInvalidID if ID is empty
//   - ErrAlreadyExists if node with this ID exists
//   - ErrStorageClosed if engine is closed
//
// Example:
//
//	node := &storage.Node{
//		ID:     storage.NodeID("user-123"),
//		Labels: []string{"User", "Admin"},
//		Properties: map[string]any{
//			"name":  "Alice",
//			"email": "alice@example.com",
//			"age":   30,
//		},
//		CreatedAt: time.Now(),
//	}
//
//	if err := engine.CreateNode(node); err != nil {
//		if errors.Is(err, storage.ErrAlreadyExists) {
//			fmt.Println("Node already exists")
//		}
//		return err
//	}
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

	// Check unique constraints for all labels and properties
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			if err := m.schema.CheckUniqueConstraint(label, propName, propValue, ""); err != nil {
				return fmt.Errorf("constraint violation: %w", err)
			}
		}
	}

	// Deep copy to prevent external mutation
	stored := m.copyNode(node)
	m.nodes[node.ID] = stored

	// Update label index (normalized to lowercase for case-insensitive matching)
	for _, label := range node.Labels {
		normalLabel := normalizeLabel(label)
		if m.nodesByLabel[normalLabel] == nil {
			m.nodesByLabel[normalLabel] = make(map[NodeID]struct{})
		}
		m.nodesByLabel[normalLabel][node.ID] = struct{}{}
	}

	// Register unique constraint values
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			m.schema.RegisterUniqueValue(label, propName, propValue, node.ID)
		}
	}

	return nil
}

// GetNode retrieves a node by its unique ID.
//
// Returns a deep copy of the node to prevent external mutations.
//
// Parameters:
//   - id: Unique node identifier
//
// Returns:
//   - Node copy on success
//   - ErrInvalidID if ID is empty
//   - ErrNotFound if node doesn't exist
//   - ErrStorageClosed if engine is closed
//
// Example:
//
//	node, err := engine.GetNode("user-123")
//	if err != nil {
//		if errors.Is(err, storage.ErrNotFound) {
//			fmt.Println("Node not found")
//		}
//		return err
//	}
//
//	fmt.Printf("User: %s\n", node.Properties["name"])
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

	// Remove from old label indexes (normalized for case-insensitive matching)
	for _, label := range existing.Labels {
		normalLabel := normalizeLabel(label)
		if m.nodesByLabel[normalLabel] != nil {
			delete(m.nodesByLabel[normalLabel], node.ID)
		}
	}

	// Store updated node
	stored := m.copyNode(node)
	m.nodes[node.ID] = stored

	// Update label index (normalized for case-insensitive matching)
	for _, label := range node.Labels {
		normalLabel := normalizeLabel(label)
		if m.nodesByLabel[normalLabel] == nil {
			m.nodesByLabel[normalLabel] = make(map[NodeID]struct{})
		}
		m.nodesByLabel[normalLabel][node.ID] = struct{}{}
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

	// Remove from label indexes (normalized for case-insensitive matching)
	for _, label := range node.Labels {
		normalLabel := normalizeLabel(label)
		if m.nodesByLabel[normalLabel] != nil {
			delete(m.nodesByLabel[normalLabel], id)
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

// BulkDeleteNodes removes multiple nodes and their edges.
// This is more efficient than calling DeleteNode repeatedly.
func (m *MemoryEngine) BulkDeleteNodes(ids []NodeID) error {
	if len(ids) == 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	for _, id := range ids {
		if id == "" {
			continue
		}

		node, exists := m.nodes[id]
		if !exists {
			continue // Best effort - skip not found
		}

		// Remove from label index
		for _, label := range node.Labels {
			normalLabel := normalizeLabel(label)
			if m.nodesByLabel[normalLabel] != nil {
				delete(m.nodesByLabel[normalLabel], id)
			}
		}

		// Delete all edges involving this node
		if m.outgoingEdges[id] != nil {
			for edgeID := range m.outgoingEdges[id] {
				if edge := m.edges[edgeID]; edge != nil {
					if m.incomingEdges[edge.EndNode] != nil {
						delete(m.incomingEdges[edge.EndNode], edgeID)
					}
				}
				delete(m.edges, edgeID)
			}
			delete(m.outgoingEdges, id)
		}

		if m.incomingEdges[id] != nil {
			for edgeID := range m.incomingEdges[id] {
				if edge := m.edges[edgeID]; edge != nil {
					if m.outgoingEdges[edge.StartNode] != nil {
						delete(m.outgoingEdges[edge.StartNode], edgeID)
					}
				}
				delete(m.edges, edgeID)
			}
			delete(m.incomingEdges, id)
		}

		delete(m.nodes, id)
	}

	return nil
}

// BulkDeleteEdges removes multiple edges.
// This is more efficient than calling DeleteEdge repeatedly.
func (m *MemoryEngine) BulkDeleteEdges(ids []EdgeID) error {
	if len(ids) == 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrStorageClosed
	}

	for _, id := range ids {
		if id == "" {
			continue
		}

		edge, exists := m.edges[id]
		if !exists {
			continue // Best effort - skip not found
		}

		// Remove from indexes
		if m.outgoingEdges[edge.StartNode] != nil {
			delete(m.outgoingEdges[edge.StartNode], id)
		}
		if m.incomingEdges[edge.EndNode] != nil {
			delete(m.incomingEdges[edge.EndNode], id)
		}

		delete(m.edges, id)
	}

	return nil
}

// BatchGetNodes fetches multiple nodes by ID in a single operation.
// Returns a map for O(1) lookup. Missing nodes are not included.
func (m *MemoryEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	if len(ids) == 0 {
		return make(map[NodeID]*Node), nil
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	result := make(map[NodeID]*Node, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if node, exists := m.nodes[id]; exists {
			result[id] = copyNode(node)
		}
	}

	return result, nil
}

// GetNodesByLabel returns all nodes that have the specified label.
//
// Uses an index for O(k) performance where k = number of nodes with this label.
// Returns deep copies of all matching nodes.
//
// Parameters:
//   - label: Label to search for (case-sensitive)
//
// Returns:
//   - Slice of matching nodes (empty slice if none found)
//   - ErrStorageClosed if engine is closed
//
// Example:
//
//	// Get all users
//	users, err := engine.GetNodesByLabel("User")
//	if err != nil {
//		return err
//	}
//
//	fmt.Printf("Found %d users:\n", len(users))
//	for _, user := range users {
//		fmt.Printf("  - %s\n", user.Properties["name"])
//	}
//
//	// Get all admins
//	admins, _ := engine.GetNodesByLabel("Admin")
//	fmt.Printf("Found %d admins\n", len(admins))
//
// Note: A node can have multiple labels. This returns nodes where the
// specified label is present in the Labels slice.
func (m *MemoryEngine) GetNodesByLabel(label string) ([]*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	// Normalize label for case-insensitive matching (Neo4j compatible)
	nodeIDs := m.nodesByLabel[normalizeLabel(label)]
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

// GetOutgoingEdges returns all edges where the given node is the source.
//
// Uses an index for O(degree) performance where degree = number of outgoing edges.
// Returns deep copies of all matching edges.
//
// Parameters:
//   - nodeID: Source node ID
//
// Returns:
//   - Slice of outgoing edges (empty if none)
//   - ErrInvalidID if ID is empty
//   - ErrStorageClosed if engine is closed
//
// Example:
//
//	// Find who Alice follows
//	outgoing, err := engine.GetOutgoingEdges("user-alice")
//	if err != nil {
//		return err
//	}
//
//	fmt.Printf("Alice follows %d people:\n", len(outgoing))
//	for _, edge := range outgoing {
//		target, _ := engine.GetNode(edge.EndNode)
//		fmt.Printf("  - %s (%s)\n", target.Properties["name"], edge.Type)
//	}
//
// Use Case:
//
//	For a social graph, this finds all people a user follows.
//	For a dependency graph, this finds all dependencies of a package.
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

// GetIncomingEdges returns all edges where the given node is the target.
//
// Uses an index for O(degree) performance where degree = number of incoming edges.
// Returns deep copies of all matching edges.
//
// Parameters:
//   - nodeID: Target node ID
//
// Returns:
//   - Slice of incoming edges (empty if none)
//   - ErrInvalidID if ID is empty
//   - ErrStorageClosed if engine is closed
//
// Example:
//
//	// Find who follows Alice
//	incoming, err := engine.GetIncomingEdges("user-alice")
//	if err != nil {
//		return err
//	}
//
//	fmt.Printf("Alice has %d followers:\n", len(incoming))
//	for _, edge := range incoming {
//		source, _ := engine.GetNode(edge.StartNode)
//		fmt.Printf("  - %s\n", source.Properties["name"])
//	}
//
// Use Case:
//
//	For a social graph, this finds all followers of a user.
//	For a dependency graph, this finds all packages that depend on this one.
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

// BulkCreateNodes creates multiple nodes in a single transaction.
//
// This is more efficient than calling CreateNode repeatedly because:
//   - Single lock acquisition for all nodes
//   - Atomic operation (all succeed or all fail)
//   - Batch index updates
//
// All nodes are validated before any are inserted. If validation fails,
// no nodes are created.
//
// Parameters:
//   - nodes: Slice of nodes to create
//
// Returns:
//   - nil on success (all nodes created)
//   - ErrInvalidData if any node is invalid
//   - ErrAlreadyExists if any ID already exists
//   - ErrStorageClosed if engine is closed
//
// Example:
//
//	nodes := []*storage.Node{
//		{ID: "u1", Labels: []string{"User"}, Properties: map[string]any{"name": "Alice"}},
//		{ID: "u2", Labels: []string{"User"}, Properties: map[string]any{"name": "Bob"}},
//		{ID: "u3", Labels: []string{"User"}, Properties: map[string]any{"name": "Carol"}},
//	}
//
//	if err := engine.BulkCreateNodes(nodes); err != nil {
//		log.Fatal(err) // All or nothing - either all 3 created or none
//	}
//
//	fmt.Println("Created 3 users in one operation")
//
// Performance:
//
//	Approximately 10x faster than individual CreateNode calls for 100+ nodes.
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

	// Check unique constraints for all nodes BEFORE inserting any
	// This ensures atomicity - either all nodes are inserted or none
	for _, node := range nodes {
		for _, label := range node.Labels {
			for propName, propValue := range node.Properties {
				if err := m.schema.CheckUniqueConstraint(label, propName, propValue, ""); err != nil {
					return fmt.Errorf("constraint violation: %w", err)
				}
			}
		}
	}

	// Insert all nodes
	for _, node := range nodes {
		stored := m.copyNode(node)
		m.nodes[node.ID] = stored

		for _, label := range node.Labels {
			normalLabel := normalizeLabel(label)
			if m.nodesByLabel[normalLabel] == nil {
				m.nodesByLabel[normalLabel] = make(map[NodeID]struct{})
			}
			m.nodesByLabel[normalLabel][node.ID] = struct{}{}
		}

		// Register unique constraint values
		for _, label := range node.Labels {
			for propName, propValue := range node.Properties {
				m.schema.RegisterUniqueValue(label, propName, propValue, node.ID)
			}
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

// Close closes the storage engine and releases all memory.
//
// After Close(), all subsequent operations will return ErrStorageClosed.
// All internal maps are set to nil to allow garbage collection.
//
// This method is idempotent - calling Close() multiple times is safe.
//
// Example:
//
//	engine := storage.NewMemoryEngine()
//	defer engine.Close() // Always close to release memory
//
//	// Use engine...
//	node := &storage.Node{...}
//	engine.CreateNode(node)
//
//	// Explicit close
//	if err := engine.Close(); err != nil {
//		log.Printf("Close error: %v", err)
//	}
//
//	// Further operations will fail
//	_, err := engine.GetNode("test")
//	fmt.Println(errors.Is(err, storage.ErrStorageClosed)) // true
//
// Returns nil (Close never fails for MemoryEngine).
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

// GetInDegree returns the number of incoming edges to a node.
func (m *MemoryEngine) GetInDegree(nodeID NodeID) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return 0
	}

	edgeIDs := m.incomingEdges[nodeID]
	return len(edgeIDs)
}

// GetOutDegree returns the number of outgoing edges from a node.
func (m *MemoryEngine) GetOutDegree(nodeID NodeID) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return 0
	}

	edgeIDs := m.outgoingEdges[nodeID]
	return len(edgeIDs)
}

// GetSchema returns the schema manager for constraint and index management.
func (m *MemoryEngine) GetSchema() *SchemaManager {
	return m.schema
}

// ============================================================================
// Transaction Support - Unlocked Methods
// ============================================================================
// These methods are used internally by Transaction.Commit() and assume the
// caller already holds the write lock. Do NOT call these directly.

// createNodeUnlocked creates a node without acquiring a lock.
// Used internally by Transaction.Commit(). Caller must hold m.mu.Lock().
func (m *MemoryEngine) createNodeUnlocked(node *Node) {
	// Deep copy to prevent external mutation
	stored := m.copyNode(node)
	m.nodes[node.ID] = stored

	// Update label index (normalized for case-insensitive matching)
	for _, label := range node.Labels {
		normalLabel := normalizeLabel(label)
		if m.nodesByLabel[normalLabel] == nil {
			m.nodesByLabel[normalLabel] = make(map[NodeID]struct{})
		}
		m.nodesByLabel[normalLabel][node.ID] = struct{}{}
	}

	// Register unique constraint values
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			m.schema.RegisterUniqueValue(label, propName, propValue, node.ID)
		}
	}
}

// updateNodeUnlocked updates a node without acquiring a lock.
// Used internally by Transaction.Commit(). Caller must hold m.mu.Lock().
func (m *MemoryEngine) updateNodeUnlocked(node *Node) {
	existing, exists := m.nodes[node.ID]
	if !exists {
		return
	}

	// Remove from old label indexes (normalized for case-insensitive matching)
	for _, label := range existing.Labels {
		normalLabel := normalizeLabel(label)
		if m.nodesByLabel[normalLabel] != nil {
			delete(m.nodesByLabel[normalLabel], node.ID)
		}
	}

	// Store updated node
	stored := m.copyNode(node)
	m.nodes[node.ID] = stored

	// Update label index (normalized for case-insensitive matching)
	for _, label := range node.Labels {
		normalLabel := normalizeLabel(label)
		if m.nodesByLabel[normalLabel] == nil {
			m.nodesByLabel[normalLabel] = make(map[NodeID]struct{})
		}
		m.nodesByLabel[normalLabel][node.ID] = struct{}{}
	}
}

// deleteNodeUnlocked deletes a node without acquiring a lock.
// Used internally by Transaction.Commit(). Caller must hold m.mu.Lock().
func (m *MemoryEngine) deleteNodeUnlocked(id NodeID) {
	node, exists := m.nodes[id]
	if !exists {
		return
	}

	// Remove from label indexes (normalized for case-insensitive matching)
	for _, label := range node.Labels {
		normalLabel := normalizeLabel(label)
		if m.nodesByLabel[normalLabel] != nil {
			delete(m.nodesByLabel[normalLabel], id)
		}
	}

	// Delete all outgoing edges
	if outgoing := m.outgoingEdges[id]; outgoing != nil {
		for edgeID := range outgoing {
			edge := m.edges[edgeID]
			if edge != nil {
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
				if outgoing := m.outgoingEdges[edge.StartNode]; outgoing != nil {
					delete(outgoing, edgeID)
				}
			}
			delete(m.edges, edgeID)
		}
		delete(m.incomingEdges, id)
	}

	// Delete the node itself
	delete(m.nodes, id)
}

// createEdgeUnlocked creates an edge without acquiring a lock.
// Used internally by Transaction.Commit(). Caller must hold m.mu.Lock().
func (m *MemoryEngine) createEdgeUnlocked(edge *Edge) {
	// Deep copy
	stored := m.copyEdge(edge)
	m.edges[edge.ID] = stored

	// Update outgoing edges index
	if m.outgoingEdges[edge.StartNode] == nil {
		m.outgoingEdges[edge.StartNode] = make(map[EdgeID]struct{})
	}
	m.outgoingEdges[edge.StartNode][edge.ID] = struct{}{}

	// Update incoming edges index
	if m.incomingEdges[edge.EndNode] == nil {
		m.incomingEdges[edge.EndNode] = make(map[EdgeID]struct{})
	}
	m.incomingEdges[edge.EndNode][edge.ID] = struct{}{}
}

// deleteEdgeUnlocked deletes an edge without acquiring a lock.
// Used internally by Transaction.Commit(). Caller must hold m.mu.Lock().
func (m *MemoryEngine) deleteEdgeUnlocked(id EdgeID) {
	edge, exists := m.edges[id]
	if !exists {
		return
	}

	// Remove from outgoing index
	if outgoing := m.outgoingEdges[edge.StartNode]; outgoing != nil {
		delete(outgoing, id)
	}

	// Remove from incoming index
	if incoming := m.incomingEdges[edge.EndNode]; incoming != nil {
		delete(incoming, id)
	}

	// Delete the edge
	delete(m.edges, id)
}

// BeginTransaction creates a new transaction bound to this engine.
func (m *MemoryEngine) BeginTransaction() *Transaction {
	return NewTransaction(m)
}

// Verify MemoryEngine implements Engine interface
var _ Engine = (*MemoryEngine)(nil)
