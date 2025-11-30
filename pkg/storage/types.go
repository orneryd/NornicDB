// Package storage provides the storage engine interface and implementations for NornicDB.
//
// The storage layer is designed for Neo4j compatibility while adding NornicDB-specific
// extensions for memory decay, vector embeddings, and automatic relationship inference.
//
// Design Principles:
//   - Neo4j JSON export/import compatibility
//   - Testability through dependency injection
//   - Thread-safe implementations
//   - Property graph model (labeled property graph)
//
// Example Usage:
//
//	// Create storage engine
//	engine := storage.NewMemoryEngine()
//	defer engine.Close()
//
//	// Create nodes
//	node := &storage.Node{
//		ID:     storage.NodeID("user-123"),
//		Labels: []string{"User", "Person"},
//		Properties: map[string]any{
//			"name":  "Alice",
//			"email": "alice@example.com",
//		},
//		CreatedAt: time.Now(),
//	}
//	engine.CreateNode(node)
//
//	// Create relationships
//	edge := &storage.Edge{
//		ID:        storage.EdgeID("follows-1"),
//		StartNode: storage.NodeID("user-123"),
//		EndNode:   storage.NodeID("user-456"),
//		Type:      "FOLLOWS",
//		CreatedAt: time.Now(),
//	}
//	engine.CreateEdge(edge)
//
//	// Export to Neo4j format
//	nodes, _ := engine.AllNodes()
//	edges, _ := engine.AllEdges()
//	export := storage.ToNeo4jExport(nodes, edges)
//
//	// Save as JSON
//	data, _ := json.MarshalIndent(export, "", "  ")
//	os.WriteFile("graph-export.json", data, 0644)
package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Common errors
var (
	ErrNotFound         = errors.New("not found")
	ErrAlreadyExists    = errors.New("already exists")
	ErrInvalidID        = errors.New("invalid id")
	ErrInvalidData      = errors.New("invalid data")
	ErrInvalidEdge      = errors.New("invalid edge: start or end node not found")
	ErrStorageClosed    = errors.New("storage closed")
	ErrIterationStopped = errors.New("iteration stopped") // Sentinel to stop streaming early
)

// NodeID is a strongly-typed unique identifier for graph nodes.
//
// Using a custom type provides:
//   - Type safety (can't accidentally use EdgeID where NodeID is expected)
//   - Clear API semantics
//   - Future extensibility (could add methods)
//
// Example:
//
//	id := storage.NodeID("user-123")
//	node, err := engine.GetNode(id)
type NodeID string

// EdgeID is a strongly-typed unique identifier for graph edges (relationships).
//
// Similar to NodeID, provides type safety and API clarity.
//
// Example:
//
//	id := storage.EdgeID("follows-456")
//	edge, err := engine.GetEdge(id)
type EdgeID string

// Node represents a graph node (vertex) in the labeled property graph.
//
// Nodes follow the Neo4j data model with NornicDB-specific extensions for
// memory decay, semantic search, and access tracking. Nodes are the fundamental
// entities in the graph and can represent people, documents, concepts, or any
// other entity in your domain.
//
// Core Neo4j Fields:
//   - ID: Unique identifier (must be unique across all nodes)
//   - Labels: Type tags like ["Person", "User"] (Neo4j :Person:User)
//   - Properties: Key-value data (any JSON-serializable types)
//
// NornicDB Extensions (not exported to Neo4j):
//   - CreatedAt: When the node was first created
//   - UpdatedAt: Last modification timestamp
//   - DecayScore: Memory importance (1.0=fresh, 0.0=decayed)
//   - LastAccessed: Last time node was queried/updated
//   - AccessCount: Total access frequency
//   - Embedding: 1024-dim vector for semantic similarity
//
// Example 1 - Basic User Node:
//
//	node := &storage.Node{
//		ID:     storage.NodeID("user-alice"),
//		Labels: []string{"Person", "User"},
//		Properties: map[string]any{
//			"name":     "Alice Johnson",
//			"age":      30,
//			"email":    "alice@example.com",
//			"verified": true,
//		},
//		CreatedAt:   time.Now(),
//		DecayScore:  1.0, // Fresh memory
//		AccessCount: 0,
//	}
//	engine.CreateNode(node)
//
// Example 2 - Document Node with Metadata:
//
//	doc := &storage.Node{
//		ID:     storage.NodeID("doc-readme"),
//		Labels: []string{"Document", "Markdown"},
//		Properties: map[string]any{
//			"title":    "README.md",
//			"content":  "# Welcome to...",
//			"path":     "./README.md",
//			"size":     4096,
//			"language": "markdown",
//		},
//		CreatedAt: time.Now(),
//		Embedding: generateEmbedding("# Welcome to..."), // For semantic search
//	}
//
// Example 3 - Concept Node for Knowledge Graph:
//
//	concept := &storage.Node{
//		ID:     storage.NodeID("concept-database"),
//		Labels: []string{"Concept", "Technology"},
//		Properties: map[string]any{
//			"name":        "Database Systems",
//			"definition":  "Systems for storing and retrieving data",
//			"category":    "Software",
//			"importance":  "high",
//		},
//		DecayScore:   0.95, // Slightly aged but still relevant
//		AccessCount:  42,   // Accessed 42 times
//		LastAccessed: time.Now().Add(-24 * time.Hour),
//	}
//
// ELI12:
//
// Think of a Node like a character card in a trading card game:
//   - ID: The card's unique number (no two cards have the same)
//   - Labels: The types on the card (["Hero", "Warrior"])
//   - Properties: Stats on the card (name: "Alice", strength: 10, health: 100)
//
// NornicDB adds extra info:
//   - DecayScore: How "fresh" the card is (new cards = 1.0, old forgotten cards = 0.2)
//   - AccessCount: How many times you've played this card
//   - Embedding: A secret code that helps find similar cards
//
// Just like trading cards can be rare or common, frequently used or forgotten,
// Nodes track their usage and importance in the graph!
//
// Neo4j Compatibility:
//   - Labels map to Neo4j labels (e.g., :Person:User)
//   - Properties map to Neo4j properties
//   - ID must be unique across all nodes
//   - Extensions stored with "_" prefix in Neo4j exports
//
// Thread Safety:
//
//	Node structs are NOT thread-safe. The storage engine handles concurrency.
type Node struct {
	ID         NodeID         `json:"id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`

	// NornicDB extensions
	CreatedAt    time.Time `json:"-"`
	UpdatedAt    time.Time `json:"-"`
	DecayScore   float64   `json:"-"`
	LastAccessed time.Time `json:"-"`
	AccessCount  int64     `json:"-"`
	Embedding    []float32 `json:"-"` // Vector embedding for semantic search
}

// Edge represents a directed graph relationship (arc) between two nodes.
//
// Edges are directed connections that link nodes together, representing relationships
// like "Alice KNOWS Bob" or "Document CITES Paper". They follow the Neo4j relationship
// model with NornicDB extensions for automatic relationship inference and confidence scoring.
//
// Core Neo4j Fields:
//   - ID: Unique identifier for the relationship
//   - StartNode: Source node ID (where the arrow starts)
//   - EndNode: Target node ID (where the arrow points)
//   - Type: Relationship type (e.g., "KNOWS", "FOLLOWS", "CONTAINS")
//   - Properties: Key-value data about the relationship
//
// NornicDB Extensions:
//   - CreatedAt: When the relationship was created
//   - Confidence: How certain we are this relationship exists (0.0-1.0)
//   - AutoGenerated: True if detected by ML/inference, false if manually created
//
// Example 1 - Social Network Relationship:
//
//	edge := &storage.Edge{
//		ID:         storage.EdgeID("friendship-123"),
//		StartNode:  storage.NodeID("alice"),
//		EndNode:    storage.NodeID("bob"),
//		Type:       "KNOWS",
//		Properties: map[string]any{
//			"since":    "2020-01-15",
//			"strength": "close_friend",
//			"mutuality": true,
//		},
//		CreatedAt:     time.Now(),
//		Confidence:    1.0,  // Manually created = 100% certain
//		AutoGenerated: false,
//	}
//	engine.CreateEdge(edge)
//
// Example 2 - Document Citation:
//
//	citation := &storage.Edge{
//		ID:        storage.EdgeID("cite-paper-5"),
//		StartNode: storage.NodeID("paper-123"),
//		EndNode:   storage.NodeID("paper-456"),
//		Type:      "CITES",
//		Properties: map[string]any{
//			"context":    "Methods section",
//			"page":       12,
//			"importance": "high",
//		},
//		CreatedAt: time.Now(),
//		Confidence: 1.0,
//	}
//
// Example 3 - Auto-Detected Semantic Relationship:
//
//	// NornicDB inference engine detected similarity
//	autoEdge := &storage.Edge{
//		ID:            storage.EdgeID("similar-42"),
//		StartNode:     storage.NodeID("note-1"),
//		EndNode:       storage.NodeID("note-2"),
//		Type:          "SIMILAR_TO",
//		Confidence:    0.87,  // 87% confidence from embedding similarity
//		AutoGenerated: true,  // Created automatically
//		Properties: map[string]any{
//			"similarity":  0.87,
//			"method":      "cosine_similarity",
//			"detected_at": time.Now(),
//			"reason":      "High semantic similarity in embeddings",
//		},
//	}
//
// ELI12:
//
// Think of an Edge like a string connecting two beads (nodes):
//   - StartNode: The first bead (where you start)
//   - EndNode: The second bead (where the string goes to)
//   - Type: What kind of string? ("FRIENDS_WITH", "PARENT_OF", "LIKES")
//   - Properties: Info about the connection ("since when?", "how strong?")
//
// The arrow matters! "Alice KNOWS Bob" is different from "Bob KNOWS Alice"
// (they could both be true, but they're separate relationships).
//
// NornicDB's cool additions:
//   - Confidence: "I'm 85% sure these two things are related"
//   - AutoGenerated: "I found this connection myself!" vs "A human told me"
//
// Imagine your brain automatically connecting ideas: "Oh, these two notes
// seem related!" That's what AutoGenerated edges do - the system notices
// patterns and creates connections for you!
//
// Neo4j Compatibility:
//   - Type maps to Neo4j relationship type (e.g., -[:KNOWS]->)
//   - StartNode/EndNode map to Neo4j node IDs
//   - Properties map to Neo4j relationship properties
//   - Direction is always preserved (Neo4j requirement)
//
// Thread Safety:
//
//	Edge structs are NOT thread-safe. The storage engine handles concurrency.
type Edge struct {
	ID         EdgeID         `json:"id"`
	StartNode  NodeID         `json:"startNode"`
	EndNode    NodeID         `json:"endNode"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`

	// NornicDB extensions
	CreatedAt     time.Time `json:"-"`
	UpdatedAt     time.Time `json:"-"`
	Confidence    float64   `json:"-"`
	AutoGenerated bool      `json:"-"`
}

// Engine defines the storage engine interface for graph database operations.
//
// All Engine implementations MUST be:
//   - Thread-safe: Safe for concurrent access from multiple goroutines
//   - ACID-like: Operations are atomic within their scope
//   - Idempotent where appropriate: CreateNode fails if ID exists
//
// The interface provides standard graph database operations:
//   - CRUD for nodes and edges
//   - Label-based queries
//   - Graph traversal (outgoing/incoming edges)
//   - Bulk operations for import/export
//   - Statistics
//
// Implementations:
//   - MemoryEngine: In-memory storage for testing and small datasets
//   - BadgerEngine: Persistent disk storage (planned)
//
// Example Usage:
//
//	var engine storage.Engine
//	engine = storage.NewMemoryEngine()
//	defer engine.Close()
//
//	// Create data
//	node := &storage.Node{
//		ID:     "n1",
//		Labels: []string{"Person"},
//		Properties: map[string]any{"name": "Alice"},
//	}
//	if err := engine.CreateNode(node); err != nil {
//		log.Fatal(err)
//	}
//
//	// Query
//	people, _ := engine.GetNodesByLabel("Person")
//	fmt.Printf("Found %d people\n", len(people))
//
//	// Traversal
//	outgoing, _ := engine.GetOutgoingEdges("n1")
//	for _, edge := range outgoing {
//		fmt.Printf("%s -> %s [%s]\n", edge.StartNode, edge.EndNode, edge.Type)
//	}
type Engine interface {
	// Node operations
	CreateNode(node *Node) error
	GetNode(id NodeID) (*Node, error)
	UpdateNode(node *Node) error
	DeleteNode(id NodeID) error

	// Edge operations
	CreateEdge(edge *Edge) error
	GetEdge(id EdgeID) (*Edge, error)
	UpdateEdge(edge *Edge) error
	DeleteEdge(id EdgeID) error

	// Query operations
	GetNodesByLabel(label string) ([]*Node, error)
	GetOutgoingEdges(nodeID NodeID) ([]*Edge, error)
	GetIncomingEdges(nodeID NodeID) ([]*Edge, error)
	GetEdgesBetween(startID, endID NodeID) ([]*Edge, error)
	GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge
	AllNodes() ([]*Node, error)
	AllEdges() ([]*Edge, error)
	GetAllNodes() []*Node

	// Degree operations (for graph algorithms)
	GetInDegree(nodeID NodeID) int
	GetOutDegree(nodeID NodeID) int

	// Schema operations
	GetSchema() *SchemaManager

	// Bulk operations (for import)
	BulkCreateNodes(nodes []*Node) error
	BulkCreateEdges(edges []*Edge) error

	// Bulk delete operations (for async flush performance)
	BulkDeleteNodes(ids []NodeID) error
	BulkDeleteEdges(ids []EdgeID) error

	// Batch read operations (for traversal performance)
	// BatchGetNodes fetches multiple nodes in a single operation
	// Returns a map for O(1) lookup by ID
	BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error)

	// Lifecycle
	Close() error

	// Stats
	NodeCount() (int64, error)
	EdgeCount() (int64, error)
}

// Neo4jExport represents the Neo4j JSON export format.
// This is compatible with `neo4j-admin database dump` JSON output.
type Neo4jExport struct {
	Nodes         []Neo4jNode         `json:"nodes"`
	Relationships []Neo4jRelationship `json:"relationships"`
}

// Neo4jNode is the Neo4j JSON export format for nodes.
type Neo4jNode struct {
	ID         string         `json:"id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`
}

// Neo4jNodeRef is a reference to a node in Neo4j relationship format.
type Neo4jNodeRef struct {
	ID     string   `json:"id"`
	Labels []string `json:"labels,omitempty"`
}

// Neo4jRelationship is the Neo4j JSON export format for relationships.
// Supports both flat format (startNode/endNode strings) and APOC format (start/end objects).
type Neo4jRelationship struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`

	// Flat format (neo4j-admin dump)
	StartNode string `json:"startNode,omitempty"`
	EndNode   string `json:"endNode,omitempty"`

	// APOC format (apoc.export.json)
	Start Neo4jNodeRef `json:"start,omitempty"`
	End   Neo4jNodeRef `json:"end,omitempty"`
}

// GetStartID returns the start node ID supporting both Neo4j export formats.
//
// Neo4j exports can use two formats:
//  1. Flat format: startNode/endNode as strings (neo4j-admin dump)
//  2. APOC format: start/end as objects (apoc.export.json)
//
// This method abstracts the difference, always returning the start node ID.
//
// Example:
//
//	// Flat format
//	rel := &Neo4jRelationship{
//		StartNode: "user-123",
//	}
//	fmt.Println(rel.GetStartID()) // "user-123"
//
//	// APOC format
//	rel = &Neo4jRelationship{
//		Start: Neo4jNodeRef{ID: "user-456"},
//	}
//	fmt.Println(rel.GetStartID()) // "user-456"
func (r *Neo4jRelationship) GetStartID() string {
	if r.Start.ID != "" {
		return r.Start.ID
	}
	return r.StartNode
}

// GetEndID returns the end node ID regardless of format.
func (r *Neo4jRelationship) GetEndID() string {
	if r.End.ID != "" {
		return r.End.ID
	}
	return r.EndNode
}

// ToNeo4jExport converts NornicDB nodes and edges to Neo4j JSON export format.
//
// This function prepares data for export that can be imported into Neo4j
// using neo4j-admin or APOC procedures. NornicDB-specific fields (decay score,
// embeddings, access counts) are stored with "_" prefix to mark them as
// system properties.
//
// The output is compatible with:
//   - `neo4j-admin database import`
//   - `CALL apoc.import.json()`
//   - Standard Neo4j JSON format
//
// Example:
//
//	// Get all data
//	nodes, _ := engine.GetNodesByLabel("") // All nodes
//	edges, _ := engine.AllEdges()
//
//	// Convert to Neo4j format
//	export := storage.ToNeo4jExport(nodes, edges)
//
//	// Save as JSON
//	data, _ := json.MarshalIndent(export, "", "  ")
//	err := os.WriteFile("neo4j-export.json", data, 0644)
//
//	// Import into Neo4j
//	// $ neo4j-admin database import --nodes=neo4j-export.json full
//	// Or in Cypher:
//	// CALL apoc.import.json("file:///neo4j-export.json")
//
// NornicDB extensions are preserved as properties:
//
//	_decayScore, _lastAccessed, _accessCount, _confidence, _autoGenerated
func ToNeo4jExport(nodes []*Node, edges []*Edge) *Neo4jExport {
	export := &Neo4jExport{
		Nodes:         make([]Neo4jNode, len(nodes)),
		Relationships: make([]Neo4jRelationship, len(edges)),
	}

	for i, n := range nodes {
		export.Nodes[i] = Neo4jNode{
			ID:         string(n.ID),
			Labels:     n.Labels,
			Properties: n.mergeInternalProperties(),
		}
	}

	for i, e := range edges {
		props := make(map[string]any)
		for k, v := range e.Properties {
			props[k] = v
		}
		// Add edge-specific internal properties
		if e.Confidence > 0 {
			props["_confidence"] = e.Confidence
		}
		if e.AutoGenerated {
			props["_autoGenerated"] = e.AutoGenerated
		}
		if !e.CreatedAt.IsZero() {
			props["_createdAt"] = e.CreatedAt.Unix()
		}

		export.Relationships[i] = Neo4jRelationship{
			ID:         string(e.ID),
			StartNode:  string(e.StartNode),
			EndNode:    string(e.EndNode),
			Type:       e.Type,
			Properties: props,
		}
	}

	return export
}

// FromNeo4jExport converts Neo4j JSON export format to NornicDB nodes and edges.
//
// This function imports data exported from Neo4j, extracting NornicDB-specific
// properties (those with "_" prefix) back into their dedicated fields.
//
// Supports both export formats:
//   - neo4j-admin database dump (flat format)
//   - apoc.export.json (nested format)
//
// Example:
//
//	// Load Neo4j export file
//	data, _ := os.ReadFile("neo4j-export.json")
//
//	var export storage.Neo4jExport
//	json.Unmarshal(data, &export)
//
//	// Convert to NornicDB format
//	nodes, edges := storage.FromNeo4jExport(&export)
//
//	// Import into NornicDB
//	if err := engine.BulkCreateNodes(nodes); err != nil {
//		log.Fatal(err)
//	}
//	if err := engine.BulkCreateEdges(edges); err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Imported %d nodes, %d edges\n", len(nodes), len(edges))
//
// Returns nodes and edges ready for storage engine insertion.
func FromNeo4jExport(export *Neo4jExport) ([]*Node, []*Edge) {
	nodes := make([]*Node, len(export.Nodes))
	edges := make([]*Edge, len(export.Relationships))

	for i, n := range export.Nodes {
		// Copy properties
		props := make(map[string]any)
		for k, v := range n.Properties {
			props[k] = v
		}

		node := &Node{
			ID:         NodeID(n.ID),
			Labels:     n.Labels,
			Properties: props,
			DecayScore: 1.0, // Default
		}
		// Extract internal properties from the properties map
		node.ExtractInternalProperties()
		nodes[i] = node
	}

	for i, r := range export.Relationships {
		// Copy properties
		props := make(map[string]any)
		for k, v := range r.Properties {
			props[k] = v
		}

		edge := &Edge{
			ID:         EdgeID(r.ID),
			StartNode:  NodeID(r.GetStartID()),
			EndNode:    NodeID(r.GetEndID()),
			Type:       r.Type,
			Properties: props,
		}

		// Extract edge-specific internal properties
		if conf, ok := props["_confidence"].(float64); ok {
			edge.Confidence = conf
			delete(edge.Properties, "_confidence")
		}
		if auto, ok := props["_autoGenerated"].(bool); ok {
			edge.AutoGenerated = auto
			delete(edge.Properties, "_autoGenerated")
		}
		if created, ok := props["_createdAt"].(float64); ok {
			edge.CreatedAt = time.Unix(int64(created), 0)
			delete(edge.Properties, "_createdAt")
		}

		edges[i] = edge
	}

	return nodes, edges
}

// MarshalNeo4jJSON serializes to Neo4j-compatible JSON.
func (n *Node) MarshalNeo4jJSON() ([]byte, error) {
	neo4j := Neo4jNode{
		ID:         string(n.ID),
		Labels:     n.Labels,
		Properties: n.mergeInternalProperties(),
	}
	return json.Marshal(neo4j)
}

// mergeInternalProperties adds NornicDB-specific fields to properties.
func (n *Node) mergeInternalProperties() map[string]any {
	props := make(map[string]any)
	for k, v := range n.Properties {
		props[k] = v
	}

	// Add internal properties with _ prefix (Neo4j convention for system props)
	props["_createdAt"] = n.CreatedAt.Unix()
	props["_updatedAt"] = n.UpdatedAt.Unix()
	props["_decayScore"] = n.DecayScore
	props["_lastAccessed"] = n.LastAccessed.Unix()
	props["_accessCount"] = n.AccessCount

	return props
}

// ExtractInternalProperties extracts NornicDB-specific fields from properties.
func (n *Node) ExtractInternalProperties() {
	if n.Properties == nil {
		return
	}

	if v, ok := n.Properties["_createdAt"].(float64); ok {
		n.CreatedAt = time.Unix(int64(v), 0)
		delete(n.Properties, "_createdAt")
	}
	if v, ok := n.Properties["_updatedAt"].(float64); ok {
		n.UpdatedAt = time.Unix(int64(v), 0)
		delete(n.Properties, "_updatedAt")
	}
	if v, ok := n.Properties["_decayScore"].(float64); ok {
		n.DecayScore = v
		delete(n.Properties, "_decayScore")
	}
	if v, ok := n.Properties["_lastAccessed"].(float64); ok {
		n.LastAccessed = time.Unix(int64(v), 0)
		delete(n.Properties, "_lastAccessed")
	}
	if v, ok := n.Properties["_accessCount"].(float64); ok {
		n.AccessCount = int64(v)
		delete(n.Properties, "_accessCount")
	}
}

// =============================================================================
// STREAMING INTERFACE
// =============================================================================

// StreamingEngine extends Engine with streaming iteration support.
// This is optional - engines that don't support streaming will use
// the default AllNodes/AllEdges with chunked processing.
type StreamingEngine interface {
	Engine

	// StreamNodes iterates over all nodes without loading all into memory.
	// The callback is called for each node. Return an error to stop iteration.
	// Returns nil on successful completion, context.Canceled on cancellation.
	StreamNodes(ctx context.Context, fn func(node *Node) error) error

	// StreamEdges iterates over all edges without loading all into memory.
	StreamEdges(ctx context.Context, fn func(edge *Edge) error) error

	// StreamNodeChunks iterates over nodes in chunks for batch processing.
	// More efficient than StreamNodes when processing in batches.
	StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error
}

// NodeVisitor is a function called for each node during streaming.
type NodeVisitor func(node *Node) error

// EdgeVisitor is a function called for each edge during streaming.
type EdgeVisitor func(edge *Edge) error

// StreamNodesWithFallback provides streaming iteration with fallback.
// If the engine supports StreamingEngine, it uses that.
// Otherwise, it loads all nodes but processes them in chunks.
func StreamNodesWithFallback(ctx context.Context, engine Engine, chunkSize int, fn NodeVisitor) error {
	// Try streaming interface first
	if streamer, ok := engine.(StreamingEngine); ok {
		return streamer.StreamNodes(ctx, fn)
	}

	// Fallback: load all but process in chunks to allow GC between
	nodes, err := engine.AllNodes()
	if err != nil {
		return err
	}

	for i, node := range nodes {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := fn(node); err != nil {
			return err
		}

		// Nil out the reference to allow GC
		nodes[i] = nil

		// Hint GC every chunk
		if chunkSize > 0 && (i+1)%chunkSize == 0 {
			// runtime.GC() // Optional: enable for aggressive GC
		}
	}

	return nil
}

// StreamEdgesWithFallback provides streaming iteration with fallback.
func StreamEdgesWithFallback(ctx context.Context, engine Engine, chunkSize int, fn EdgeVisitor) error {
	// Try streaming interface first
	if streamer, ok := engine.(StreamingEngine); ok {
		return streamer.StreamEdges(ctx, fn)
	}

	// Fallback: load all but process in chunks
	edges, err := engine.AllEdges()
	if err != nil {
		return err
	}

	for i, edge := range edges {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := fn(edge); err != nil {
			return err
		}

		// Nil out the reference to allow GC
		edges[i] = nil
	}

	return nil
}

// NodeNeedsEmbedding checks if a node needs an embedding to be generated.
// Returns true if the node should have an embedding generated, false if it should be skipped.
//
// A node is skipped (returns false) if:
//   - It has an internal label (starts with '_')
//   - It already has an embedding
//   - It has the "embedding_skipped" property set
//   - It has "has_embedding" property explicitly set to false
//
// Example:
//
//	for _, node := range nodes {
//	    if storage.NodeNeedsEmbedding(node) {
//	        generateEmbedding(node)
//	    }
//	}
func NodeNeedsEmbedding(node *Node) bool {
	if node == nil {
		return false
	}

	// Skip internal nodes (labels starting with _)
	for _, label := range node.Labels {
		if len(label) > 0 && label[0] == '_' {
			return false
		}
	}

	// Skip if already has embedding
	if len(node.Embedding) > 0 {
		return false
	}

	// Skip if already processed (marked as skipped)
	if _, skipped := node.Properties["embedding_skipped"]; skipped {
		return false
	}

	// Skip if explicitly marked as not needing embedding
	if hasEmbed, ok := node.Properties["has_embedding"].(bool); ok && !hasEmbed {
		return false
	}

	return true
}

// CountNodesWithLabel counts nodes with a specific label using streaming.
func CountNodesWithLabel(ctx context.Context, engine Engine, label string) (int64, error) {
	var count int64

	err := StreamNodesWithFallback(ctx, engine, 1000, func(node *Node) error {
		for _, l := range node.Labels {
			if l == label {
				count++
				break
			}
		}
		return nil
	})

	return count, err
}

// CollectLabels collects all unique labels using streaming.
func CollectLabels(ctx context.Context, engine Engine) ([]string, error) {
	labelSet := make(map[string]struct{})

	err := StreamNodesWithFallback(ctx, engine, 1000, func(node *Node) error {
		for _, l := range node.Labels {
			labelSet[l] = struct{}{}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	labels := make([]string, 0, len(labelSet))
	for l := range labelSet {
		labels = append(labels, l)
	}
	return labels, nil
}

// CollectEdgeTypes collects all unique edge types using streaming.
func CollectEdgeTypes(ctx context.Context, engine Engine) ([]string, error) {
	typeSet := make(map[string]struct{})

	err := StreamEdgesWithFallback(ctx, engine, 1000, func(edge *Edge) error {
		typeSet[edge.Type] = struct{}{}
		return nil
	})

	if err != nil {
		return nil, err
	}

	types := make([]string, 0, len(typeSet))
	for t := range typeSet {
		types = append(types, t)
	}
	return types, nil
}
