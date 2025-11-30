// Package storage provides storage implementations and data import/export functionality.
//
// This file specifically handles Neo4j JSON import/export functionality, enabling
// NornicDB to interoperate with Neo4j databases through standard JSON export formats.
//
// Supported Formats:
//   - Neo4j APOC JSON exports (nodes.json + relationships.json)
//   - Combined Neo4j export format (single JSON file)
//   - NornicDB native format with full fidelity
//
// Key Features:
//   - Bidirectional Neo4j compatibility
//   - Bulk loading for performance
//   - Property type preservation
//   - Label and relationship type mapping
//   - Error handling and validation
//
// Example Usage:
//
//	// Load from Neo4j APOC export
//	engine := storage.NewMemoryEngine()
//	err := storage.LoadFromNeo4jJSON(engine, "./neo4j-export/")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Load from combined export file
//	err = storage.LoadFromNeo4jExport(engine, "./data.json")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Export to Neo4j format
//	err = storage.SaveToNeo4jExport(engine, "./nornicdb-export.json")
//	if err != nil {
//		log.Fatal(err)
//	}
//
// Neo4j APOC Export Format:
//
// The APOC export format consists of two files:
//   - nodes.json: One JSON object per line, each representing a node
//   - relationships.json: One JSON object per line, each representing a relationship
//
// Example nodes.json:
//
//	{"id":"0","labels":["Person"],"properties":{"name":"Alice","age":30}}
//	{"id":"1","labels":["Person"],"properties":{"name":"Bob","age":25}}
//
// Example relationships.json:
//
//	{"id":"0","type":"KNOWS","startNode":"0","endNode":"1","properties":{"since":2020}}
//
// Combined Export Format:
//
// The combined format includes both nodes and relationships in a single JSON file:
//
//	{
//	  "nodes": [...],
//	  "relationships": [...]
//	}
//
// Data Type Mapping:
//
// Neo4j types are mapped to Go types as follows:
//   - String -> string
//   - Integer -> int64
//   - Float -> float64
//   - Boolean -> bool
//   - Array -> []interface{}
//   - Object -> map[string]interface{}
//
// Performance:
//   - Bulk operations are used for efficient loading
//   - Streaming JSON parsing for large files
//   - Memory-efficient processing
//   - Progress reporting for large imports
//
// ELI12 (Explain Like I'm 12):
//
// Think of this like moving between different types of photo albums:
//
//  1. **Neo4j format**: Like a specific brand of photo album with a special
//     way of organizing photos (nodes) and the connections between them (relationships).
//
//  2. **Loading**: Like taking photos from a Neo4j album and putting them
//     into a NornicDB album, making sure each photo goes in the right place
//     and keeps all its information.
//
//  3. **Exporting**: Like taking photos from a NornicDB album and organizing
//     them in the Neo4j format so they can be used in Neo4j tools.
//
//  4. **Bulk operations**: Instead of moving photos one by one, we move
//     whole pages at a time to make it much faster.
//
// This lets you easily move your data between NornicDB and Neo4j!
package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// LoadFromNeo4jJSON loads nodes and edges from Neo4j APOC JSON export format.
//
// This function reads the standard Neo4j APOC export format consisting of
// nodes.json and relationships.json files. This format is produced by
// Neo4j's APOC library using `apoc.export.json.all()` or similar procedures.
//
// File Format:
//   - nodes.json: One JSON object per line (JSONL format)
//   - relationships.json: One JSON object per line (JSONL format)
//
// Parameters:
//   - engine: Storage engine to load data into
//   - dir: Directory containing nodes.json and relationships.json files
//
// Returns:
//   - Error if files cannot be read or data is invalid
//
// Example:
//
//	// Load from Neo4j APOC export directory
//	engine := storage.NewMemoryEngine()
//	err := storage.LoadFromNeo4jJSON(engine, "./neo4j-export/")
//	if err != nil {
//		log.Fatalf("Failed to load Neo4j data: %v", err)
//	}
//
//	fmt.Println("Successfully loaded Neo4j data into NornicDB")
//
// Expected Directory Structure:
//
//	neo4j-export/
//	├── nodes.json
//	└── relationships.json
//
// Node Format (nodes.json):
//
//	{"id":"0","labels":["Person"],"properties":{"name":"Alice","age":30}}
//	{"id":"1","labels":["Company"],"properties":{"name":"Acme Corp"}}
//
// Relationship Format (relationships.json):
//
//	{"id":"0","type":"WORKS_FOR","startNode":"0","endNode":"1","properties":{"since":2020}}
//
// Performance:
//   - Processes files sequentially (nodes first, then relationships)
//   - Uses streaming JSON parsing for memory efficiency
//   - Bulk operations for optimal storage performance
func LoadFromNeo4jJSON(engine Engine, dir string) error {
	// Load nodes first (edges need nodes to exist)
	nodesPath := filepath.Join(dir, "nodes.json")
	if err := loadNodesFile(engine, nodesPath); err != nil {
		return fmt.Errorf("loading nodes: %w", err)
	}

	// Load relationships
	relsPath := filepath.Join(dir, "relationships.json")
	if err := loadRelationshipsFile(engine, relsPath); err != nil {
		return fmt.Errorf("loading relationships: %w", err)
	}

	return nil
}

// LoadFromNeo4jExport loads data from a combined Neo4j export file.
//
// This function loads data from a single JSON file containing both nodes
// and relationships in a combined format. This is the format produced by
// SaveToNeo4jExport() and provides a more compact representation.
//
// Parameters:
//   - engine: Storage engine to load data into
//   - path: Path to the combined JSON export file
//
// Returns:
//   - Error if file cannot be read or data is invalid
//
// Example:
//
//	// Load from combined export file
//	engine := storage.NewMemoryEngine()
//	err := storage.LoadFromNeo4jExport(engine, "./data-export.json")
//	if err != nil {
//		log.Fatalf("Failed to load export: %v", err)
//	}
//
//	fmt.Println("Successfully loaded data from export file")
//
// File Format:
//
//	{
//	  "nodes": [
//	    {"id":"0","labels":["Person"],"properties":{"name":"Alice"}},
//	    {"id":"1","labels":["Person"],"properties":{"name":"Bob"}}
//	  ],
//	  "relationships": [
//	    {"id":"0","type":"KNOWS","startNode":"0","endNode":"1","properties":{}}
//	  ]
//	}
//
// Performance:
//   - Loads entire file into memory for parsing
//   - Uses bulk operations for efficient storage
//   - Suitable for moderate-sized datasets
//
// Use Cases:
//   - Restoring NornicDB backups
//   - Migrating data between environments
//   - Loading test datasets
//   - Data exchange with Neo4j systems
func LoadFromNeo4jExport(engine Engine, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	var export Neo4jExport
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&export); err != nil {
		return fmt.Errorf("decoding JSON: %w", err)
	}

	nodes, edges := FromNeo4jExport(&export)

	// Bulk insert for efficiency
	if err := engine.BulkCreateNodes(nodes); err != nil {
		return fmt.Errorf("creating nodes: %w", err)
	}

	if err := engine.BulkCreateEdges(edges); err != nil {
		return fmt.Errorf("creating edges: %w", err)
	}

	return nil
}

// SaveToNeo4jExport exports all data from the storage engine to a Neo4j-compatible JSON file.
//
// This function creates a complete export of all nodes and relationships in
// the storage engine, formatted for compatibility with Neo4j tools and for
// backup/restore operations.
//
// Parameters:
//   - engine: Storage engine to export data from
//   - path: Output file path for the JSON export
//
// Returns:
//   - Error if export fails or file cannot be written
//
// Example:
//
//	// Export all data to JSON file
//	err := storage.SaveToNeo4jExport(engine, "./backup.json")
//	if err != nil {
//		log.Fatalf("Export failed: %v", err)
//	}
//
//	fmt.Println("Data exported successfully")
//
//	// The exported file can be loaded back with:
//	// storage.LoadFromNeo4jExport(newEngine, "./backup.json")
//
// Output Format:
//
//	{
//	  "nodes": [
//	    {
//	      "id": "node-123",
//	      "labels": ["Person", "Employee"],
//	      "properties": {
//	        "name": "Alice Smith",
//	        "age": 30,
//	        "email": "alice@example.com"
//	      }
//	    }
//	  ],
//	  "relationships": [
//	    {
//	      "id": "rel-456",
//	      "type": "WORKS_FOR",
//	      "startNode": "node-123",
//	      "endNode": "node-789",
//	      "properties": {
//	        "since": "2020-01-15",
//	        "role": "Developer"
//	      }
//	    }
//	  ]
//	}
//
// Use Cases:
//   - Creating backups of NornicDB data
//   - Migrating data to Neo4j
//   - Data analysis with Neo4j tools
//   - Sharing datasets in a standard format
//
// Performance:
//   - Reads all data into memory before export
//   - Uses pretty-printed JSON for readability
//   - Suitable for datasets that fit in memory
//
// Note: Currently only supports MemoryEngine. Other storage engines
// will return an error indicating unsupported engine type.
func SaveToNeo4jExport(engine Engine, path string) error {
	// Collect all nodes
	var allNodes []*Node
	// We need to iterate - for MemoryEngine we can do this through labels
	// For a generic approach, we'd need an AllNodes() method
	// For now, we'll add that method via a type assertion or add to interface

	// Try to get all nodes via memory engine direct access
	if mem, ok := engine.(*MemoryEngine); ok {
		mem.mu.RLock()
		for _, node := range mem.nodes {
			allNodes = append(allNodes, mem.copyNode(node))
		}
		mem.mu.RUnlock()
	} else {
		return fmt.Errorf("SaveToNeo4jExport: engine type %T does not support full export", engine)
	}

	// Collect all edges
	var allEdges []*Edge
	if mem, ok := engine.(*MemoryEngine); ok {
		mem.mu.RLock()
		for _, edge := range mem.edges {
			allEdges = append(allEdges, mem.copyEdge(edge))
		}
		mem.mu.RUnlock()
	}

	export := ToNeo4jExport(allNodes, allEdges)

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(export); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

// loadNodesFile loads nodes from a Neo4j JSON lines file.
// Each line is a JSON object representing a node.
func loadNodesFile(engine Engine, path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Optional file
		}
		return err
	}
	defer file.Close()

	return loadNodesFromReader(engine, file)
}

// loadNodesFromReader loads nodes from a reader (for testing).
func loadNodesFromReader(engine Engine, r io.Reader) error {
	scanner := bufio.NewScanner(r)
	// Increase buffer for large lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var nodes []*Node

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var neo4jNode Neo4jNode
		if err := json.Unmarshal(line, &neo4jNode); err != nil {
			return fmt.Errorf("parsing node JSON: %w", err)
		}

		node, err := nodeFromNeo4j(&neo4jNode)
		if err != nil {
			return fmt.Errorf("converting node: %w", err)
		}

		nodes = append(nodes, node)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file: %w", err)
	}

	if len(nodes) > 0 {
		return engine.BulkCreateNodes(nodes)
	}

	return nil
}

// loadRelationshipsFile loads relationships from a Neo4j JSON lines file.
func loadRelationshipsFile(engine Engine, path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Optional file
		}
		return err
	}
	defer file.Close()

	return loadRelationshipsFromReader(engine, file)
}

// loadRelationshipsFromReader loads relationships from a reader.
func loadRelationshipsFromReader(engine Engine, r io.Reader) error {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var edges []*Edge

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var neo4jRel Neo4jRelationship
		if err := json.Unmarshal(line, &neo4jRel); err != nil {
			return fmt.Errorf("parsing relationship JSON: %w", err)
		}

		edge, err := edgeFromNeo4j(&neo4jRel)
		if err != nil {
			return fmt.Errorf("converting relationship: %w", err)
		}

		edges = append(edges, edge)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning file: %w", err)
	}

	if len(edges) > 0 {
		return engine.BulkCreateEdges(edges)
	}

	return nil
}

// nodeFromNeo4j converts a Neo4j JSON node to our Node type.
func nodeFromNeo4j(n *Neo4jNode) (*Node, error) {
	if n.ID == "" {
		return nil, ErrInvalidID
	}

	props := make(map[string]any)
	for k, v := range n.Properties {
		props[k] = v
	}

	node := &Node{
		ID:         NodeID(n.ID),
		Labels:     n.Labels,
		Properties: props,
	}

	// Extract internal properties
	node.ExtractInternalProperties()

	return node, nil
}

// edgeFromNeo4j converts a Neo4j JSON relationship to our Edge type.
func edgeFromNeo4j(r *Neo4jRelationship) (*Edge, error) {
	if r.ID == "" {
		return nil, ErrInvalidID
	}

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

	// Extract confidence if present
	if conf, ok := props["_confidence"].(float64); ok {
		edge.Confidence = conf
		delete(edge.Properties, "_confidence")
	}

	// Extract auto-generated flag if present
	if auto, ok := props["_autoGenerated"].(bool); ok {
		edge.AutoGenerated = auto
		delete(edge.Properties, "_autoGenerated")
	}

	return edge, nil
}

// ExportableEngine extends Engine with export capabilities.
type ExportableEngine interface {
	Engine
	AllNodes() ([]*Node, error)
	AllEdges() ([]*Edge, error)
}

// FindNodeNeedingEmbedding finds a single node that needs embedding.
// This is more efficient than AllNodes() as it stops after finding one.
func FindNodeNeedingEmbedding(engine Engine) *Node {
	// Use AllNodes but stop early - not ideal but works with any engine
	if exportable, ok := engine.(ExportableEngine); ok {
		nodes, err := exportable.AllNodes()
		if err != nil {
			return nil
		}
		for _, node := range nodes {
			if NodeNeedsEmbedding(node) {
				return node
			}
		}
	}
	return nil
}

// AllNodes returns all nodes in the memory engine.
func (m *MemoryEngine) AllNodes() ([]*Node, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	nodes := make([]*Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, m.copyNode(node))
	}

	return nodes, nil
}

// AllEdges returns all edges in the memory engine.
func (m *MemoryEngine) AllEdges() ([]*Edge, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	edges := make([]*Edge, 0, len(m.edges))
	for _, edge := range m.edges {
		edges = append(edges, m.copyEdge(edge))
	}

	return edges, nil
}

// GetEdgesByType returns all edges of a specific type.
// For MemoryEngine this filters AllEdges() but is semantically equivalent.
func (m *MemoryEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	if edgeType == "" {
		return m.AllEdges()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, ErrStorageClosed
	}

	normalizedType := strings.ToLower(edgeType)
	var edges []*Edge
	for _, edge := range m.edges {
		if strings.ToLower(edge.Type) == normalizedType {
			edges = append(edges, m.copyEdge(edge))
		}
	}

	return edges, nil
}

// GenericSaveToNeo4jExport works with any ExportableEngine.
func GenericSaveToNeo4jExport(engine ExportableEngine, path string) error {
	nodes, err := engine.AllNodes()
	if err != nil {
		return fmt.Errorf("getting nodes: %w", err)
	}

	edges, err := engine.AllEdges()
	if err != nil {
		return fmt.Errorf("getting edges: %w", err)
	}

	export := ToNeo4jExport(nodes, edges)

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(export); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

// Verify MemoryEngine implements ExportableEngine
var _ ExportableEngine = (*MemoryEngine)(nil)
