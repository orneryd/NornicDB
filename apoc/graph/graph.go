// Package graph provides APOC graph manipulation functions.
//
// This package implements all apoc.graph.* functions for creating
// and manipulating virtual graphs and graph structures.
package graph

import (
	"fmt"
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

// Graph represents a virtual graph.
type Graph struct {
	Nodes         []*Node
	Relationships []*Relationship
}

// From creates a virtual graph from nodes and relationships.
//
// Example:
//
//	apoc.graph.from(nodes, rels, 'MyGraph') => virtual graph
func From(nodes []*Node, rels []*Relationship, name string) *Graph {
	return &Graph{
		Nodes:         nodes,
		Relationships: rels,
	}
}

// FromData creates a virtual graph from data.
//
// Example:
//
//	apoc.graph.fromData(data, 'MyGraph') => virtual graph
func FromData(data map[string]interface{}, name string) *Graph {
	graph := &Graph{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}

	if nodes, ok := data["nodes"].([]interface{}); ok {
		for _, n := range nodes {
			if nodeMap, ok := n.(map[string]interface{}); ok {
				node := &Node{
					Labels:     []string{},
					Properties: make(map[string]interface{}),
				}
				if id, ok := nodeMap["id"].(int64); ok {
					node.ID = id
				}
				if labels, ok := nodeMap["labels"].([]string); ok {
					node.Labels = labels
				}
				if props, ok := nodeMap["properties"].(map[string]interface{}); ok {
					node.Properties = props
				}
				graph.Nodes = append(graph.Nodes, node)
			}
		}
	}

	return graph
}

// FromPath creates a virtual graph from a path.
//
// Example:
//
//	apoc.graph.fromPath(path, 'MyGraph') => virtual graph
func FromPath(nodes []*Node, rels []*Relationship, name string) *Graph {
	return From(nodes, rels, name)
}

// FromPaths creates a virtual graph from multiple paths.
//
// Example:
//
//	apoc.graph.fromPaths(paths, 'MyGraph') => virtual graph
func FromPaths(paths []map[string]interface{}, name string) *Graph {
	graph := &Graph{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}

	nodeSet := make(map[int64]*Node)
	relSet := make(map[int64]*Relationship)

	for _, path := range paths {
		if nodes, ok := path["nodes"].([]*Node); ok {
			for _, node := range nodes {
				if _, exists := nodeSet[node.ID]; !exists {
					nodeSet[node.ID] = node
					graph.Nodes = append(graph.Nodes, node)
				}
			}
		}

		if rels, ok := path["relationships"].([]*Relationship); ok {
			for _, rel := range rels {
				if _, exists := relSet[rel.ID]; !exists {
					relSet[rel.ID] = rel
					graph.Relationships = append(graph.Relationships, rel)
				}
			}
		}
	}

	return graph
}

// FromDocument creates a virtual graph from a document structure.
//
// Example:
//
//	apoc.graph.fromDocument(doc, {}) => virtual graph
func FromDocument(doc map[string]interface{}, config map[string]interface{}) *Graph {
	graph := &Graph{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}

	// Create root node
	root := &Node{
		ID:         1,
		Labels:     []string{"Document"},
		Properties: make(map[string]interface{}),
	}

	for key, val := range doc {
		root.Properties[key] = val
	}

	graph.Nodes = append(graph.Nodes, root)

	return graph
}

// FromCypher creates a virtual graph from Cypher query results.
//
// Example:
//
//	apoc.graph.fromCypher('MATCH (n) RETURN n', {}) => virtual graph
func FromCypher(query string, params map[string]interface{}) *Graph {
	// Placeholder - would execute query and build graph
	return &Graph{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}
}

// Validate validates a graph structure.
//
// Example:
//
//	apoc.graph.validate(graph) => {valid: true, errors: []}
func Validate(graph *Graph) map[string]interface{} {
	errors := make([]string, 0)

	// Check for orphaned relationships
	nodeIDs := make(map[int64]bool)
	for _, node := range graph.Nodes {
		nodeIDs[node.ID] = true
	}

	for _, rel := range graph.Relationships {
		if !nodeIDs[rel.StartNode] {
			errors = append(errors, fmt.Sprintf("Relationship %d references non-existent start node %d", rel.ID, rel.StartNode))
		}
		if !nodeIDs[rel.EndNode] {
			errors = append(errors, fmt.Sprintf("Relationship %d references non-existent end node %d", rel.ID, rel.EndNode))
		}
	}

	return map[string]interface{}{
		"valid":  len(errors) == 0,
		"errors": errors,
	}
}

// Nodes returns all nodes in a graph.
//
// Example:
//
//	apoc.graph.nodes(graph) => [node1, node2, ...]
func Nodes(graph *Graph) []*Node {
	return graph.Nodes
}

// Relationships returns all relationships in a graph.
//
// Example:
//
//	apoc.graph.relationships(graph) => [rel1, rel2, ...]
func Relationships(graph *Graph) []*Relationship {
	return graph.Relationships
}

// Merge merges multiple graphs into one.
//
// Example:
//
//	apoc.graph.merge(graph1, graph2) => merged graph
func Merge(graphs ...*Graph) *Graph {
	merged := &Graph{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}

	nodeSet := make(map[int64]*Node)
	relSet := make(map[int64]*Relationship)

	for _, graph := range graphs {
		for _, node := range graph.Nodes {
			if _, exists := nodeSet[node.ID]; !exists {
				nodeSet[node.ID] = node
				merged.Nodes = append(merged.Nodes, node)
			}
		}

		for _, rel := range graph.Relationships {
			if _, exists := relSet[rel.ID]; !exists {
				relSet[rel.ID] = rel
				merged.Relationships = append(merged.Relationships, rel)
			}
		}
	}

	return merged
}

// Clone creates a deep copy of a graph.
//
// Example:
//
//	apoc.graph.clone(graph) => cloned graph
func Clone(graph *Graph) *Graph {
	cloned := &Graph{
		Nodes:         make([]*Node, 0, len(graph.Nodes)),
		Relationships: make([]*Relationship, 0, len(graph.Relationships)),
	}

	// Clone nodes
	for _, node := range graph.Nodes {
		clonedNode := &Node{
			ID:         node.ID,
			Labels:     make([]string, len(node.Labels)),
			Properties: make(map[string]interface{}),
		}
		copy(clonedNode.Labels, node.Labels)
		for k, v := range node.Properties {
			clonedNode.Properties[k] = v
		}
		cloned.Nodes = append(cloned.Nodes, clonedNode)
	}

	// Clone relationships
	for _, rel := range graph.Relationships {
		clonedRel := &Relationship{
			ID:         rel.ID,
			Type:       rel.Type,
			StartNode:  rel.StartNode,
			EndNode:    rel.EndNode,
			Properties: make(map[string]interface{}),
		}
		for k, v := range rel.Properties {
			clonedRel.Properties[k] = v
		}
		cloned.Relationships = append(cloned.Relationships, clonedRel)
	}

	return cloned
}

// Stats returns statistics about a graph.
//
// Example:
//
//	apoc.graph.stats(graph) => {nodes: 10, relationships: 15}
func Stats(graph *Graph) map[string]interface{} {
	labelCounts := make(map[string]int)
	typeCounts := make(map[string]int)

	for _, node := range graph.Nodes {
		for _, label := range node.Labels {
			labelCounts[label]++
		}
	}

	for _, rel := range graph.Relationships {
		typeCounts[rel.Type]++
	}

	return map[string]interface{}{
		"nodes":          len(graph.Nodes),
		"relationships":  len(graph.Relationships),
		"labels":         labelCounts,
		"relationshipTypes": typeCounts,
	}
}

// ToMap converts a graph to a map representation.
//
// Example:
//
//	apoc.graph.toMap(graph) => {nodes: [...], relationships: [...]}
func ToMap(graph *Graph) map[string]interface{} {
	return map[string]interface{}{
		"nodes":         graph.Nodes,
		"relationships": graph.Relationships,
	}
}

// FromMap creates a graph from a map representation.
//
// Example:
//
//	apoc.graph.fromMap(map) => graph
func FromMap(data map[string]interface{}) *Graph {
	graph := &Graph{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}

	if nodes, ok := data["nodes"].([]*Node); ok {
		graph.Nodes = nodes
	}

	if rels, ok := data["relationships"].([]*Relationship); ok {
		graph.Relationships = rels
	}

	return graph
}

// Subgraph extracts a subgraph based on node IDs.
//
// Example:
//
//	apoc.graph.subgraph(graph, [1, 2, 3]) => subgraph
func Subgraph(graph *Graph, nodeIDs []int64) *Graph {
	nodeSet := make(map[int64]bool)
	for _, id := range nodeIDs {
		nodeSet[id] = true
	}

	subgraph := &Graph{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}

	// Add nodes
	for _, node := range graph.Nodes {
		if nodeSet[node.ID] {
			subgraph.Nodes = append(subgraph.Nodes, node)
		}
	}

	// Add relationships where both nodes are in subgraph
	for _, rel := range graph.Relationships {
		if nodeSet[rel.StartNode] && nodeSet[rel.EndNode] {
			subgraph.Relationships = append(subgraph.Relationships, rel)
		}
	}

	return subgraph
}
