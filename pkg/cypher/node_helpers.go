// Node and edge conversion helpers for NornicDB Cypher.
//
// This file contains helper functions for converting storage nodes and edges
// to map representations suitable for query results, and for extracting
// information from Cypher patterns.
//
// # Conversion Functions
//
// These functions convert internal storage types to result-friendly formats:
//
//   - nodeToMap: Convert storage.Node to map for RETURN
//   - edgeToMap: Convert storage.Edge to map for RETURN
//   - buildEmbeddingSummary: Create embedding status summary
//
// # Pattern Extraction
//
// These functions extract information from Cypher pattern strings:
//
//   - extractVarName: Get variable name from "(n:Label)"
//   - extractLabels: Get labels from "(n:Label1:Label2)"
//
// # Internal Properties
//
// Some properties are marked as internal and filtered from results:
//
//   - embedding, embeddings, vector, vectors
//   - embedding_model, embedding_dimensions
//   - has_embedding, embedded_at
//
// These are filtered because:
//  1. Embedding arrays are huge (hundreds of floats)
//  2. They're internal implementation details
//  3. They're exposed via buildEmbeddingSummary instead
//
// # ELI12
//
// When you search for something in NornicDB, you get back "nodes" (like
// people or places) and "edges" (like "knows" or "lives at"). These helper
// functions:
//
//  1. Turn internal storage format into nice readable maps
//  2. Hide the huge embedding arrays (they're just numbers, not useful to see)
//  3. Pull out useful info like variable names and labels from patterns
//
// It's like when you ask someone about their friend - they say "Alice, 30,
// from New York" not the person's entire DNA sequence!
//
// # Neo4j Compatibility
//
// These conversions match Neo4j's result format for compatibility.

package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// nodeToMap converts a storage.Node to a map for result output.
// Filters out internal properties like embeddings which are huge.
// Properties are included at the top level for Neo4j compatibility.
// Embeddings are replaced with a summary showing status and dimensions.
//
// # Parameters
//
//   - node: The storage node to convert
//
// # Returns
//
//   - A map suitable for query result rows
//
// # Example
//
//	node := &storage.Node{
//	    ID: "123",
//	    Labels: []string{"Person"},
//	    Properties: map[string]interface{}{"name": "Alice"},
//	}
//	result := exec.nodeToMap(node)
//	// result = {"_nodeId": "123", "labels": ["Person"], "name": "Alice", ...}
func (e *StorageExecutor) nodeToMap(node *storage.Node) map[string]interface{} {
	// Start with node metadata
	// Use _nodeId for internal storage ID to avoid conflicts with user "id" property
	result := map[string]interface{}{
		"_nodeId": string(node.ID), // Internal storage ID for DELETE operations
		"labels":  node.Labels,
	}

	// Add properties at top level for Neo4j compatibility
	for k, v := range node.Properties {
		if e.isInternalProperty(k) {
			continue
		}
		result[k] = v
	}

	// If no user "id" property, use storage ID for backward compatibility
	if _, hasUserID := result["id"]; !hasUserID {
		result["id"] = string(node.ID)
	}

	// Add embedding summary instead of large array
	result["embedding"] = e.buildEmbeddingSummary(node)

	return result
}

// buildEmbeddingSummary creates a summary of embedding status without the actual vector.
// Embeddings are internal-only and generated asynchronously by the embed queue.
//
// # Parameters
//
//   - node: The node whose embedding status to summarize
//
// # Returns
//
//   - A map with status, dimensions, and optionally model info
//
// # Example
//
//	summary := exec.buildEmbeddingSummary(node)
//	// If embedded: {"status": "ready", "dimensions": 384, "model": "all-minilm"}
//	// If pending:  {"status": "pending", "dimensions": 0}
func (e *StorageExecutor) buildEmbeddingSummary(node *storage.Node) map[string]interface{} {
	summary := map[string]interface{}{}

	// Check if node has embedding in dedicated storage field (the only valid location)
	if len(node.Embedding) > 0 {
		summary["status"] = "ready"
		summary["dimensions"] = len(node.Embedding)
	} else {
		// No embedding yet - will be generated asynchronously by embed queue
		summary["status"] = "pending"
		summary["dimensions"] = 0
	}

	// Include model info from properties if available
	if model, ok := node.Properties["embedding_model"]; ok {
		summary["model"] = model
	}

	return summary
}

// edgeToMap converts a storage.Edge to a map for result output.
//
// # Parameters
//
//   - edge: The storage edge to convert
//
// # Returns
//
//   - A map suitable for query result rows
//
// # Example
//
//	edge := &storage.Edge{
//	    ID: "e1",
//	    Type: "KNOWS",
//	    StartNode: "n1",
//	    EndNode: "n2",
//	}
//	result := exec.edgeToMap(edge)
//	// result = {"_edgeId": "e1", "type": "KNOWS", "startNode": "n1", ...}
func (e *StorageExecutor) edgeToMap(edge *storage.Edge) map[string]interface{} {
	return map[string]interface{}{
		"_edgeId":    string(edge.ID),
		"type":       edge.Type,
		"startNode":  string(edge.StartNode),
		"endNode":    string(edge.EndNode),
		"properties": edge.Properties,
	}
}

// internalProps is pre-computed at package init to avoid allocation per call.
// These properties should not be returned in query results.
// All keys are lowercase for case-insensitive matching.
var internalProps = map[string]bool{
	// Embedding arrays (huge float arrays - never return these)
	"embedding":        true,
	"embeddings":       true,
	"vector":           true,
	"vectors":          true,
	"_embedding":       true,
	"_embeddings":      true,
	"chunk_embedding":  true,
	"chunk_embeddings": true,
	// Embedding metadata (shown in embedding summary instead)
	"embedding_model":      true,
	"embedding_dimensions": true,
	"has_embedding":        true,
	"embedded_at":          true,
}

// isInternalProperty returns true for properties that should not be returned in results.
// This includes embeddings (huge float arrays) and other internal metadata.
// Uses direct lookup for common lowercase names, falls back to ToLower for mixed case.
//
// # Parameters
//
//   - propName: The property name to check
//
// # Returns
//
//   - true if the property should be hidden from results
func (e *StorageExecutor) isInternalProperty(propName string) bool {
	// Fast path: check if it's already lowercase (most common case)
	if internalProps[propName] {
		return true
	}
	// Check for uppercase first char (common pattern: Embedding, Vector)
	if len(propName) > 0 && propName[0] >= 'A' && propName[0] <= 'Z' {
		return internalProps[strings.ToLower(propName)]
	}
	return false
}

// extractVarName extracts the variable name from a pattern like "(n:Label {...})".
//
// # Parameters
//
//   - pattern: The Cypher pattern string
//
// # Returns
//
//   - The variable name, or "n" as default
//
// # Example
//
//	extractVarName("(person:Person {name: 'Alice'})")
//	// Returns: "person"
//
//	extractVarName("(:Person)")
//	// Returns: "n" (default)
func (e *StorageExecutor) extractVarName(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	pattern = strings.TrimPrefix(pattern, "(")
	// Find first : or { or )
	for i, c := range pattern {
		if c == ':' || c == '{' || c == ')' || c == ' ' {
			name := strings.TrimSpace(pattern[:i])
			if name != "" {
				return name
			}
			break
		}
	}
	return "n" // Default variable name
}

// extractLabels extracts labels from a pattern like "(n:Label1:Label2 {...})".
//
// # Parameters
//
//   - pattern: The Cypher pattern string
//
// # Returns
//
//   - Slice of label strings
//
// # Example
//
//	extractLabels("(n:Person:Employee {name: 'Alice'})")
//	// Returns: ["Person", "Employee"]
//
//	extractLabels("(n)")
//	// Returns: []
func (e *StorageExecutor) extractLabels(pattern string) []string {
	pattern = strings.TrimSpace(pattern)
	pattern = strings.TrimPrefix(pattern, "(")
	pattern = strings.TrimSuffix(pattern, ")")

	// Remove properties block
	if propsStart := strings.Index(pattern, "{"); propsStart > 0 {
		pattern = pattern[:propsStart]
	}

	// Split by : and extract labels
	parts := strings.Split(pattern, ":")
	labels := []string{}
	for i := 1; i < len(parts); i++ {
		label := strings.TrimSpace(parts[i])
		// Remove spaces and trailing characters
		if spaceIdx := strings.IndexAny(label, " {"); spaceIdx > 0 {
			label = label[:spaceIdx]
		}
		if label != "" {
			labels = append(labels, label)
		}
	}
	return labels
}
