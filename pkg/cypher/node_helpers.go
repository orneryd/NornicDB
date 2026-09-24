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
//
// # Pattern Extraction
//
// These functions extract information from Cypher pattern strings:
//
//   - extractVarName: Get variable name from "(n:Label)"
//   - extractLabels: Get labels from "(n:Label1:Label2)"
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
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type materializedAccessRecorder interface {
	RecordMaterializedAccess(entityID string)
}

func (e *StorageExecutor) recordMaterializedResultAccess(result *ExecuteResult) {
	if result == nil {
		return
	}
	recorder, ok := e.storage.(materializedAccessRecorder)
	if !ok || recorder == nil {
		return
	}
	for _, row := range result.Rows {
		for _, value := range row {
			e.recordMaterializedValue(recorder, value)
		}
	}
}

func (e *StorageExecutor) recordMaterializedValue(recorder materializedAccessRecorder, value interface{}) {
	switch v := value.(type) {
	case *storage.Node:
		if v != nil {
			recorder.RecordMaterializedAccess(string(v.ID))
		}
	case *storage.Edge:
		if v != nil {
			recorder.RecordMaterializedAccess(string(v.ID))
		}
	case map[string]interface{}:
		if nodeID, ok := v["_nodeId"].(string); ok && nodeID != "" {
			recorder.RecordMaterializedAccess(nodeID)
			return
		}
		if edgeID, ok := v["_edgeId"].(string); ok && edgeID != "" {
			recorder.RecordMaterializedAccess(edgeID)
			return
		}
		for _, nested := range v {
			e.recordMaterializedValue(recorder, nested)
		}
	case []interface{}:
		for _, nested := range v {
			e.recordMaterializedValue(recorder, nested)
		}
	case []map[string]interface{}:
		for _, nested := range v {
			e.recordMaterializedValue(recorder, nested)
		}
	}
}

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

	// Add properties both at top level (for Neo4j compatibility) and nested (for standard graph format)
	be := unwrapBadgerEngine(e.storage)
	var nowNanos int64
	if be != nil {
		nowNanos = storage.DecayScoringTime()
	}
	props := node.Properties
	if be != nil {
		createdAt := node.CreatedAt.UnixNano()
		versionAt := nodeVersionAtNanos(node, createdAt)
		filtered := make(map[string]interface{}, len(node.Properties))
		for k, v := range node.Properties {
			if be.FilterPropertyByDecay(node.ID, node.Labels, k, createdAt, versionAt, nowNanos) {
				continue
			}
			filtered[k] = v
		}
		props = filtered
	}
	for k, v := range props {
		result[k] = v
	}
	result["properties"] = props

	// If no user "id" property, use storage ID for backward compatibility
	if _, hasUserID := result["id"]; !hasUserID {
		result["id"] = string(node.ID)
	}

	return result
}

func nodeVersionAtNanos(node *storage.Node, createdAt int64) int64 {
	if node == nil || node.UpdatedAt.IsZero() {
		return createdAt
	}
	return node.UpdatedAt.UnixNano()
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
	props := edge.Properties
	be := unwrapBadgerEngine(e.storage)
	if be != nil {
		nowNanos := storage.DecayScoringTime()
		createdAt := edge.CreatedAt.UnixNano()
		versionAt := createdAt
		if !edge.UpdatedAt.IsZero() {
			versionAt = edge.UpdatedAt.UnixNano()
		}
		filtered := make(map[string]interface{}, len(edge.Properties))
		for k, v := range edge.Properties {
			if be.FilterEdgePropertyByDecay(edge.ID, edge.Type, k, createdAt, versionAt, nowNanos) {
				continue
			}
			filtered[k] = v
		}
		props = filtered
	}
	return map[string]interface{}{
		"_edgeId":    string(edge.ID),
		"type":       edge.Type,
		"startNode":  string(edge.StartNode),
		"endNode":    string(edge.EndNode),
		"properties": props,
	}
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
	head, _ := splitNodePatternProperties(pattern)
	_, labels, _ := parseNodeHead(head)
	return labels
}

// applyArraySuffix applies an array suffix operation (like [..10] or [5]) to a collected list.
// This handles slicing and indexing operations on aggregation results.
//
// # Parameters
//   - collected: the list to apply the suffix to
//   - suffix: the suffix string (e.g., "[..10]", "[5]", "[2..5]")
//
// # Returns
//   - The sliced or indexed result
func (e *StorageExecutor) applyArraySuffix(collected []interface{}, suffix string) interface{} {
	suffix = strings.TrimSpace(suffix)
	if suffix == "" || !strings.HasPrefix(suffix, "[") || !strings.HasSuffix(suffix, "]") {
		return collected
	}

	// Extract the index/slice expression inside [ ]
	indexExpr := suffix[1 : len(suffix)-1]

	// Check for slice notation [..N] or [N..M] or [N..]
	if strings.Contains(indexExpr, "..") {
		parts := strings.SplitN(indexExpr, "..", 2)
		startIdx := int64(0)
		endIdx := int64(len(collected))

		if parts[0] != "" {
			if n, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64); err == nil {
				startIdx = n
			}
		}
		if len(parts) > 1 && parts[1] != "" {
			if n, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
				endIdx = n
			}
		}

		// Handle negative indices
		if startIdx < 0 {
			startIdx = int64(len(collected)) + startIdx
		}
		if endIdx < 0 {
			endIdx = int64(len(collected)) + endIdx
		}
		// Clamp
		if startIdx < 0 {
			startIdx = 0
		}
		if endIdx > int64(len(collected)) {
			endIdx = int64(len(collected))
		}
		if startIdx >= endIdx {
			return []interface{}{}
		}
		return collected[startIdx:endIdx]
	}

	// Single index access [N]
	if idx, err := strconv.ParseInt(strings.TrimSpace(indexExpr), 10, 64); err == nil {
		if idx < 0 {
			idx = int64(len(collected)) + idx
		}
		if idx >= 0 && idx < int64(len(collected)) {
			return collected[idx]
		}
		return nil
	}

	return collected
}
