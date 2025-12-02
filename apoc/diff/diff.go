// Package diff provides APOC diff operations.
//
// This package implements all apoc.diff.* functions for comparing
// and finding differences between nodes, relationships, and data structures.
package diff

import (
	"fmt"
	"reflect"
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

// DiffResult represents the result of a diff operation.
type DiffResult struct {
	Added    map[string]interface{}
	Removed  map[string]interface{}
	Changed  map[string]interface{}
	Unchanged map[string]interface{}
}

// Nodes compares two nodes and returns differences.
//
// Example:
//
//	apoc.diff.nodes(node1, node2) => {added: {...}, removed: {...}, changed: {...}}
func Nodes(node1, node2 *Node) *DiffResult {
	result := &DiffResult{
		Added:    make(map[string]interface{}),
		Removed:  make(map[string]interface{}),
		Changed:  make(map[string]interface{}),
		Unchanged: make(map[string]interface{}),
	}

	// Check for added/changed properties
	for k, v2 := range node2.Properties {
		if v1, ok := node1.Properties[k]; ok {
			if !reflect.DeepEqual(v1, v2) {
				result.Changed[k] = map[string]interface{}{
					"old": v1,
					"new": v2,
				}
			} else {
				result.Unchanged[k] = v1
			}
		} else {
			result.Added[k] = v2
		}
	}

	// Check for removed properties
	for k, v := range node1.Properties {
		if _, ok := node2.Properties[k]; !ok {
			result.Removed[k] = v
		}
	}

	return result
}

// Relationships compares two relationships and returns differences.
//
// Example:
//
//	apoc.diff.relationships(rel1, rel2) => {added: {...}, removed: {...}, changed: {...}}
func Relationships(rel1, rel2 *Relationship) *DiffResult {
	result := &DiffResult{
		Added:    make(map[string]interface{}),
		Removed:  make(map[string]interface{}),
		Changed:  make(map[string]interface{}),
		Unchanged: make(map[string]interface{}),
	}

	// Check for added/changed properties
	for k, v2 := range rel2.Properties {
		if v1, ok := rel1.Properties[k]; ok {
			if !reflect.DeepEqual(v1, v2) {
				result.Changed[k] = map[string]interface{}{
					"old": v1,
					"new": v2,
				}
			} else {
				result.Unchanged[k] = v1
			}
		} else {
			result.Added[k] = v2
		}
	}

	// Check for removed properties
	for k, v := range rel1.Properties {
		if _, ok := rel2.Properties[k]; !ok {
			result.Removed[k] = v
		}
	}

	return result
}

// Maps compares two maps and returns differences.
//
// Example:
//
//	apoc.diff.maps({a: 1}, {a: 2, b: 3}) => {added: {b: 3}, changed: {a: {old: 1, new: 2}}}
func Maps(map1, map2 map[string]interface{}) *DiffResult {
	result := &DiffResult{
		Added:    make(map[string]interface{}),
		Removed:  make(map[string]interface{}),
		Changed:  make(map[string]interface{}),
		Unchanged: make(map[string]interface{}),
	}

	// Check for added/changed keys
	for k, v2 := range map2 {
		if v1, ok := map1[k]; ok {
			if !reflect.DeepEqual(v1, v2) {
				result.Changed[k] = map[string]interface{}{
					"old": v1,
					"new": v2,
				}
			} else {
				result.Unchanged[k] = v1
			}
		} else {
			result.Added[k] = v2
		}
	}

	// Check for removed keys
	for k, v := range map1 {
		if _, ok := map2[k]; !ok {
			result.Removed[k] = v
		}
	}

	return result
}

// Lists compares two lists and returns differences.
//
// Example:
//
//	apoc.diff.lists([1, 2, 3], [2, 3, 4]) => {added: [4], removed: [1], common: [2, 3]}
func Lists(list1, list2 []interface{}) map[string]interface{} {
	added := make([]interface{}, 0)
	removed := make([]interface{}, 0)
	common := make([]interface{}, 0)

	// Build sets for comparison
	set1 := make(map[string]bool)
	set2 := make(map[string]bool)

	for _, item := range list1 {
		set1[fmt.Sprintf("%v", item)] = true
	}

	for _, item := range list2 {
		key := fmt.Sprintf("%v", item)
		set2[key] = true
		if set1[key] {
			common = append(common, item)
		} else {
			added = append(added, item)
		}
	}

	for _, item := range list1 {
		key := fmt.Sprintf("%v", item)
		if !set2[key] {
			removed = append(removed, item)
		}
	}

	return map[string]interface{}{
		"added":   added,
		"removed": removed,
		"common":  common,
	}
}

// Strings compares two strings and returns character-level differences.
//
// Example:
//
//	apoc.diff.strings('hello', 'hallo') => differences
func Strings(str1, str2 string) []map[string]interface{} {
	differences := make([]map[string]interface{}, 0)

	// Simple character-by-character comparison
	maxLen := len(str1)
	if len(str2) > maxLen {
		maxLen = len(str2)
	}

	for i := 0; i < maxLen; i++ {
		var char1, char2 string
		if i < len(str1) {
			char1 = string(str1[i])
		}
		if i < len(str2) {
			char2 = string(str2[i])
		}

		if char1 != char2 {
			differences = append(differences, map[string]interface{}{
				"position": i,
				"old":      char1,
				"new":      char2,
			})
		}
	}

	return differences
}

// Deep performs a deep comparison of two values.
//
// Example:
//
//	apoc.diff.deep(value1, value2) => true/false
func Deep(value1, value2 interface{}) bool {
	return reflect.DeepEqual(value1, value2)
}

// Patch applies a diff patch to a map.
//
// Example:
//
//	apoc.diff.patch(original, diff) => patched map
func Patch(original map[string]interface{}, diff *DiffResult) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy unchanged and existing values
	for k, v := range original {
		if _, removed := diff.Removed[k]; !removed {
			result[k] = v
		}
	}

	// Apply changes
	for k, v := range diff.Changed {
		if changeMap, ok := v.(map[string]interface{}); ok {
			if newVal, ok := changeMap["new"]; ok {
				result[k] = newVal
			}
		}
	}

	// Add new values
	for k, v := range diff.Added {
		result[k] = v
	}

	return result
}

// Merge merges two maps with conflict resolution.
//
// Example:
//
//	apoc.diff.merge(map1, map2, 'prefer_new') => merged map
func Merge(map1, map2 map[string]interface{}, strategy string) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy first map
	for k, v := range map1 {
		result[k] = v
	}

	// Merge second map based on strategy
	for k, v2 := range map2 {
		if v1, exists := result[k]; exists {
			switch strategy {
			case "prefer_new":
				result[k] = v2
			case "prefer_old":
				// Keep existing value
			case "combine":
				// Try to combine values
				result[k] = combineValues(v1, v2)
			default:
				result[k] = v2
			}
		} else {
			result[k] = v2
		}
	}

	return result
}

// combineValues attempts to combine two values intelligently.
func combineValues(v1, v2 interface{}) interface{} {
	// If both are lists, concatenate
	if list1, ok1 := v1.([]interface{}); ok1 {
		if list2, ok2 := v2.([]interface{}); ok2 {
			return append(list1, list2...)
		}
	}

	// If both are numbers, add
	if num1, ok1 := v1.(float64); ok1 {
		if num2, ok2 := v2.(float64); ok2 {
			return num1 + num2
		}
	}

	// If both are strings, concatenate
	if str1, ok1 := v1.(string); ok1 {
		if str2, ok2 := v2.(string); ok2 {
			return str1 + str2
		}
	}

	// Default: prefer new value
	return v2
}

// Summary provides a summary of differences.
//
// Example:
//
//	apoc.diff.summary(diff) => {added: 2, removed: 1, changed: 3}
func Summary(diff *DiffResult) map[string]int {
	return map[string]int{
		"added":   len(diff.Added),
		"removed": len(diff.Removed),
		"changed": len(diff.Changed),
		"unchanged": len(diff.Unchanged),
	}
}
