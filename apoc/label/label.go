// Package label provides APOC label operations.
//
// This package implements all apoc.label.* functions for working
// with node labels in graph queries.
package label

import (
	"fmt"
	"strings"
)

// Node represents a graph node.
type Node struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

// Exists checks if a label exists in the database.
//
// Example:
//
//	apoc.label.exists('Person') => true
func Exists(label string) bool {
	// Placeholder - would query database
	return false
}

// List returns all labels in the database.
//
// Example:
//
//	apoc.label.list() => ['Person', 'Company', 'Product']
func List() []string {
	// Placeholder - would query database
	return []string{}
}

// Count returns the count of nodes with a specific label.
//
// Example:
//
//	apoc.label.count('Person') => 42
func Count(label string) int {
	// Placeholder - would query database
	return 0
}

// Nodes returns all nodes with a specific label.
//
// Example:
//
//	apoc.label.nodes('Person') => [node1, node2, ...]
func Nodes(label string) []*Node {
	// Placeholder - would query database
	return []*Node{}
}

// Add adds a label to a node.
//
// Example:
//
//	apoc.label.add(node, 'Employee') => updated node
func Add(node *Node, label string) *Node {
	if !Has(node, label) {
		node.Labels = append(node.Labels, label)
	}
	return node
}

// Remove removes a label from a node.
//
// Example:
//
//	apoc.label.remove(node, 'Employee') => updated node
func Remove(node *Node, label string) *Node {
	newLabels := make([]string, 0)
	for _, l := range node.Labels {
		if l != label {
			newLabels = append(newLabels, l)
		}
	}
	node.Labels = newLabels
	return node
}

// Replace replaces all labels on a node.
//
// Example:
//
//	apoc.label.replace(node, ['Person', 'Employee']) => updated node
func Replace(node *Node, labels []string) *Node {
	node.Labels = labels
	return node
}

// Has checks if a node has a specific label.
//
// Example:
//
//	apoc.label.has(node, 'Person') => true
func Has(node *Node, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}

// HasAny checks if a node has any of the specified labels.
//
// Example:
//
//	apoc.label.hasAny(node, ['Person', 'Company']) => true
func HasAny(node *Node, labels []string) bool {
	for _, label := range labels {
		if Has(node, label) {
			return true
		}
	}
	return false
}

// HasAll checks if a node has all of the specified labels.
//
// Example:
//
//	apoc.label.hasAll(node, ['Person', 'Employee']) => true
func HasAll(node *Node, labels []string) bool {
	for _, label := range labels {
		if !Has(node, label) {
			return false
		}
	}
	return true
}

// Get returns all labels of a node.
//
// Example:
//
//	apoc.label.get(node) => ['Person', 'Employee']
func Get(node *Node) []string {
	return node.Labels
}

// Set sets labels on a node (replaces existing).
//
// Example:
//
//	apoc.label.set(node, ['Person', 'Manager']) => updated node
func Set(node *Node, labels []string) *Node {
	node.Labels = labels
	return node
}

// Clear removes all labels from a node.
//
// Example:
//
//	apoc.label.clear(node) => updated node
func Clear(node *Node) *Node {
	node.Labels = []string{}
	return node
}

// Merge merges labels onto a node (adds without removing existing).
//
// Example:
//
//	apoc.label.merge(node, ['Manager', 'Senior']) => updated node
func Merge(node *Node, labels []string) *Node {
	for _, label := range labels {
		Add(node, label)
	}
	return node
}

// Diff returns the difference between two sets of labels.
//
// Example:
//
//	apoc.label.diff(['A', 'B'], ['B', 'C']) => {added: ['C'], removed: ['A'], common: ['B']}
func Diff(labels1, labels2 []string) map[string]interface{} {
	set1 := make(map[string]bool)
	set2 := make(map[string]bool)

	for _, label := range labels1 {
		set1[label] = true
	}

	for _, label := range labels2 {
		set2[label] = true
	}

	added := make([]string, 0)
	removed := make([]string, 0)
	common := make([]string, 0)

	for _, label := range labels2 {
		if set1[label] {
			common = append(common, label)
		} else {
			added = append(added, label)
		}
	}

	for _, label := range labels1 {
		if !set2[label] {
			removed = append(removed, label)
		}
	}

	return map[string]interface{}{
		"added":   added,
		"removed": removed,
		"common":  common,
	}
}

// Union returns the union of multiple label sets.
//
// Example:
//
//	apoc.label.union(['A', 'B'], ['B', 'C']) => ['A', 'B', 'C']
func Union(labelSets ...[]string) []string {
	labelMap := make(map[string]bool)

	for _, labels := range labelSets {
		for _, label := range labels {
			labelMap[label] = true
		}
	}

	result := make([]string, 0, len(labelMap))
	for label := range labelMap {
		result = append(result, label)
	}

	return result
}

// Intersection returns the intersection of multiple label sets.
//
// Example:
//
//	apoc.label.intersection(['A', 'B'], ['B', 'C']) => ['B']
func Intersection(labelSets ...[]string) []string {
	if len(labelSets) == 0 {
		return []string{}
	}

	// Count occurrences
	counts := make(map[string]int)
	for _, labels := range labelSets {
		seen := make(map[string]bool)
		for _, label := range labels {
			if !seen[label] {
				counts[label]++
				seen[label] = true
			}
		}
	}

	// Find labels that appear in all sets
	result := make([]string, 0)
	for label, count := range counts {
		if count == len(labelSets) {
			result = append(result, label)
		}
	}

	return result
}

// Validate validates label names.
//
// Example:
//
//	apoc.label.validate('Person') => {valid: true}
func Validate(label string) map[string]interface{} {
	errors := make([]string, 0)

	if label == "" {
		errors = append(errors, "Label cannot be empty")
	}

	if strings.HasPrefix(label, "_") {
		errors = append(errors, "Label cannot start with underscore")
	}

	if strings.Contains(label, " ") {
		errors = append(errors, "Label cannot contain spaces")
	}

	return map[string]interface{}{
		"valid":  len(errors) == 0,
		"errors": errors,
	}
}

// Normalize normalizes label names (e.g., trim, capitalize).
//
// Example:
//
//	apoc.label.normalize(' person ') => 'Person'
func Normalize(label string) string {
	label = strings.TrimSpace(label)
	if len(label) > 0 {
		label = strings.ToUpper(label[:1]) + label[1:]
	}
	return label
}

// Pattern creates a label pattern for matching.
//
// Example:
//
//	apoc.label.pattern(['Person', 'Employee']) => ':Person:Employee'
func Pattern(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	return ":" + strings.Join(labels, ":")
}

// FromPattern extracts labels from a pattern string.
//
// Example:
//
//	apoc.label.fromPattern(':Person:Employee') => ['Person', 'Employee']
func FromPattern(pattern string) []string {
	pattern = strings.TrimPrefix(pattern, ":")
	if pattern == "" {
		return []string{}
	}
	return strings.Split(pattern, ":")
}

// Stats returns statistics about labels.
//
// Example:
//
//	apoc.label.stats() => {total: 5, counts: {...}}
func Stats() map[string]interface{} {
	// Placeholder - would query database
	return map[string]interface{}{
		"total":  0,
		"counts": map[string]int{},
	}
}

// Search searches for labels matching a pattern.
//
// Example:
//
//	apoc.label.search('Per*') => ['Person', 'Permission']
func Search(pattern string) []string {
	// Placeholder - would query database with pattern matching
	return []string{}
}

// Compare compares labels of two nodes.
//
// Example:
//
//	apoc.label.compare(node1, node2) => {same: true, diff: {...}}
func Compare(node1, node2 *Node) map[string]interface{} {
	diff := Diff(node1.Labels, node2.Labels)

	same := len(diff["added"].([]string)) == 0 && len(diff["removed"].([]string)) == 0

	return map[string]interface{}{
		"same": same,
		"diff": diff,
	}
}

// ToString converts labels to a string representation.
//
// Example:
//
//	apoc.label.toString(['Person', 'Employee']) => 'Person, Employee'
func ToString(labels []string) string {
	return strings.Join(labels, ", ")
}

// FromString parses labels from a string representation.
//
// Example:
//
//	apoc.label.fromString('Person, Employee') => ['Person', 'Employee']
func FromString(str string) []string {
	parts := strings.Split(str, ",")
	labels := make([]string, 0, len(parts))
	for _, part := range parts {
		label := strings.TrimSpace(part)
		if label != "" {
			labels = append(labels, label)
		}
	}
	return labels
}

// Format formats labels with a custom template.
//
// Example:
//
//	apoc.label.format(['Person', 'Employee'], ':%s') => ':Person :Employee'
func Format(labels []string, template string) string {
	formatted := make([]string, len(labels))
	for i, label := range labels {
		formatted[i] = fmt.Sprintf(template, label)
	}
	return strings.Join(formatted, " ")
}
