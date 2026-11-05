// Package node provides APOC node operations.
//
// This package implements all apoc.node.* functions for working with
// nodes in Cypher queries.
package node

import "errors"

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Functions binds this package's storage-dependent helpers to one storage
// backend. The backend is fixed when the instance is created, so a handler an
// already-admitted caller holds keeps the backend it was built for.
type Functions struct {
	backend storage.Storage
}

// New returns this package's helpers bound to backend. One instance serves one
// backend; a reconfiguration builds a new instance instead of rebinding this
// one.
func New(backend storage.Storage) *Functions {
	return &Functions{backend: backend}
}

// Degree returns the degree (number of relationships) of a node.
//
// Example:
//
//	apoc.node.degree(node) => 5
func (f *Functions) Degree(ctx context.Context, node *Node, relType string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	degree, err := f.backend.GetNodeDegree(ctx, node.ID, relType, storage.DirectionBoth)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return 0, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}
		return 0, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return degree, nil
}

// DegreeIn returns the in-degree of a node.
//
// Example:
//
//	apoc.node.degree.in(node, 'KNOWS') => 3
func (f *Functions) DegreeIn(ctx context.Context, node *Node, relType string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	degree, err := f.backend.GetNodeDegree(ctx, node.ID, relType, storage.DirectionIncoming)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return 0, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}
		return 0, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return degree, nil
}

// DegreeOut returns the out-degree of a node.
//
// Example:
//
//	apoc.node.degree.out(node, 'KNOWS') => 2
func (f *Functions) DegreeOut(ctx context.Context, node *Node, relType string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	degree, err := f.backend.GetNodeDegree(ctx, node.ID, relType, storage.DirectionOutgoing)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return 0, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}
		return 0, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return degree, nil
}

// ID returns the ID of a node.
//
// Example:
//
//	apoc.node.id(node) => 123
func ID(node *Node) int64 {
	return node.ID
}

// Labels returns the labels of a node.
//
// Example:
//
//	apoc.node.labels(node) => ['Person', 'Employee']
func Labels(node *Node) []string {
	return node.Labels
}

// Properties returns all properties of a node.
//
// Example:
//
//	apoc.node.properties(node) => {name:'Alice', age:30}
func Properties(node *Node) map[string]interface{} {
	return node.Properties
}

// Property returns a specific property value.
//
// Example:
//
//	apoc.node.property(node, 'name') => 'Alice'
func Property(node *Node, key string) interface{} {
	return node.Properties[key]
}

// HasLabel checks if a node has a specific label.
//
// Example:
//
//	apoc.node.hasLabel(node, 'Person') => true
func HasLabel(node *Node, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}

// HasLabels checks if a node has all specified labels.
//
// Example:
//
//	apoc.node.hasLabels(node, ['Person', 'Employee']) => true
func HasLabels(node *Node, labels []string) bool {
	for _, label := range labels {
		if !HasLabel(node, label) {
			return false
		}
	}
	return true
}

// RelationshipTypes returns all relationship types of a node.
//
// Example:
//
//	apoc.node.relationshipTypes(node) => ['KNOWS', 'WORKS_AT']
func (f *Functions) RelationshipTypes(ctx context.Context, node *Node) ([]string, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	rels, err := f.backend.GetNodeRelationships(ctx, node.ID, "", storage.DirectionBoth)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []string{}, nil
	}

	types := make(map[string]bool)
	for _, rel := range rels {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		types[rel.Type] = true
	}

	result := make([]string, 0, len(types))
	for t := range types {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		result = append(result, t)
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return result, nil
}

// RelationshipTypesIn returns incoming relationship types.
//
// Example:
//
//	apoc.node.relationshipTypes.in(node) => ['KNOWS', 'FOLLOWS']
func (f *Functions) RelationshipTypesIn(ctx context.Context, node *Node) ([]string, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	rels, err := f.backend.GetNodeRelationships(ctx, node.ID, "", storage.DirectionIncoming)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []string{}, nil
	}

	types := make(map[string]bool)
	for _, rel := range rels {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		types[rel.Type] = true
	}

	result := make([]string, 0, len(types))
	for t := range types {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		result = append(result, t)
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return result, nil
}

// RelationshipTypesOut returns outgoing relationship types.
//
// Example:
//
//	apoc.node.relationshipTypes.out(node) => ['KNOWS', 'WORKS_AT']
func (f *Functions) RelationshipTypesOut(ctx context.Context, node *Node) ([]string, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	rels, err := f.backend.GetNodeRelationships(ctx, node.ID, "", storage.DirectionOutgoing)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []string{}, nil
	}

	types := make(map[string]bool)
	for _, rel := range rels {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		types[rel.Type] = true
	}

	result := make([]string, 0, len(types))
	for t := range types {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		result = append(result, t)
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return result, nil
}

// Relationships returns all relationships of a node.
//
// Example:
//
//	apoc.node.relationships(node) => [rel1, rel2, ...]
func (f *Functions) Relationships(ctx context.Context, node *Node, relType string) ([]*Relationship, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	rels, err := f.backend.GetNodeRelationships(ctx, node.ID, relType, storage.DirectionBoth)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Relationship{}, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return rels, nil
}

// RelationshipsIn returns incoming relationships.
//
// Example:
//
//	apoc.node.relationships.in(node, 'KNOWS') => [rel1, rel2]
func (f *Functions) RelationshipsIn(ctx context.Context, node *Node, relType string) ([]*Relationship, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	rels, err := f.backend.GetNodeRelationships(ctx, node.ID, relType, storage.DirectionIncoming)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Relationship{}, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return rels, nil
}

// RelationshipsOut returns outgoing relationships.
//
// Example:
//
//	apoc.node.relationships.out(node, 'KNOWS') => [rel1, rel2]
func (f *Functions) RelationshipsOut(ctx context.Context, node *Node, relType string) ([]*Relationship, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	rels, err := f.backend.GetNodeRelationships(ctx, node.ID, relType, storage.DirectionOutgoing)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Relationship{}, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return rels, nil
}

// RelationshipExists checks if a relationship exists.
//
// Example:
//
//	apoc.node.relationshipExists(node, 'KNOWS>') => true
func (f *Functions) RelationshipExists(ctx context.Context, node *Node, relPattern string) (bool, error) {
	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}

	// Parse pattern to extract type and direction
	relType := relPattern
	direction := storage.DirectionBoth

	if len(relPattern) > 0 {
		if relPattern[len(relPattern)-1] == '>' {
			direction = storage.DirectionOutgoing
			relType = relPattern[:len(relPattern)-1]
		} else if relPattern[0] == '<' {
			direction = storage.DirectionIncoming
			relType = relPattern[1:]
		}
	}

	rels, err := f.backend.GetNodeRelationships(ctx, node.ID, relType, direction)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return false, err
	}

	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}
	return err == nil && len(rels) > 0, nil
}

// Connected checks if two nodes are connected.
//
// Example:
//
//	apoc.node.connected(node1, node2, 'KNOWS') => true
func (f *Functions) Connected(ctx context.Context, node1, node2 *Node, relType string) (bool, error) {
	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}

	path, err := f.backend.FindShortestPath(ctx, node1.ID, node2.ID, relType, 1)
	if err != nil && !errors.Is(err, storage.ErrPathNotFound) {
		return false, err
	}

	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}
	return err == nil && path != nil, nil
}

// Neighbors returns neighboring nodes.
//
// Example:
//
//	apoc.node.neighbors(node, 'KNOWS') => [node1, node2, ...]
func (f *Functions) Neighbors(ctx context.Context, node *Node, relType string) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	neighbors, err := f.backend.GetNodeNeighbors(ctx, node.ID, relType, storage.DirectionBoth)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Node{}, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return neighbors, nil
}

// NeighborsIn returns incoming neighbors.
//
// Example:
//
//	apoc.node.neighbors.in(node, 'KNOWS') => [node1, node2]
func (f *Functions) NeighborsIn(ctx context.Context, node *Node, relType string) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	neighbors, err := f.backend.GetNodeNeighbors(ctx, node.ID, relType, storage.DirectionIncoming)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Node{}, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return neighbors, nil
}

// NeighborsOut returns outgoing neighbors.
//
// Example:
//
//	apoc.node.neighbors.out(node, 'KNOWS') => [node1, node2]
func (f *Functions) NeighborsOut(ctx context.Context, node *Node, relType string) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	neighbors, err := f.backend.GetNodeNeighbors(ctx, node.ID, relType, storage.DirectionOutgoing)
	if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Node{}, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return neighbors, nil
}

// IsDense checks if a node is dense (many relationships).
//
// Example:
//
//	apoc.node.isDense(node) => true
func (f *Functions) IsDense(ctx context.Context, node *Node, threshold int) (bool, error) {
	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}

	helperValue1, helperError1 := f.Degree(ctx, node, "")
	if helperError1 != nil {
		return false, helperError1
	}
	return helperValue1 > threshold, nil
}

// ToMap converts a node to a map.
//
// Example:
//
//	apoc.node.toMap(node) => {id:123, labels:['Person'], properties:{...}}
func ToMap(node *Node) map[string]interface{} {
	return map[string]interface{}{
		"id":         node.ID,
		"labels":     node.Labels,
		"properties": node.Properties,
	}
}

// FromMap creates a node from a map.
//
// Example:
//
//	apoc.node.fromMap({labels:['Person'], properties:{name:'Alice'}})
func FromMap(m map[string]interface{}) *Node {
	node := &Node{
		Labels:     []string{},
		Properties: make(map[string]interface{}),
	}

	if id, ok := m["id"].(int64); ok {
		node.ID = id
	}

	if labels, ok := m["labels"].([]interface{}); ok {
		for _, label := range labels {
			if labelStr, ok := label.(string); ok {
				node.Labels = append(node.Labels, labelStr)
			}
		}
	}

	if props, ok := m["properties"].(map[string]interface{}); ok {
		node.Properties = props
	}

	return node
}

// SetProperty sets a property on a node.
//
// Example:
//
//	apoc.node.setProperty(node, 'age', 31)
func SetProperty(node *Node, key string, value interface{}) *Node {
	node.Properties[key] = value
	return node
}

// SetProperties sets multiple properties on a node.
//
// Example:
//
//	apoc.node.setProperties(node, {age:31, city:'NYC'})
func SetProperties(node *Node, props map[string]interface{}) *Node {
	for k, v := range props {
		node.Properties[k] = v
	}
	return node
}

// RemoveProperty removes a property from a node.
//
// Example:
//
//	apoc.node.removeProperty(node, 'age')
func RemoveProperty(node *Node, key string) *Node {
	delete(node.Properties, key)
	return node
}

// RemoveProperties removes multiple properties from a node.
//
// Example:
//
//	apoc.node.removeProperties(node, ['age', 'city'])
func RemoveProperties(node *Node, keys []string) *Node {
	for _, key := range keys {
		delete(node.Properties, key)
	}
	return node
}

// AddLabel adds a label to a node.
//
// Example:
//
//	apoc.node.addLabel(node, 'Employee')
func AddLabel(node *Node, label string) *Node {
	if !HasLabel(node, label) {
		node.Labels = append(node.Labels, label)
	}
	return node
}

// AddLabels adds multiple labels to a node.
//
// Example:
//
//	apoc.node.addLabels(node, ['Employee', 'Manager'])
func AddLabels(node *Node, labels []string) *Node {
	for _, label := range labels {
		AddLabel(node, label)
	}
	return node
}

// RemoveLabel removes a label from a node.
//
// Example:
//
//	apoc.node.removeLabel(node, 'Employee')
func RemoveLabel(node *Node, label string) *Node {
	newLabels := make([]string, 0)
	for _, l := range node.Labels {
		if l != label {
			newLabels = append(newLabels, l)
		}
	}
	node.Labels = newLabels
	return node
}

// RemoveLabels removes multiple labels from a node.
//
// Example:
//
//	apoc.node.removeLabels(node, ['Employee', 'Manager'])
func RemoveLabels(node *Node, labels []string) *Node {
	for _, label := range labels {
		RemoveLabel(node, label)
	}
	return node
}

// Clone creates a copy of a node.
//
// Example:
//
//	apoc.node.clone(node) => new node with same properties
func Clone(node *Node) *Node {
	newNode := &Node{
		ID:         0, // New node gets new ID
		Labels:     make([]string, len(node.Labels)),
		Properties: make(map[string]interface{}),
	}

	copy(newNode.Labels, node.Labels)

	for k, v := range node.Properties {
		newNode.Properties[k] = v
	}

	return newNode
}

// Diff compares two nodes and returns differences.
//
// Example:
//
//	apoc.node.diff(node1, node2)
//	=> {added:{...}, removed:{...}, changed:{...}}
func Diff(node1, node2 *Node) map[string]interface{} {
	added := make(map[string]interface{})
	removed := make(map[string]interface{})
	changed := make(map[string]interface{})

	// Check for added/changed properties
	for k, v2 := range node2.Properties {
		if v1, ok := node1.Properties[k]; ok {
			if fmt.Sprintf("%v", v1) != fmt.Sprintf("%v", v2) {
				changed[k] = map[string]interface{}{
					"old": v1,
					"new": v2,
				}
			}
		} else {
			added[k] = v2
		}
	}

	// Check for removed properties
	for k, v := range node1.Properties {
		if _, ok := node2.Properties[k]; !ok {
			removed[k] = v
		}
	}

	return map[string]interface{}{
		"added":   added,
		"removed": removed,
		"changed": changed,
	}
}

// Equals checks if two nodes are equal.
//
// Example:
//
//	apoc.node.equals(node1, node2) => true
func Equals(node1, node2 *Node) bool {
	if node1.ID != node2.ID {
		return false
	}

	if len(node1.Labels) != len(node2.Labels) {
		return false
	}

	for i, label := range node1.Labels {
		if label != node2.Labels[i] {
			return false
		}
	}

	if len(node1.Properties) != len(node2.Properties) {
		return false
	}

	for k, v1 := range node1.Properties {
		if v2, ok := node2.Properties[k]; !ok || fmt.Sprintf("%v", v1) != fmt.Sprintf("%v", v2) {
			return false
		}
	}

	return true
}
