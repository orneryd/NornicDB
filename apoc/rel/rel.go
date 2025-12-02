// Package rel provides APOC relationship operations.
//
// This package implements all apoc.rel.* functions for working
// with relationships in graph queries.
package rel

import (
	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Storage is the interface for database operations.
var Storage storage.Storage = storage.NewInMemoryStorage()

// ID returns the ID of a relationship.
//
// Example:
//
//	apoc.rel.id(rel) => 123
func ID(rel *Relationship) int64 {
	return rel.ID
}

// Type returns the type of a relationship.
//
// Example:
//
//	apoc.rel.type(rel) => 'KNOWS'
func Type(rel *Relationship) string {
	return rel.Type
}

// Properties returns all properties of a relationship.
//
// Example:
//
//	apoc.rel.properties(rel) => {since: 2020, weight: 0.8}
func Properties(rel *Relationship) map[string]interface{} {
	return rel.Properties
}

// Property returns a specific property value.
//
// Example:
//
//	apoc.rel.property(rel, 'since') => 2020
func Property(rel *Relationship, key string) interface{} {
	return rel.Properties[key]
}

// StartNode returns the start node of a relationship.
//
// Example:
//
//	apoc.rel.startNode(rel) => start node
func StartNode(rel *Relationship) *Node {
	node, _ := Storage.GetNode(rel.StartNode)
	return node
}

// EndNode returns the end node of a relationship.
//
// Example:
//
//	apoc.rel.endNode(rel) => end node
func EndNode(rel *Relationship) *Node {
	node, _ := Storage.GetNode(rel.EndNode)
	return node
}

// Nodes returns both start and end nodes.
//
// Example:
//
//	apoc.rel.nodes(rel) => [startNode, endNode]
func Nodes(rel *Relationship) []*Node {
	start, _ := Storage.GetNode(rel.StartNode)
	end, _ := Storage.GetNode(rel.EndNode)
	return []*Node{start, end}
}

// SetProperty sets a property on a relationship.
//
// Example:
//
//	apoc.rel.setProperty(rel, 'weight', 0.9) => updated rel
func SetProperty(rel *Relationship, key string, value interface{}) *Relationship {
	rel.Properties[key] = value
	return rel
}

// SetProperties sets multiple properties on a relationship.
//
// Example:
//
//	apoc.rel.setProperties(rel, {weight: 0.9, active: true}) => updated rel
func SetProperties(rel *Relationship, props map[string]interface{}) *Relationship {
	for k, v := range props {
		rel.Properties[k] = v
	}
	return rel
}

// RemoveProperty removes a property from a relationship.
//
// Example:
//
//	apoc.rel.removeProperty(rel, 'weight') => updated rel
func RemoveProperty(rel *Relationship, key string) *Relationship {
	delete(rel.Properties, key)
	return rel
}

// RemoveProperties removes multiple properties from a relationship.
//
// Example:
//
//	apoc.rel.removeProperties(rel, ['weight', 'active']) => updated rel
func RemoveProperties(rel *Relationship, keys []string) *Relationship {
	for _, key := range keys {
		delete(rel.Properties, key)
	}
	return rel
}

// ToMap converts a relationship to a map.
//
// Example:
//
//	apoc.rel.toMap(rel) => {id: 123, type: 'KNOWS', properties: {...}}
func ToMap(rel *Relationship) map[string]interface{} {
	return map[string]interface{}{
		"id":         rel.ID,
		"type":       rel.Type,
		"startNode":  rel.StartNode,
		"endNode":    rel.EndNode,
		"properties": rel.Properties,
	}
}

// FromMap creates a relationship from a map.
//
// Example:
//
//	apoc.rel.fromMap({type: 'KNOWS', startNode: 1, endNode: 2}) => relationship
func FromMap(m map[string]interface{}) *Relationship {
	rel := &Relationship{
		Properties: make(map[string]interface{}),
	}

	if id, ok := m["id"].(int64); ok {
		rel.ID = id
	}

	if relType, ok := m["type"].(string); ok {
		rel.Type = relType
	}

	if start, ok := m["startNode"].(int64); ok {
		rel.StartNode = start
	}

	if end, ok := m["endNode"].(int64); ok {
		rel.EndNode = end
	}

	if props, ok := m["properties"].(map[string]interface{}); ok {
		rel.Properties = props
	}

	return rel
}

// Exists checks if a relationship exists.
//
// Example:
//
//	apoc.rel.exists(relId) => true/false
func Exists(relID int64) bool {
	rel, err := Storage.GetRelationship(relID)
	return err == nil && rel != nil
}

// Delete deletes a relationship.
//
// Example:
//
//	apoc.rel.delete(rel) => success
func Delete(rel *Relationship) bool {
	err := Storage.DeleteRelationship(rel.ID)
	return err == nil
}

// Clone creates a copy of a relationship.
//
// Example:
//
//	apoc.rel.clone(rel) => new relationship
func Clone(rel *Relationship) *Relationship {
	props := make(map[string]interface{})
	for k, v := range rel.Properties {
		props[k] = v
	}

	created, _ := Storage.CreateRelationship(rel.StartNode, rel.EndNode, rel.Type, props)
	return created
}

// Reverse reverses the direction of a relationship.
//
// Example:
//
//	apoc.rel.reverse(rel) => reversed relationship
func Reverse(rel *Relationship) *Relationship {
	rel.StartNode, rel.EndNode = rel.EndNode, rel.StartNode
	return rel
}

// IsType checks if a relationship is of a specific type.
//
// Example:
//
//	apoc.rel.isType(rel, 'KNOWS') => true/false
func IsType(rel *Relationship, relType string) bool {
	return rel.Type == relType
}

// IsAnyType checks if a relationship is any of the specified types.
//
// Example:
//
//	apoc.rel.isAnyType(rel, ['KNOWS', 'FOLLOWS']) => true/false
func IsAnyType(rel *Relationship, types []string) bool {
	for _, t := range types {
		if rel.Type == t {
			return true
		}
	}
	return false
}

// HasProperty checks if a relationship has a property.
//
// Example:
//
//	apoc.rel.hasProperty(rel, 'weight') => true/false
func HasProperty(rel *Relationship, key string) bool {
	_, ok := rel.Properties[key]
	return ok
}

// HasProperties checks if a relationship has all specified properties.
//
// Example:
//
//	apoc.rel.hasProperties(rel, ['weight', 'since']) => true/false
func HasProperties(rel *Relationship, keys []string) bool {
	for _, key := range keys {
		if !HasProperty(rel, key) {
			return false
		}
	}
	return true
}

// Equals checks if two relationships are equal.
//
// Example:
//
//	apoc.rel.equals(rel1, rel2) => true/false
func Equals(rel1, rel2 *Relationship) bool {
	return rel1.ID == rel2.ID
}

// Compare compares two relationships.
//
// Example:
//
//	apoc.rel.compare(rel1, rel2) => {same: true, diff: {...}}
func Compare(rel1, rel2 *Relationship) map[string]interface{} {
	same := rel1.ID == rel2.ID &&
		rel1.Type == rel2.Type &&
		rel1.StartNode == rel2.StartNode &&
		rel1.EndNode == rel2.EndNode

	diff := make(map[string]interface{})

	if rel1.Type != rel2.Type {
		diff["type"] = map[string]interface{}{
			"old": rel1.Type,
			"new": rel2.Type,
		}
	}

	return map[string]interface{}{
		"same": same,
		"diff": diff,
	}
}

// Weight returns the weight of a relationship.
//
// Example:
//
//	apoc.rel.weight(rel, 'weight', 1.0) => weight value
func Weight(rel *Relationship, property string, defaultValue float64) float64 {
	if val, ok := rel.Properties[property]; ok {
		if weight, ok := val.(float64); ok {
			return weight
		}
	}
	return defaultValue
}

// Direction returns the direction relative to a node.
//
// Example:
//
//	apoc.rel.direction(rel, node) => 'OUTGOING' or 'INCOMING'
func Direction(rel *Relationship, node *Node) string {
	if rel.StartNode == node.ID {
		return "OUTGOING"
	} else if rel.EndNode == node.ID {
		return "INCOMING"
	}
	return "NONE"
}

// OtherNode returns the other node in a relationship.
//
// Example:
//
//	apoc.rel.otherNode(rel, node) => other node
func OtherNode(rel *Relationship, node *Node) *Node {
	if rel.StartNode == node.ID {
		otherNode, _ := Storage.GetNode(rel.EndNode)
		return otherNode
	} else if rel.EndNode == node.ID {
		otherNode, _ := Storage.GetNode(rel.StartNode)
		return otherNode
	}
	return nil
}

// IsLoop checks if a relationship is a self-loop.
//
// Example:
//
//	apoc.rel.isLoop(rel) => true/false
func IsLoop(rel *Relationship) bool {
	return rel.StartNode == rel.EndNode
}

// IsBetween checks if a relationship connects two specific nodes.
//
// Example:
//
//	apoc.rel.isBetween(rel, node1, node2) => true/false
func IsBetween(rel *Relationship, node1, node2 *Node) bool {
	return (rel.StartNode == node1.ID && rel.EndNode == node2.ID) ||
		(rel.StartNode == node2.ID && rel.EndNode == node1.ID)
}

// IsDirectedBetween checks if a relationship is directed from node1 to node2.
//
// Example:
//
//	apoc.rel.isDirectedBetween(rel, node1, node2) => true/false
func IsDirectedBetween(rel *Relationship, node1, node2 *Node) bool {
	return rel.StartNode == node1.ID && rel.EndNode == node2.ID
}
