// Package create provides APOC dynamic creation functions.
//
// This package implements all apoc.create.* functions for dynamically
// creating nodes and relationships in Cypher queries.
package create

import (
	"fmt"
)

// NodeData represents a graph node result.
type NodeData struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

// RelData represents a graph relationship result.
type RelData struct {
	ID         int64
	Type       string
	StartNode  int64
	EndNode    int64
	Properties map[string]interface{}
}

// Node creates a new node with labels and properties.
//
// Example:
//
//	apoc.create.node(['Person'], {name: 'Alice', age: 30})
func Node(labels []string, properties map[string]interface{}) *NodeData {
	return &NodeData{
		ID:         generateID(),
		Labels:     labels,
		Properties: properties,
	}
}

// Nodes creates multiple nodes with the same labels.
//
// Example:
//
//	apoc.create.nodes(['Person'], [{name:'Alice'}, {name:'Bob'}])
func Nodes(labels []string, propertiesList []map[string]interface{}) []*NodeData {
	nodes := make([]*NodeData, len(propertiesList))
	for i, props := range propertiesList {
		nodes[i] = Node(labels, props)
	}
	return nodes
}

// Relationship creates a new relationship between nodes.
//
// Example:
//
//	apoc.create.relationship(alice, 'KNOWS', {since: 2020}, bob)
func Relationship(from *NodeData, relType string, properties map[string]interface{}, to *NodeData) *RelData {
	return &RelData{
		ID:         generateID(),
		Type:       relType,
		StartNode:  from.ID,
		EndNode:    to.ID,
		Properties: properties,
	}
}

// VNode creates a virtual node (not persisted).
//
// Example:
//
//	apoc.create.vNode(['Person'], {name: 'Virtual'})
func VNode(labels []string, properties map[string]interface{}) *NodeData {
	return &NodeData{
		ID:         -generateID(), // Negative ID for virtual
		Labels:     labels,
		Properties: properties,
	}
}

// VNodes creates multiple virtual nodes.
//
// Example:
//
//	apoc.create.vNodes(['Person'], [{name:'A'}, {name:'B'}])
func VNodes(labels []string, propertiesList []map[string]interface{}) []*NodeData {
	nodes := make([]*NodeData, len(propertiesList))
	for i, props := range propertiesList {
		nodes[i] = VNode(labels, props)
	}
	return nodes
}

// VRelationship creates a virtual relationship.
//
// Example:
//
//	apoc.create.vRelationship(node1, 'KNOWS', {}, node2)
func VRelationship(from *NodeData, relType string, properties map[string]interface{}, to *NodeData) *RelData {
	return &RelData{
		ID:         -generateID(), // Negative ID for virtual
		Type:       relType,
		StartNode:  from.ID,
		EndNode:    to.ID,
		Properties: properties,
	}
}

// VPattern creates a virtual pattern (nodes and relationships).
//
// Example:
//
//	apoc.create.vPattern({_labels:['Person'], name:'Alice'}, 'KNOWS', {}, {_labels:['Person'], name:'Bob'})
func VPattern(startProps map[string]interface{}, relType string, relProps map[string]interface{}, endProps map[string]interface{}) (*NodeData, *RelData, *NodeData) {
	startLabels := extractLabels(startProps)
	endLabels := extractLabels(endProps)

	start := VNode(startLabels, startProps)
	end := VNode(endLabels, endProps)
	rel := VRelationship(start, relType, relProps, end)

	return start, rel, end
}

// AddLabels adds labels to a node.
//
// Example:
//
//	apoc.create.addLabels(node, ['Employee', 'Manager'])
func AddLabels(node *NodeData, labels []string) *NodeData {
	for _, label := range labels {
		if !hasLabel(node, label) {
			node.Labels = append(node.Labels, label)
		}
	}
	return node
}

// RemoveLabels removes labels from a node.
//
// Example:
//
//	apoc.create.removeLabels(node, ['Employee'])
func RemoveLabels(node *NodeData, labels []string) *NodeData {
	newLabels := make([]string, 0)
	labelSet := make(map[string]bool)
	for _, label := range labels {
		labelSet[label] = true
	}

	for _, label := range node.Labels {
		if !labelSet[label] {
			newLabels = append(newLabels, label)
		}
	}

	node.Labels = newLabels
	return node
}

// SetProperty sets a property on a node.
//
// Example:
//
//	apoc.create.setProperty(node, 'age', 31)
func SetProperty(node *NodeData, key string, value interface{}) *NodeData {
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	node.Properties[key] = value
	return node
}

// SetProperties sets multiple properties on a node.
//
// Example:
//
//	apoc.create.setProperties(node, {age: 31, city: 'NYC'})
func SetProperties(node *NodeData, properties map[string]interface{}) *NodeData {
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	for k, v := range properties {
		node.Properties[k] = v
	}
	return node
}

// RemoveProperties removes properties from a node.
//
// Example:
//
//	apoc.create.removeProperties(node, ['age', 'city'])
func RemoveProperties(node *NodeData, keys []string) *NodeData {
	for _, key := range keys {
		delete(node.Properties, key)
	}
	return node
}

// SetRelProperty sets a property on a relationship.
//
// Example:
//
//	apoc.create.setRelProperty(rel, 'weight', 0.8)
func SetRelProperty(rel *RelData, key string, value interface{}) *RelData {
	if rel.Properties == nil {
		rel.Properties = make(map[string]interface{})
	}
	rel.Properties[key] = value
	return rel
}

// SetRelProperties sets multiple properties on a relationship.
//
// Example:
//
//	apoc.create.setRelProperties(rel, {weight: 0.8, since: 2020})
func SetRelProperties(rel *RelData, properties map[string]interface{}) *RelData {
	if rel.Properties == nil {
		rel.Properties = make(map[string]interface{})
	}
	for k, v := range properties {
		rel.Properties[k] = v
	}
	return rel
}

// RemoveRelProperties removes properties from a relationship.
//
// Example:
//
//	apoc.create.removeRelProperties(rel, ['weight'])
func RemoveRelProperties(rel *RelData, keys []string) *RelData {
	for _, key := range keys {
		delete(rel.Properties, key)
	}
	return rel
}

// UUID generates a UUID for a node.
//
// Example:
//
//	apoc.create.uuid() => '550e8400-e29b-41d4-a716-446655440000'
func UUID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		generateID()&0xffffffff,
		generateID()&0xffff,
		(generateID()&0x0fff)|0x4000,
		(generateID()&0x3fff)|0x8000,
		generateID()&0xffffffffffff,
	)
}

// UUIDs generates multiple UUIDs.
//
// Example:
//
//	apoc.create.uuids(5) => ['uuid1', 'uuid2', ...]
func UUIDs(count int) []string {
	uuids := make([]string, count)
	for i := 0; i < count; i++ {
		uuids[i] = UUID()
	}
	return uuids
}

// Clone creates a copy of a node.
//
// Example:
//
//	apoc.create.clone(node) => new node with same properties
func Clone(node *NodeData) *NodeData {
	newNode := &NodeData{
		ID:         generateID(),
		Labels:     make([]string, len(node.Labels)),
		Properties: make(map[string]interface{}),
	}

	copy(newNode.Labels, node.Labels)

	for k, v := range node.Properties {
		newNode.Properties[k] = v
	}

	return newNode
}

// CloneSubgraph clones a subgraph of nodes and relationships.
//
// Example:
//
//	apoc.create.cloneSubgraph([node1, node2], [rel1])
func CloneSubgraph(nodes []*NodeData, rels []*RelData) ([]*NodeData, []*RelData) {
	// Map old IDs to new nodes
	idMap := make(map[int64]*NodeData)

	// Clone nodes
	newNodes := make([]*NodeData, len(nodes))
	for i, node := range nodes {
		newNodes[i] = Clone(node)
		idMap[node.ID] = newNodes[i]
	}

	// Clone relationships
	newRels := make([]*RelData, len(rels))
	for i, rel := range rels {
		newRels[i] = &RelData{
			ID:         generateID(),
			Type:       rel.Type,
			StartNode:  idMap[rel.StartNode].ID,
			EndNode:    idMap[rel.EndNode].ID,
			Properties: make(map[string]interface{}),
		}

		for k, v := range rel.Properties {
			newRels[i].Properties[k] = v
		}
	}

	return newNodes, newRels
}

// Helper functions

var idCounter int64 = 1

func generateID() int64 {
	id := idCounter
	idCounter++
	return id
}

func hasLabel(node *NodeData, label string) bool {
	for _, l := range node.Labels {
		if l == label {
			return true
		}
	}
	return false
}

func extractLabels(props map[string]interface{}) []string {
	if labels, ok := props["_labels"].([]interface{}); ok {
		result := make([]string, len(labels))
		for i, label := range labels {
			if labelStr, ok := label.(string); ok {
				result[i] = labelStr
			}
		}
		return result
	}
	return []string{}
}
