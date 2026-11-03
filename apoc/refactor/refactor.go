// Package refactor provides APOC graph refactoring functions.
//
// This package implements all apoc.refactor.* functions for restructuring
// and transforming graph data.
package refactor

import "errors"

import (
	"context"

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
func (f *Functions) CloneNodes(ctx context.Context, nodes []*Node) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	cloned := make([]*Node, len(nodes))

	for i, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		cloned[i] = &Node{
			Labels:     make([]string, len(node.Labels)),
			Properties: make(map[string]interface{}),
		}
		copy(cloned[i].Labels, node.Labels)
		for k, v := range node.Properties {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			cloned[i].Properties[k] = v
		}

		created, storageError1 := f.backend.CreateNode(ctx, cloned[i].Labels, cloned[i].Properties)
		if storageError1 != nil {
			return nil, storageError1
		}
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		cloned[i] = created
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return cloned, nil
}

// CloneSubgraph clones a subgraph.
//
// Example:
//
//	apoc.refactor.cloneSubgraph([node1, node2], [rel1]) => {nodes: [...], rels: [...]}
func (f *Functions) CloneSubgraph(ctx context.Context, nodes []*Node, rels []*Relationship) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	idMap := make(map[int64]int64)

	// Clone nodes
	newNodes := make([]*Node, len(nodes))
	for i, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		newNodes[i] = &Node{
			Labels:     make([]string, len(node.Labels)),
			Properties: make(map[string]interface{}),
		}
		copy(newNodes[i].Labels, node.Labels)
		for k, v := range node.Properties {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			newNodes[i].Properties[k] = v
		}

		created, storageError2 := f.backend.CreateNode(ctx, newNodes[i].Labels, newNodes[i].Properties)
		if storageError2 != nil {
			return nil, storageError2
		}
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		newNodes[i] = created
		idMap[node.ID] = created.ID
	}

	// Clone relationships
	newRels := make([]*Relationship, len(rels))
	for i, rel := range rels {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		newRels[i] = &Relationship{
			Type:       rel.Type,
			StartNode:  idMap[rel.StartNode],
			EndNode:    idMap[rel.EndNode],
			Properties: make(map[string]interface{}),
		}
		for k, v := range rel.Properties {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			newRels[i].Properties[k] = v
		}

		created, storageError3 := f.backend.CreateRelationship(ctx, newRels[i].StartNode, newRels[i].EndNode, newRels[i].Type, newRels[i].Properties)
		if storageError3 != nil && !errors.Is(storageError3, storage.ErrNodeNotFound) {
			return nil, storageError3
		}
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		newRels[i] = created
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{
		"nodes":         newNodes,
		"relationships": newRels,
	}, nil
}

// CollapseNode collapses a node by connecting its neighbors.
//
// Example:
//
//	apoc.refactor.collapseNode(node, 'CONNECTED') => relationships created
func (f *Functions) CollapseNode(ctx context.Context, node *Node, relType string) ([]*Relationship, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	incoming, storageError4 := f.backend.GetNodeRelationships(ctx, node.ID, "", storage.DirectionIncoming)
	if storageError4 != nil && !errors.Is(storageError4, storage.ErrNodeNotFound) {
		return nil, storageError4
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	outgoing, storageError5 := f.backend.GetNodeRelationships(ctx, node.ID, "", storage.DirectionOutgoing)
	if storageError5 != nil && !errors.Is(storageError5, storage.ErrNodeNotFound) {
		return nil, storageError5
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	newRels := make([]*Relationship, 0)

	for _, in := range incoming {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		for _, out := range outgoing {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			rel := &Relationship{
				Type:       relType,
				StartNode:  in.StartNode,
				EndNode:    out.EndNode,
				Properties: make(map[string]interface{}),
			}

			created, storageError6 := f.backend.CreateRelationship(ctx, rel.StartNode, rel.EndNode, rel.Type, rel.Properties)
			if storageError6 != nil && !errors.Is(storageError6, storage.ErrNodeNotFound) {
				return nil, storageError6
			}
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			newRels = append(newRels, created)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return newRels, nil
}

// ExtractNode extracts a node from a relationship.
//
// Example:
//
//	apoc.refactor.extractNode(rel, ['Person'], {name: 'Intermediate'}) => node
func (f *Functions) ExtractNode(ctx context.Context, rel *Relationship, labels []string, props map[string]interface{}) (*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	created, storageError7 := f.backend.CreateNode(ctx, labels, props)
	if storageError7 != nil {
		return nil, storageError7
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	// Create two new relationships
	rel1 := &Relationship{
		Type:       rel.Type,
		StartNode:  rel.StartNode,
		EndNode:    created.ID,
		Properties: make(map[string]interface{}),
	}
	{
		_, storageError8 := f.backend.CreateRelationship(ctx, rel1.StartNode, rel1.EndNode, rel1.Type, rel1.Properties)
		if storageError8 != nil && !errors.Is(storageError8, storage.ErrNodeNotFound) {
			return nil, storageError8
		}
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

	}

	rel2 := &Relationship{
		Type:       rel.Type,
		StartNode:  created.ID,
		EndNode:    rel.EndNode,
		Properties: make(map[string]interface{}),
	}
	{
		_, storageError9 := f.backend.CreateRelationship(ctx, rel2.StartNode, rel2.EndNode, rel2.Type, rel2.Properties)
		if storageError9 != nil && !errors.Is(storageError9, storage.ErrNodeNotFound) {
			return nil, storageError9
		}
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return created, nil
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

// RenameLabel renames a label on all nodes that have it.
//
// Example:
//
//	apoc.refactor.rename.label('OldLabel', 'NewLabel') => count
func (f *Functions) RenameLabel(ctx context.Context, oldLabel, newLabel string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	nodes, err := f.backend.AllNodes(ctx)
	if err != nil {
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

	count := 0
	for _, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}

		for i, label := range node.Labels {
			if operationError := ctx.Err(); operationError != nil {
				return 0, operationError
			}

			if label == oldLabel {
				node.Labels[i] = newLabel
				{
					err := f.backend.UpdateNodeLabels(ctx, node.ID, node.Labels)
					if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
						return 0, err
					}
					if operationError := ctx.Err(); operationError != nil {
						return 0, operationError
					}

					if err == nil {
						count++
					}
				}
				break
			}
		}
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return count, nil
}

// RenameType renames a relationship type on all relationships.
//
// Example:
//
//	apoc.refactor.rename.type('OLD_TYPE', 'NEW_TYPE') => count
func (f *Functions) RenameType(ctx context.Context, oldType, newType string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	rels, err := f.backend.AllRelationships(ctx)
	if err != nil {
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

	count := 0
	for _, rel := range rels {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}

		if rel.Type == oldType {
			{
				err := f.backend.UpdateRelationshipType(ctx, rel.ID, newType)
				if err != nil && !errors.Is(err, storage.ErrRelationshipNotFound) {
					return 0, err
				}
				if operationError := ctx.Err(); operationError != nil {
					return 0, operationError
				}

				if err == nil {
					count++
				}
			}
		}
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return count, nil
}

// renameProperty owns the property-map transition for node and relationship
// renames. Each caller retains its entity-specific persistence and counts only
// successful updates. Missing keys are untouched, including nil maps; an
// existing destination is replaced, and renaming a key to itself still counts.
func renameProperty(properties map[string]interface{}, oldName, newName string) bool {
	value, exists := properties[oldName]
	if !exists {
		return false
	}
	delete(properties, oldName)
	properties[newName] = value
	return true
}

// RenameProperty renames a property on all nodes that have it.
//
// Example:
//
//	apoc.refactor.rename.nodeProperty('oldName', 'newName') => count
func (f *Functions) RenameProperty(ctx context.Context, oldName, newName string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	nodes, err := f.backend.AllNodes(ctx)
	if err != nil {
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

	count := 0
	for _, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}

		if renameProperty(node.Properties, oldName, newName) {
			{
				err := f.backend.UpdateNode(ctx, node.ID, node.Properties)
				if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
					return 0, err
				}
				if operationError := ctx.Err(); operationError != nil {
					return 0, operationError
				}

				if err == nil {
					count++
				}
			}
		}
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return count, nil
}

// RenameRelProperty renames a property on all relationships that have it.
//
// Example:
//
//	apoc.refactor.rename.relProperty('oldName', 'newName') => count
func (f *Functions) RenameRelProperty(ctx context.Context, oldName, newName string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	// The caller supplies the lifetime for storage work.

	rels, err := f.backend.AllRelationships(ctx)
	if err != nil {
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

	count := 0
	for _, rel := range rels {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}

		if renameProperty(rel.Properties, oldName, newName) {
			{
				err := f.backend.UpdateRelationship(ctx, rel.ID, rel.Properties)
				if err != nil && !errors.Is(err, storage.ErrRelationshipNotFound) {
					return 0, err
				}
				if operationError := ctx.Err(); operationError != nil {
					return 0, operationError
				}

				if err == nil {
					count++
				}
			}
		}
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return count, nil
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
func (f *Functions) DeleteAndReconnect(ctx context.Context, nodes []*Node, relType string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	count := 0

	for _, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}

		// Collapse node first
		_, helperError1 := f.CollapseNode(ctx, node, relType)
		if helperError1 != nil {
			return 0, helperError1
		}

		// Delete node
		{
			err := f.backend.DeleteNode(ctx, node.ID)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return 0, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return 0, operationError
			}

			if err == nil {
				count++
			}
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return count, nil
}

// CloneSubgraphFromPaths clones subgraph from paths.
//
// Example:
//
//	apoc.refactor.cloneSubgraphFromPaths(paths) => {nodes: [...], rels: [...]}
func (f *Functions) CloneSubgraphFromPaths(ctx context.Context, paths []map[string]interface{}) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	nodeSet := make(map[int64]*Node)
	relSet := make(map[int64]*Relationship)

	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if nodes, ok := path["nodes"].([]*Node); ok {
			for _, node := range nodes {
				if operationError := ctx.Err(); operationError != nil {
					return nil, operationError
				}

				nodeSet[node.ID] = node
			}
		}

		if rels, ok := path["relationships"].([]*Relationship); ok {
			for _, rel := range rels {
				if operationError := ctx.Err(); operationError != nil {
					return nil, operationError
				}

				relSet[rel.ID] = rel
			}
		}
	}

	nodes := make([]*Node, 0, len(nodeSet))
	for _, node := range nodeSet {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		nodes = append(nodes, node)
	}

	rels := make([]*Relationship, 0, len(relSet))
	for _, rel := range relSet {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		rels = append(rels, rel)
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return f.CloneSubgraph(ctx, nodes, rels)
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
func (f *Functions) Normalize(ctx context.Context, node *Node, property, newLabel, relType string) (map[string]interface{}, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if val, ok := node.Properties[property]; ok {
		// Create new node for property value
		newNode := &Node{
			Labels:     []string{newLabel},
			Properties: map[string]interface{}{"value": val},
		}

		created, storageError19 := f.backend.CreateNode(ctx, newNode.Labels, newNode.Properties)
		if storageError19 != nil {
			return nil, storageError19
		}
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		// Create relationship
		createdRel, storageError20 := f.backend.CreateRelationship(ctx, node.ID, created.ID, relType, make(map[string]interface{}))
		if storageError20 != nil && !errors.Is(storageError20, storage.ErrNodeNotFound) {
			return nil, storageError20
		}
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		// Remove property from original node
		delete(node.Properties, property)

		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return map[string]interface{}{
			"node":         created,
			"relationship": createdRel,
		}, nil
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return map[string]interface{}{}, nil
}

// Denormalize denormalizes graph structure.
//
// Example:
//
//	apoc.refactor.denormalize(node, 'HAS', 'property') => denormalized
func (f *Functions) Denormalize(ctx context.Context, node *Node, relType, property string) (*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	rels, storageError21 := f.backend.GetNodeRelationships(ctx, node.ID, relType, storage.DirectionOutgoing)
	if storageError21 != nil && !errors.Is(storageError21, storage.ErrNodeNotFound) {
		return nil, storageError21
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	for _, rel := range rels {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		{
			targetNode, err := f.backend.GetNode(ctx, rel.EndNode)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return nil, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if err == nil {
				if val, ok := targetNode.Properties["value"]; ok {
					node.Properties[property] = val
				}
			}
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return node, nil
}
