// Package nodes provides APOC batch node operations.
//
// This package implements all apoc.nodes.* functions for batch
// operations on multiple nodes.
package nodes

import "errors"

import (
	"context"
	"fmt"
	"sort"

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

// Get retrieves multiple nodes by IDs.
//
// Example:
//
//	apoc.nodes.get([1, 2, 3]) => [node1, node2, node3]
func (f *Functions) Get(ctx context.Context, ids []int64) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	nodes := make([]*Node, 0, len(ids))
	for _, id := range ids {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		{
			node, err := f.backend.GetNode(ctx, id)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return nil, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if err == nil {
				nodes = append(nodes, node)
			}
		}
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return nodes, nil
}

// Delete deletes multiple nodes.
//
// Example:
//
//	apoc.nodes.delete([node1, node2], true) => deleted count
func (f *Functions) Delete(ctx context.Context, nodes []*Node, detachRelationships bool) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	count := 0
	for _, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}

		if detachRelationships {
			// Delete relationships first
			rels, storageError2 := f.backend.GetNodeRelationships(ctx, node.ID, "", storage.DirectionBoth)
			if storageError2 != nil && !errors.Is(storageError2, storage.ErrNodeNotFound) {
				return 0, storageError2
			}
			if operationError := ctx.Err(); operationError != nil {
				return 0, operationError
			}

			for _, rel := range rels {
				if operationError := ctx.Err(); operationError != nil {
					return 0, operationError
				}

				{
					storageError3 := f.backend.DeleteRelationship(ctx, rel.ID)
					if storageError3 != nil && !errors.Is(storageError3, storage.ErrRelationshipNotFound) {
						return 0, storageError3
					}
					if operationError := ctx.Err(); operationError != nil {
						return 0, operationError
					}

				}
			}
		}
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

// Link creates relationships between multiple nodes.
//
// Example:
//
//	apoc.nodes.link([node1, node2, node3], 'NEXT') => relationships
func (f *Functions) Link(ctx context.Context, nodes []*Node, relType string) ([]*Relationship, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	rels := make([]*Relationship, 0)
	for i := 0; i < len(nodes)-1; i++ {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		rel := &Relationship{
			Type:       relType,
			StartNode:  nodes[i].ID,
			EndNode:    nodes[i+1].ID,
			Properties: make(map[string]interface{}),
		}
		{
			created, err := f.backend.CreateRelationship(ctx, rel.StartNode, rel.EndNode, rel.Type, rel.Properties)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return nil, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if err == nil {
				rels = append(rels, created)
			}
		}
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return rels, nil
}

// Collapse collapses a path of nodes into a single relationship.
//
// Example:
//
//	apoc.nodes.collapse([node1, node2, node3], 'CONNECTED') => relationship
func (f *Functions) Collapse(ctx context.Context, nodes []*Node, relType string) (*Relationship, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if len(nodes) < 2 {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return nil, nil
	}

	rel := &Relationship{
		Type:       relType,
		StartNode:  nodes[0].ID,
		EndNode:    nodes[len(nodes)-1].ID,
		Properties: make(map[string]interface{}),
	}

	// Store intermediate nodes as property
	intermediateIDs := make([]int64, 0)
	for i := 1; i < len(nodes)-1; i++ {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		intermediateIDs = append(intermediateIDs, nodes[i].ID)
	}
	if len(intermediateIDs) > 0 {
		rel.Properties["intermediateNodes"] = intermediateIDs
	}

	created, storageError6 := f.backend.CreateRelationship(ctx, rel.StartNode, rel.EndNode, rel.Type, rel.Properties)
	if storageError6 != nil && !errors.Is(storageError6, storage.ErrNodeNotFound) {
		return nil, storageError6
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return created, nil
}

// Group groups nodes by a property value.
//
// Example:
//
//	apoc.nodes.group(nodes, 'category') => grouped map
func Group(nodes []*Node, property string) map[interface{}][]*Node {
	groups := make(map[interface{}][]*Node)

	for _, node := range nodes {
		if value, ok := node.Properties[property]; ok {
			key := fmt.Sprintf("%v", value)
			groups[key] = append(groups[key], node)
		} else {
			groups[nil] = append(groups[nil], node)
		}
	}

	return groups
}

// Partition partitions nodes into N groups.
//
// Example:
//
//	apoc.nodes.partition(nodes, 3) => [group1, group2, group3]
func Partition(nodes []*Node, partitions int) [][]*Node {
	if partitions <= 0 {
		return [][]*Node{nodes}
	}

	result := make([][]*Node, partitions)
	for i := range result {
		result[i] = make([]*Node, 0)
	}

	for i, node := range nodes {
		partition := i % partitions
		result[partition] = append(result[partition], node)
	}

	return result
}

// Distinct returns distinct nodes (by ID).
//
// Example:
//
//	apoc.nodes.distinct([node1, node1, node2]) => [node1, node2]
func Distinct(nodes []*Node) []*Node {
	seen := make(map[int64]bool)
	result := make([]*Node, 0)

	for _, node := range nodes {
		if !seen[node.ID] {
			seen[node.ID] = true
			result = append(result, node)
		}
	}

	return result
}

// Intersect returns nodes that appear in all lists.
//
// Example:
//
//	apoc.nodes.intersect([list1, list2]) => common nodes
func Intersect(nodeLists [][]*Node) []*Node {
	if len(nodeLists) == 0 {
		return []*Node{}
	}

	// Count occurrences
	counts := make(map[int64]int)
	for _, list := range nodeLists {
		seen := make(map[int64]bool)
		for _, node := range list {
			if !seen[node.ID] {
				counts[node.ID]++
				seen[node.ID] = true
			}
		}
	}

	// Find nodes in all lists
	result := make([]*Node, 0)
	for _, node := range nodeLists[0] {
		if counts[node.ID] == len(nodeLists) {
			result = append(result, node)
		}
	}

	return result
}

// Union returns all unique nodes from multiple lists.
//
// Example:
//
//	apoc.nodes.union([list1, list2]) => all unique nodes
func Union(nodeLists [][]*Node) []*Node {
	seen := make(map[int64]*Node)

	for _, list := range nodeLists {
		for _, node := range list {
			seen[node.ID] = node
		}
	}

	result := make([]*Node, 0, len(seen))
	for _, node := range seen {
		result = append(result, node)
	}

	return result
}

// Difference returns nodes in first list but not in others.
//
// Example:
//
//	apoc.nodes.difference(list1, [list2, list3]) => unique to list1
func Difference(nodes []*Node, excludeLists [][]*Node) []*Node {
	exclude := make(map[int64]bool)

	for _, list := range excludeLists {
		for _, node := range list {
			exclude[node.ID] = true
		}
	}

	result := make([]*Node, 0)
	for _, node := range nodes {
		if !exclude[node.ID] {
			result = append(result, node)
		}
	}

	return result
}

// Sort sorts nodes by a property.
//
// Example:
//
//	apoc.nodes.sort(nodes, 'age', true) => sorted nodes
func Sort(nodes []*Node, property string, ascending bool) []*Node {
	sorted := make([]*Node, len(nodes))
	copy(sorted, nodes)

	sort.Slice(sorted, func(i, j int) bool {
		vi := sorted[i].Properties[property]
		vj := sorted[j].Properties[property]

		// Compare based on type
		switch vi.(type) {
		case int, int64:
			ii, _ := vi.(int64)
			jj, _ := vj.(int64)
			if ascending {
				return ii < jj
			}
			return ii > jj
		case float64:
			fi, _ := vi.(float64)
			fj, _ := vj.(float64)
			if ascending {
				return fi < fj
			}
			return fi > fj
		case string:
			si, _ := vi.(string)
			sj, _ := vj.(string)
			if ascending {
				return si < sj
			}
			return si > sj
		}

		return false
	})

	return sorted
}

// Filter filters nodes by a predicate function.
//
// Example:
//
//	apoc.nodes.filter(nodes, func(n) { return n.Properties["age"] > 18 })
func Filter(nodes []*Node, predicate func(*Node) bool) []*Node {
	result := make([]*Node, 0)
	for _, node := range nodes {
		if predicate(node) {
			result = append(result, node)
		}
	}
	return result
}

// Map transforms nodes using a function.
//
// Example:
//
//	apoc.nodes.map(nodes, func(n) { return n.Properties["name"] })
func Map(nodes []*Node, transform func(*Node) interface{}) []interface{} {
	result := make([]interface{}, len(nodes))
	for i, node := range nodes {
		result[i] = transform(node)
	}
	return result
}

// Reduce reduces nodes to a single value.
//
// Example:
//
//	apoc.nodes.reduce(nodes, 0, func(acc, n) { return acc + n.Properties["count"] })
func Reduce(nodes []*Node, initial interface{}, reducer func(interface{}, *Node) interface{}) interface{} {
	result := initial
	for _, node := range nodes {
		result = reducer(result, node)
	}
	return result
}

// Connected checks if all nodes are connected.
//
// Example:
//
//	apoc.nodes.connected([node1, node2, node3]) => true/false
func (f *Functions) Connected(ctx context.Context, nodes []*Node) (bool, error) {
	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}

	if len(nodes) <= 1 {
		if operationError := ctx.Err(); operationError != nil {
			return false, operationError
		}
		return true, nil
	}

	for i := 0; i < len(nodes)-1; i++ {
		if operationError := ctx.Err(); operationError != nil {
			return false, operationError
		}

		path, err := f.backend.FindShortestPath(ctx, nodes[i].ID, nodes[i+1].ID, "", 10)
		if err != nil && !errors.Is(err, storage.ErrPathNotFound) {
			return false, err
		}
		if operationError := ctx.Err(); operationError != nil {
			return false, operationError
		}

		if err != nil || path == nil {
			if operationError := ctx.Err(); operationError != nil {
				return false, operationError
			}
			return false, nil
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}
	return true, nil
}

// IsDense checks if nodes are densely connected.
//
// Example:
//
//	apoc.nodes.isDense(nodes, 0.5) => true/false
func (f *Functions) IsDense(ctx context.Context, nodes []*Node, threshold float64) (bool, error) {
	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}

	if len(nodes) <= 1 {
		if operationError := ctx.Err(); operationError != nil {
			return false, operationError
		}
		return false, nil
	}

	totalPossible := len(nodes) * (len(nodes) - 1) / 2
	actualConnections := 0

	for i := 0; i < len(nodes); i++ {
		if operationError := ctx.Err(); operationError != nil {
			return false, operationError
		}

		for j := i + 1; j < len(nodes); j++ {
			if operationError := ctx.Err(); operationError != nil {
				return false, operationError
			}

			path, err := f.backend.FindShortestPath(ctx, nodes[i].ID, nodes[j].ID, "", 1)
			if err != nil && !errors.Is(err, storage.ErrPathNotFound) {
				return false, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return false, operationError
			}

			if err == nil && path != nil {
				actualConnections++
			}
		}
	}

	density := float64(actualConnections) / float64(totalPossible)
	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}
	return density >= threshold, nil
}

// Relationships returns all relationships between nodes.
//
// Example:
//
//	apoc.nodes.relationships(nodes, 'KNOWS') => relationships
func (f *Functions) Relationships(ctx context.Context, nodes []*Node, relType string) ([]*Relationship, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	nodeSet := make(map[int64]bool)
	for _, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		nodeSet[node.ID] = true
	}

	rels := make([]*Relationship, 0)
	for _, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		nodeRels, err := f.backend.GetNodeRelationships(ctx, node.ID, relType, storage.DirectionBoth)
		if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
			return nil, err
		}
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if err == nil {
			for _, rel := range nodeRels {
				if operationError := ctx.Err(); operationError != nil {
					return nil, operationError
				}

				// Only include if both nodes are in the set
				if nodeSet[rel.StartNode] && nodeSet[rel.EndNode] {
					rels = append(rels, rel)
				}
			}
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return DistinctRels(rels), nil
}

// DistinctRels returns distinct relationships.
func DistinctRels(rels []*Relationship) []*Relationship {
	seen := make(map[int64]bool)
	result := make([]*Relationship, 0)

	for _, rel := range rels {
		if !seen[rel.ID] {
			seen[rel.ID] = true
			result = append(result, rel)
		}
	}

	return result
}

// ToMap converts nodes to a map keyed by property.
//
// Example:
//
//	apoc.nodes.toMap(nodes, 'id') => {1: node1, 2: node2}
func ToMap(nodes []*Node, keyProperty string) map[interface{}]*Node {
	result := make(map[interface{}]*Node)

	for _, node := range nodes {
		if key, ok := node.Properties[keyProperty]; ok {
			result[key] = node
		}
	}

	return result
}

// FromMap creates nodes from a map.
//
// Example:
//
//	apoc.nodes.fromMap({1: {name: 'Alice'}, 2: {name: 'Bob'}}) => nodes
func (f *Functions) FromMap(ctx context.Context, data map[string]interface{}, labels []string) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	nodes := make([]*Node, 0, len(data))

	for _, value := range data {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if props, ok := value.(map[string]interface{}); ok {
			node := &Node{
				Labels:     labels,
				Properties: props,
			}
			{
				created, err := f.backend.CreateNode(ctx, node.Labels, node.Properties)
				if err != nil {
					return nil, err
				}
				if operationError := ctx.Err(); operationError != nil {
					return nil, operationError
				}

				if err == nil {
					nodes = append(nodes, created)
				}
			}
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return nodes, nil
}

// Batch processes nodes in batches.
//
// Example:
//
//	apoc.nodes.batch(nodes, 100, processFunc) => results
func Batch(nodes []*Node, batchSize int, process func([]*Node) interface{}) []interface{} {
	results := make([]interface{}, 0)

	for i := 0; i < len(nodes); i += batchSize {
		end := i + batchSize
		if end > len(nodes) {
			end = len(nodes)
		}

		batch := nodes[i:end]
		result := process(batch)
		results = append(results, result)
	}

	return results
}

// Cycles detects cycles involving the given nodes.
//
// Example:
//
//	apoc.nodes.cycles(nodes) => cycles found
func (f *Functions) Cycles(ctx context.Context, nodes []*Node) ([][]int64, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	cycles := make([][]int64, 0)

	for _, node := range nodes {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		visited := make(map[int64]bool)
		path := make([]int64, 0)

		var dfs func(int64) bool
		dfs = func(id int64) bool {
			if visited[id] {
				// Found cycle
				cycleStart := -1
				for i, nid := range path {
					if nid == id {
						cycleStart = i
						break
					}
				}
				if cycleStart >= 0 {
					cycle := make([]int64, len(path)-cycleStart)
					copy(cycle, path[cycleStart:])
					cycles = append(cycles, cycle)
				}
				return true
			}

			visited[id] = true
			path = append(path, id)

			neighbors, err := f.backend.GetNodeNeighbors(ctx, id, "", storage.DirectionOutgoing)
			if err == nil {
				for _, neighbor := range neighbors {
					dfs(neighbor.ID)
				}
			}

			path = path[:len(path)-1]
			return false
		}

		dfs(node.ID)
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return cycles, nil
}
