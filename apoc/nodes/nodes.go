// Package nodes provides APOC batch node operations.
//
// This package implements all apoc.nodes.* functions for batch
// operations on multiple nodes.
package nodes

import (
	"fmt"
	"sort"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Storage is the interface for database operations.
var Storage storage.Storage = storage.NewInMemoryStorage()

// Get retrieves multiple nodes by IDs.
//
// Example:
//
//	apoc.nodes.get([1, 2, 3]) => [node1, node2, node3]
func Get(ids []int64) []*Node {
	nodes := make([]*Node, 0, len(ids))
	for _, id := range ids {
		if node, err := Storage.GetNode(id); err == nil {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// Delete deletes multiple nodes.
//
// Example:
//
//	apoc.nodes.delete([node1, node2], true) => deleted count
func Delete(nodes []*Node, detachRelationships bool) int {
	count := 0
	for _, node := range nodes {
		if detachRelationships {
			// Delete relationships first
			rels, _ := Storage.GetNodeRelationships(node.ID, "", storage.DirectionBoth)
			for _, rel := range rels {
				Storage.DeleteRelationship(rel.ID)
			}
		}
		if err := Storage.DeleteNode(node.ID); err == nil {
			count++
		}
	}
	return count
}

// Link creates relationships between multiple nodes.
//
// Example:
//
//	apoc.nodes.link([node1, node2, node3], 'NEXT') => relationships
func Link(nodes []*Node, relType string) []*Relationship {
	rels := make([]*Relationship, 0)
	for i := 0; i < len(nodes)-1; i++ {
		rel := &Relationship{
			Type:       relType,
			StartNode:  nodes[i].ID,
			EndNode:    nodes[i+1].ID,
			Properties: make(map[string]interface{}),
		}
		if created, err := Storage.CreateRelationship(rel.StartNode, rel.EndNode, rel.Type, rel.Properties); err == nil {
			rels = append(rels, created)
		}
	}
	return rels
}

// Collapse collapses a path of nodes into a single relationship.
//
// Example:
//
//	apoc.nodes.collapse([node1, node2, node3], 'CONNECTED') => relationship
func Collapse(nodes []*Node, relType string) *Relationship {
	if len(nodes) < 2 {
		return nil
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
		intermediateIDs = append(intermediateIDs, nodes[i].ID)
	}
	if len(intermediateIDs) > 0 {
		rel.Properties["intermediateNodes"] = intermediateIDs
	}

	created, _ := Storage.CreateRelationship(rel.StartNode, rel.EndNode, rel.Type, rel.Properties)
	return created
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
func Connected(nodes []*Node) bool {
	if len(nodes) <= 1 {
		return true
	}

	for i := 0; i < len(nodes)-1; i++ {
		path, err := Storage.FindShortestPath(nodes[i].ID, nodes[i+1].ID, "", 10)
		if err != nil || path == nil {
			return false
		}
	}

	return true
}

// IsDense checks if nodes are densely connected.
//
// Example:
//
//	apoc.nodes.isDense(nodes, 0.5) => true/false
func IsDense(nodes []*Node, threshold float64) bool {
	if len(nodes) <= 1 {
		return false
	}

	totalPossible := len(nodes) * (len(nodes) - 1) / 2
	actualConnections := 0

	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			path, err := Storage.FindShortestPath(nodes[i].ID, nodes[j].ID, "", 1)
			if err == nil && path != nil {
				actualConnections++
			}
		}
	}

	density := float64(actualConnections) / float64(totalPossible)
	return density >= threshold
}

// Relationships returns all relationships between nodes.
//
// Example:
//
//	apoc.nodes.relationships(nodes, 'KNOWS') => relationships
func Relationships(nodes []*Node, relType string) []*Relationship {
	nodeSet := make(map[int64]bool)
	for _, node := range nodes {
		nodeSet[node.ID] = true
	}

	rels := make([]*Relationship, 0)
	for _, node := range nodes {
		nodeRels, err := Storage.GetNodeRelationships(node.ID, relType, storage.DirectionBoth)
		if err == nil {
			for _, rel := range nodeRels {
				// Only include if both nodes are in the set
				if nodeSet[rel.StartNode] && nodeSet[rel.EndNode] {
					rels = append(rels, rel)
				}
			}
		}
	}

	return DistinctRels(rels)
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
func FromMap(data map[string]interface{}, labels []string) []*Node {
	nodes := make([]*Node, 0, len(data))

	for _, value := range data {
		if props, ok := value.(map[string]interface{}); ok {
			node := &Node{
				Labels:     labels,
				Properties: props,
			}
			if created, err := Storage.CreateNode(node.Labels, node.Properties); err == nil {
				nodes = append(nodes, created)
			}
		}
	}

	return nodes
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
func Cycles(nodes []*Node) [][]int64 {
	cycles := make([][]int64, 0)

	for _, node := range nodes {
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

			neighbors, err := Storage.GetNodeNeighbors(id, "", storage.DirectionOutgoing)
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

	return cycles
}
