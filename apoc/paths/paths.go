// Package paths provides APOC advanced path operations.
//
// This package implements all apoc.paths.* functions for advanced
// path finding and analysis (plural of path package).
package paths

import (
	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Path represents a path through the graph.
type Path = storage.Path

// Storage is the interface for database operations.
var Storage storage.Storage = storage.NewInMemoryStorage()

// All finds all paths between two nodes.
//
// Example:
//
//	apoc.paths.all(start, end, 'KNOWS', 5) => all paths
func All(start, end *Node, relType string, maxLength int) []*Path {
	paths, err := Storage.FindAllPaths(start.ID, end.ID, relType, maxLength)
	if err != nil {
		return []*Path{}
	}
	return paths
}

// Shortest finds shortest paths between two nodes.
//
// Example:
//
//	apoc.paths.shortest(start, end, 'KNOWS', 10) => shortest paths
func Shortest(start, end *Node, relType string, maxLength int) []*Path {
	paths, err := Storage.FindAllPaths(start.ID, end.ID, relType, maxLength)
	if err != nil || len(paths) == 0 {
		return []*Path{}
	}

	// Find minimum length
	minLength := paths[0].Length
	for _, path := range paths {
		if path.Length < minLength {
			minLength = path.Length
		}
	}

	// Filter to shortest paths
	shortest := make([]*Path, 0)
	for _, path := range paths {
		if path.Length == minLength {
			shortest = append(shortest, path)
		}
	}

	return shortest
}

// Longest finds longest paths between two nodes.
//
// Example:
//
//	apoc.paths.longest(start, end, 'KNOWS', 10) => longest paths
func Longest(start, end *Node, relType string, maxLength int) []*Path {
	paths, err := Storage.FindAllPaths(start.ID, end.ID, relType, maxLength)
	if err != nil || len(paths) == 0 {
		return []*Path{}
	}

	// Find maximum length
	maxLen := paths[0].Length
	for _, path := range paths {
		if path.Length > maxLen {
			maxLen = path.Length
		}
	}

	// Filter to longest paths
	longest := make([]*Path, 0)
	for _, path := range paths {
		if path.Length == maxLen {
			longest = append(longest, path)
		}
	}

	return longest
}

// Simple finds simple paths (no repeated nodes).
//
// Example:
//
//	apoc.paths.simple(start, end, 'KNOWS', 10) => simple paths
func Simple(start, end *Node, relType string, maxLength int) []*Path {
	paths := All(start, end, relType, maxLength)
	simple := make([]*Path, 0)

	for _, path := range paths {
		if isSimplePath(path) {
			simple = append(simple, path)
		}
	}

	return simple
}

// isSimplePath checks if a path has no repeated nodes.
func isSimplePath(path *Path) bool {
	seen := make(map[int64]bool)
	for _, node := range path.Nodes {
		if seen[node.ID] {
			return false
		}
		seen[node.ID] = true
	}
	return true
}

// Elementary finds elementary paths (no repeated edges).
//
// Example:
//
//	apoc.paths.elementary(start, end, 'KNOWS', 10) => elementary paths
func Elementary(start, end *Node, relType string, maxLength int) []*Path {
	paths := All(start, end, relType, maxLength)
	elementary := make([]*Path, 0)

	for _, path := range paths {
		if isElementaryPath(path) {
			elementary = append(elementary, path)
		}
	}

	return elementary
}

// isElementaryPath checks if a path has no repeated edges.
func isElementaryPath(path *Path) bool {
	seen := make(map[int64]bool)
	for _, rel := range path.Relationships {
		if seen[rel.ID] {
			return false
		}
		seen[rel.ID] = true
	}
	return true
}

// Disjoint finds node-disjoint paths.
//
// Example:
//
//	apoc.paths.disjoint(start, end, 'KNOWS', 10, 3) => disjoint paths
func Disjoint(start, end *Node, relType string, maxLength, count int) []*Path {
	allPaths := All(start, end, relType, maxLength)
	disjoint := make([]*Path, 0)
	usedNodes := make(map[int64]bool)

	// Mark start and end as always usable
	usedNodes[start.ID] = false
	usedNodes[end.ID] = false

	for _, path := range allPaths {
		if len(disjoint) >= count {
			break
		}

		// Check if path uses any already-used nodes (except start/end)
		canUse := true
		for _, node := range path.Nodes {
			if node.ID != start.ID && node.ID != end.ID && usedNodes[node.ID] {
				canUse = false
				break
			}
		}

		if canUse {
			disjoint = append(disjoint, path)
			// Mark all nodes in this path as used
			for _, node := range path.Nodes {
				usedNodes[node.ID] = true
			}
		}
	}

	return disjoint
}

// EdgeDisjoint finds edge-disjoint paths.
//
// Example:
//
//	apoc.paths.edgeDisjoint(start, end, 'KNOWS', 10, 3) => edge-disjoint paths
func EdgeDisjoint(start, end *Node, relType string, maxLength, count int) []*Path {
	allPaths := All(start, end, relType, maxLength)
	edgeDisjoint := make([]*Path, 0)
	usedEdges := make(map[int64]bool)

	for _, path := range allPaths {
		if len(edgeDisjoint) >= count {
			break
		}

		// Check if path uses any already-used edges
		canUse := true
		for _, rel := range path.Relationships {
			if usedEdges[rel.ID] {
				canUse = false
				break
			}
		}

		if canUse {
			edgeDisjoint = append(edgeDisjoint, path)
			// Mark all edges in this path as used
			for _, rel := range path.Relationships {
				usedEdges[rel.ID] = true
			}
		}
	}

	return edgeDisjoint
}

// Cycles finds all cycles starting from a node.
//
// Example:
//
//	apoc.paths.cycles(start, 'KNOWS', 10) => cycles
func Cycles(start *Node, relType string, maxLength int) []*Path {
	// Find paths that start and end at the same node
	return All(start, start, relType, maxLength)
}

// Hamiltonian finds Hamiltonian paths (visiting all nodes once).
//
// Example:
//
//	apoc.paths.hamiltonian(nodes, start, end) => Hamiltonian paths
func Hamiltonian(nodes []*Node, start, end *Node) []*Path {
	if len(nodes) == 0 {
		return []*Path{}
	}

	targetCount := len(nodes)
	paths := All(start, end, "", targetCount)
	hamiltonian := make([]*Path, 0)

	for _, path := range paths {
		if len(path.Nodes) == targetCount && isSimplePath(path) {
			hamiltonian = append(hamiltonian, path)
		}
	}

	return hamiltonian
}

// Eulerian finds Eulerian paths (visiting all edges once).
//
// Example:
//
//	apoc.paths.eulerian(start, end) => Eulerian paths
func Eulerian(start, end *Node) []*Path {
	// Placeholder - would implement Eulerian path algorithm
	return []*Path{}
}

// KShortest finds k shortest paths.
//
// Example:
//
//	apoc.paths.kShortest(start, end, 'KNOWS', 10, 5) => 5 shortest paths
func KShortest(start, end *Node, relType string, maxLength, k int) []*Path {
	allPaths := All(start, end, relType, maxLength)

	// Sort by length
	for i := 0; i < len(allPaths); i++ {
		for j := i + 1; j < len(allPaths); j++ {
			if allPaths[i].Length > allPaths[j].Length {
				allPaths[i], allPaths[j] = allPaths[j], allPaths[i]
			}
		}
	}

	if len(allPaths) > k {
		return allPaths[:k]
	}
	return allPaths
}

// WithLength finds paths of specific length.
//
// Example:
//
//	apoc.paths.withLength(start, end, 'KNOWS', 3) => paths of length 3
func WithLength(start, end *Node, relType string, length int) []*Path {
	paths := All(start, end, relType, length)
	result := make([]*Path, 0)

	for _, path := range paths {
		if path.Length == length {
			result = append(result, path)
		}
	}

	return result
}

// WithinLength finds paths within length range.
//
// Example:
//
//	apoc.paths.withinLength(start, end, 'KNOWS', 2, 5) => paths length 2-5
func WithinLength(start, end *Node, relType string, minLength, maxLength int) []*Path {
	paths := All(start, end, relType, maxLength)
	result := make([]*Path, 0)

	for _, path := range paths {
		if path.Length >= minLength && path.Length <= maxLength {
			result = append(result, path)
		}
	}

	return result
}

// Count counts paths between nodes.
//
// Example:
//
//	apoc.paths.count(start, end, 'KNOWS', 10) => count
func Count(start, end *Node, relType string, maxLength int) int {
	paths := All(start, end, relType, maxLength)
	return len(paths)
}

// Exists checks if any path exists.
//
// Example:
//
//	apoc.paths.exists(start, end, 'KNOWS', 10) => true/false
func Exists(start, end *Node, relType string, maxLength int) bool {
	path, err := Storage.FindShortestPath(start.ID, end.ID, relType, maxLength)
	return err == nil && path != nil
}

// Distance calculates path distance (length).
//
// Example:
//
//	apoc.paths.distance(start, end, 'KNOWS') => distance
func Distance(start, end *Node, relType string) int {
	path, err := Storage.FindShortestPath(start.ID, end.ID, relType, 100)
	if err != nil || path == nil {
		return -1
	}
	return path.Length
}

// Common finds common nodes in multiple paths.
//
// Example:
//
//	apoc.paths.common([path1, path2, path3]) => common nodes
func Common(paths []*Path) []*Node {
	if len(paths) == 0 {
		return []*Node{}
	}

	// Count node occurrences
	counts := make(map[int64]int)
	nodeMap := make(map[int64]*Node)

	for _, path := range paths {
		seen := make(map[int64]bool)
		for _, node := range path.Nodes {
			if !seen[node.ID] {
				counts[node.ID]++
				nodeMap[node.ID] = node
				seen[node.ID] = true
			}
		}
	}

	// Find nodes in all paths
	common := make([]*Node, 0)
	for id, count := range counts {
		if count == len(paths) {
			common = append(common, nodeMap[id])
		}
	}

	return common
}

// Unique finds unique nodes across paths.
//
// Example:
//
//	apoc.paths.unique([path1, path2]) => all unique nodes
func Unique(paths []*Path) []*Node {
	seen := make(map[int64]*Node)

	for _, path := range paths {
		for _, node := range path.Nodes {
			seen[node.ID] = node
		}
	}

	result := make([]*Node, 0, len(seen))
	for _, node := range seen {
		result = append(result, node)
	}

	return result
}

// Merge merges multiple paths into one.
//
// Example:
//
//	apoc.paths.merge([path1, path2]) => merged path
func Merge(paths []*Path) *Path {
	if len(paths) == 0 {
		return &Path{}
	}

	merged := &Path{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}

	for _, path := range paths {
		merged.Nodes = append(merged.Nodes, path.Nodes...)
		merged.Relationships = append(merged.Relationships, path.Relationships...)
	}

	merged.Length = len(merged.Relationships)
	return merged
}

// Reverse reverses a path.
//
// Example:
//
//	apoc.paths.reverse(path) => reversed path
func Reverse(path *Path) *Path {
	reversed := &Path{
		Nodes:         make([]*Node, len(path.Nodes)),
		Relationships: make([]*Relationship, len(path.Relationships)),
		Length:        path.Length,
	}

	// Reverse nodes
	for i, j := 0, len(path.Nodes)-1; i < len(path.Nodes); i, j = i+1, j-1 {
		reversed.Nodes[i] = path.Nodes[j]
	}

	// Reverse relationships
	for i, j := 0, len(path.Relationships)-1; i < len(path.Relationships); i, j = i+1, j-1 {
		reversed.Relationships[i] = path.Relationships[j]
	}

	return reversed
}

// Slice extracts a subpath.
//
// Example:
//
//	apoc.paths.slice(path, 1, 3) => subpath
func Slice(path *Path, start, end int) *Path {
	if start < 0 {
		start = 0
	}
	if end > len(path.Nodes) {
		end = len(path.Nodes)
	}
	if start >= end {
		return &Path{}
	}

	sliced := &Path{
		Nodes:         path.Nodes[start:end],
		Relationships: make([]*Relationship, 0),
	}

	if start < len(path.Relationships) {
		relEnd := end - 1
		if relEnd > len(path.Relationships) {
			relEnd = len(path.Relationships)
		}
		sliced.Relationships = path.Relationships[start:relEnd]
	}

	sliced.Length = len(sliced.Relationships)
	return sliced
}
