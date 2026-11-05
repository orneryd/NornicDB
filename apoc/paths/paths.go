// Package paths provides APOC advanced path operations.
//
// This package implements all apoc.paths.* functions for advanced
// path finding and analysis (plural of path package).
package paths

import "errors"

import (
	"context"
	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Path represents a path through the graph.
type Path = storage.Path

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

// All finds all paths between two nodes.
//
// Example:
//
//	apoc.paths.all(start, end, 'KNOWS', 5) => all paths
func (f *Functions) All(ctx context.Context, start, end *Node, relType string, maxLength int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	paths, err := f.backend.FindAllPaths(ctx, start.ID, end.ID, relType, maxLength)
	if err != nil && !errors.Is(err, storage.ErrPathNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Path{}, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return paths, nil
}

// Shortest finds shortest paths between two nodes.
//
// Example:
//
//	apoc.paths.shortest(start, end, 'KNOWS', 10) => shortest paths
func (f *Functions) Shortest(ctx context.Context, start, end *Node, relType string, maxLength int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	paths, err := f.backend.FindAllPaths(ctx, start.ID, end.ID, relType, maxLength)
	if err != nil && !errors.Is(err, storage.ErrPathNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil || len(paths) == 0 {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Path{}, nil
	}

	// Find minimum length
	minLength := paths[0].Length
	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if path.Length < minLength {
			minLength = path.Length
		}
	}

	// Filter to shortest paths
	shortest := make([]*Path, 0)
	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if path.Length == minLength {
			shortest = append(shortest, path)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return shortest, nil
}

// Longest finds longest paths between two nodes.
//
// Example:
//
//	apoc.paths.longest(start, end, 'KNOWS', 10) => longest paths
func (f *Functions) Longest(ctx context.Context, start, end *Node, relType string, maxLength int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	paths, err := f.backend.FindAllPaths(ctx, start.ID, end.ID, relType, maxLength)
	if err != nil && !errors.Is(err, storage.ErrPathNotFound) {
		return nil, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if err != nil || len(paths) == 0 {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Path{}, nil
	}

	// Find maximum length
	maxLen := paths[0].Length
	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if path.Length > maxLen {
			maxLen = path.Length
		}
	}

	// Filter to longest paths
	longest := make([]*Path, 0)
	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if path.Length == maxLen {
			longest = append(longest, path)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return longest, nil
}

// Simple finds simple paths (no repeated nodes).
//
// Example:
//
//	apoc.paths.simple(start, end, 'KNOWS', 10) => simple paths
func (f *Functions) Simple(ctx context.Context, start, end *Node, relType string, maxLength int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	helperValue1, helperError1 := f.All(ctx, start, end, relType, maxLength)
	if helperError1 != nil {
		return nil, helperError1
	}
	paths := helperValue1
	simple := make([]*Path, 0)

	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if isSimplePath(path) {
			simple = append(simple, path)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return simple, nil
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
func (f *Functions) Elementary(ctx context.Context, start, end *Node, relType string, maxLength int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	helperValue2, helperError2 := f.All(ctx, start, end, relType, maxLength)
	if helperError2 != nil {
		return nil, helperError2
	}
	paths := helperValue2
	elementary := make([]*Path, 0)

	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if isElementaryPath(path) {
			elementary = append(elementary, path)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return elementary, nil
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
func (f *Functions) Disjoint(ctx context.Context, start, end *Node, relType string, maxLength, count int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	helperValue3, helperError3 := f.All(ctx, start, end, relType, maxLength)
	if helperError3 != nil {
		return nil, helperError3
	}
	allPaths := helperValue3
	disjoint := make([]*Path, 0)
	usedNodes := make(map[int64]bool)

	// Mark start and end as always usable
	usedNodes[start.ID] = false
	usedNodes[end.ID] = false

	for _, path := range allPaths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if len(disjoint) >= count {
			break
		}

		// Check if path uses any already-used nodes (except start/end)
		canUse := true
		for _, node := range path.Nodes {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if node.ID != start.ID && node.ID != end.ID && usedNodes[node.ID] {
				canUse = false
				break
			}
		}

		if canUse {
			disjoint = append(disjoint, path)
			// Mark all nodes in this path as used
			for _, node := range path.Nodes {
				if operationError := ctx.Err(); operationError != nil {
					return nil, operationError
				}

				usedNodes[node.ID] = true
			}
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return disjoint, nil
}

// EdgeDisjoint finds edge-disjoint paths.
//
// Example:
//
//	apoc.paths.edgeDisjoint(start, end, 'KNOWS', 10, 3) => edge-disjoint paths
func (f *Functions) EdgeDisjoint(ctx context.Context, start, end *Node, relType string, maxLength, count int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	helperValue4, helperError4 := f.All(ctx, start, end, relType, maxLength)
	if helperError4 != nil {
		return nil, helperError4
	}
	allPaths := helperValue4
	edgeDisjoint := make([]*Path, 0)
	usedEdges := make(map[int64]bool)

	for _, path := range allPaths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if len(edgeDisjoint) >= count {
			break
		}

		// Check if path uses any already-used edges
		canUse := true
		for _, rel := range path.Relationships {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if usedEdges[rel.ID] {
				canUse = false
				break
			}
		}

		if canUse {
			edgeDisjoint = append(edgeDisjoint, path)
			// Mark all edges in this path as used
			for _, rel := range path.Relationships {
				if operationError := ctx.Err(); operationError != nil {
					return nil, operationError
				}

				usedEdges[rel.ID] = true
			}
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return edgeDisjoint, nil
}

// Cycles finds all cycles starting from a node.
//
// Example:
//
//	apoc.paths.cycles(start, 'KNOWS', 10) => cycles
func (f *Functions) Cycles(ctx context.Context, start *Node, relType string, maxLength int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	// Find paths that start and end at the same node

	return f.All(ctx, start, start, relType, maxLength)
}

// Hamiltonian finds Hamiltonian paths (visiting all nodes once).
//
// Example:
//
//	apoc.paths.hamiltonian(nodes, start, end) => Hamiltonian paths
func (f *Functions) Hamiltonian(ctx context.Context, nodes []*Node, start, end *Node) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if len(nodes) == 0 {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Path{}, nil
	}

	targetCount := len(nodes)
	helperValue5, helperError5 := f.All(ctx, start, end, "", targetCount)
	if helperError5 != nil {
		return nil, helperError5
	}
	paths := helperValue5
	hamiltonian := make([]*Path, 0)

	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if len(path.Nodes) == targetCount && isSimplePath(path) {
			hamiltonian = append(hamiltonian, path)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return hamiltonian, nil
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
func (f *Functions) KShortest(ctx context.Context, start, end *Node, relType string, maxLength, k int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	helperValue6, helperError6 := f.All(ctx, start, end, relType, maxLength)
	if helperError6 != nil {
		return nil, helperError6
	}
	allPaths := helperValue6

	// Sort by length
	for i := 0; i < len(allPaths); i++ {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		for j := i + 1; j < len(allPaths); j++ {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if allPaths[i].Length > allPaths[j].Length {
				allPaths[i], allPaths[j] = allPaths[j], allPaths[i]
			}
		}
	}

	if len(allPaths) > k {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return allPaths[:k], nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return allPaths, nil
}

// WithLength finds paths of specific length.
//
// Example:
//
//	apoc.paths.withLength(start, end, 'KNOWS', 3) => paths of length 3
func (f *Functions) WithLength(ctx context.Context, start, end *Node, relType string, length int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	helperValue7, helperError7 := f.All(ctx, start, end, relType, length)
	if helperError7 != nil {
		return nil, helperError7
	}
	paths := helperValue7
	result := make([]*Path, 0)

	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if path.Length == length {
			result = append(result, path)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return result, nil
}

// WithinLength finds paths within length range.
//
// Example:
//
//	apoc.paths.withinLength(start, end, 'KNOWS', 2, 5) => paths length 2-5
func (f *Functions) WithinLength(ctx context.Context, start, end *Node, relType string, minLength, maxLength int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	helperValue8, helperError8 := f.All(ctx, start, end, relType, maxLength)
	if helperError8 != nil {
		return nil, helperError8
	}
	paths := helperValue8
	result := make([]*Path, 0)

	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if path.Length >= minLength && path.Length <= maxLength {
			result = append(result, path)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return result, nil
}

// Count counts paths between nodes.
//
// Example:
//
//	apoc.paths.count(start, end, 'KNOWS', 10) => count
func (f *Functions) Count(ctx context.Context, start, end *Node, relType string, maxLength int) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	helperValue9, helperError9 := f.All(ctx, start, end, relType, maxLength)
	if helperError9 != nil {
		return 0, helperError9
	}
	paths := helperValue9
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return len(paths), nil
}

// Exists checks if any path exists.
//
// Example:
//
//	apoc.paths.exists(start, end, 'KNOWS', 10) => true/false
func (f *Functions) Exists(ctx context.Context, start, end *Node, relType string, maxLength int) (bool, error) {
	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}

	path, err := f.backend.FindShortestPath(ctx, start.ID, end.ID, relType, maxLength)
	if err != nil && !errors.Is(err, storage.ErrPathNotFound) {
		return false, err
	}

	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}
	return err == nil && path != nil, nil
}

// Distance calculates path distance (length).
//
// Example:
//
//	apoc.paths.distance(start, end, 'KNOWS') => distance
func (f *Functions) Distance(ctx context.Context, start, end *Node, relType string) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	path, err := f.backend.FindShortestPath(ctx, start.ID, end.ID, relType, 100)
	if err != nil && !errors.Is(err, storage.ErrPathNotFound) {
		return 0, err
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	if err != nil || path == nil {
		if operationError := ctx.Err(); operationError != nil {
			return 0, operationError
		}
		return -1, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return path.Length, nil
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
