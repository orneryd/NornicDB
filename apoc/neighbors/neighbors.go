// Package neighbors provides APOC neighbor traversal functions.
//
// This package implements all apoc.neighbors.* functions for finding
// and analyzing node neighbors in graph queries.
package neighbors

import (
	"github.com/orneryd/nornicdb/apoc/storage"
)

// Node represents a graph node.
type Node = storage.Node

// Relationship represents a graph relationship.
type Relationship = storage.Relationship

// Storage is the interface for database operations.
var Storage storage.Storage = storage.NewInMemoryStorage()

// AtHop returns neighbors at a specific hop distance.
//
// Example:
//
//	apoc.neighbors.atHop(node, 'KNOWS', 2) => nodes at distance 2
func AtHop(node *Node, relType string, hops int) []*Node {
	if hops == 0 {
		return []*Node{node}
	}

	visited := make(map[int64]bool)
	current := []*Node{node}
	visited[node.ID] = true

	for i := 0; i < hops; i++ {
		next := make([]*Node, 0)
		for _, n := range current {
			neighbors, err := Storage.GetNodeNeighbors(n.ID, relType, storage.DirectionBoth)
			if err == nil {
				for _, neighbor := range neighbors {
					if !visited[neighbor.ID] {
						visited[neighbor.ID] = true
						next = append(next, neighbor)
					}
				}
			}
		}
		current = next
	}

	return current
}

// ToHop returns all neighbors up to a specific hop distance.
//
// Example:
//
//	apoc.neighbors.toHop(node, 'KNOWS', 3) => all nodes within 3 hops
func ToHop(node *Node, relType string, maxHops int) []*Node {
	visited := make(map[int64]bool)
	result := make([]*Node, 0)
	queue := []*Node{node}
	visited[node.ID] = true
	hops := 0

	for len(queue) > 0 && hops < maxHops {
		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			current := queue[0]
			queue = queue[1:]
			result = append(result, current)

			neighbors, err := Storage.GetNodeNeighbors(current.ID, relType, storage.DirectionBoth)
			if err == nil {
				for _, neighbor := range neighbors {
					if !visited[neighbor.ID] {
						visited[neighbor.ID] = true
						queue = append(queue, neighbor)
					}
				}
			}
		}
		hops++
	}

	return result
}

// BFS performs breadth-first search from a node.
//
// Example:
//
//	apoc.neighbors.bfs(node, 'KNOWS', 5) => nodes in BFS order
func BFS(node *Node, relType string, maxDepth int) []*Node {
	return ToHop(node, relType, maxDepth)
}

// DFS performs depth-first search from a node.
//
// Example:
//
//	apoc.neighbors.dfs(node, 'KNOWS', 5) => nodes in DFS order
func DFS(node *Node, relType string, maxDepth int) []*Node {
	visited := make(map[int64]bool)
	result := make([]*Node, 0)

	var dfs func(*Node, int)
	dfs = func(n *Node, depth int) {
		if depth > maxDepth || visited[n.ID] {
			return
		}

		visited[n.ID] = true
		result = append(result, n)

		neighbors, err := Storage.GetNodeNeighbors(n.ID, relType, storage.DirectionBoth)
		if err == nil {
			for _, neighbor := range neighbors {
				dfs(neighbor, depth+1)
			}
		}
	}

	dfs(node, 0)
	return result
}

// Count counts neighbors at a specific distance.
//
// Example:
//
//	apoc.neighbors.count(node, 'KNOWS', 2) => count
func Count(node *Node, relType string, hops int) int {
	neighbors := AtHop(node, relType, hops)
	return len(neighbors)
}

// Exists checks if neighbors exist at a specific distance.
//
// Example:
//
//	apoc.neighbors.exists(node, 'KNOWS', 2) => true/false
func Exists(node *Node, relType string, hops int) bool {
	return Count(node, relType, hops) > 0
}
