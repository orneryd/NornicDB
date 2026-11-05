// Package neighbors provides APOC neighbor traversal functions.
//
// This package implements all apoc.neighbors.* functions for finding
// and analyzing node neighbors in graph queries.
package neighbors

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

// AtHop returns neighbors at a specific hop distance.
//
// Example:
//
//	apoc.neighbors.atHop(node, 'KNOWS', 2) => nodes at distance 2
func (f *Functions) AtHop(ctx context.Context, node *Node, relType string, hops int) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	if hops == 0 {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return []*Node{node}, nil
	}

	visited := make(map[int64]bool)
	current := []*Node{node}
	visited[node.ID] = true

	for i := 0; i < hops; i++ {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		next := make([]*Node, 0)
		for _, n := range current {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			neighbors, err := f.backend.GetNodeNeighbors(ctx, n.ID, relType, storage.DirectionBoth)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return nil, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if err == nil {
				for _, neighbor := range neighbors {
					if operationError := ctx.Err(); operationError != nil {
						return nil, operationError
					}

					if !visited[neighbor.ID] {
						visited[neighbor.ID] = true
						next = append(next, neighbor)
					}
				}
			}
		}
		current = next
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return current, nil
}

// ToHop returns all neighbors up to a specific hop distance.
//
// Example:
//
//	apoc.neighbors.toHop(node, 'KNOWS', 3) => all nodes within 3 hops
func (f *Functions) ToHop(ctx context.Context, node *Node, relType string, maxHops int) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	visited := make(map[int64]bool)
	result := make([]*Node, 0)
	queue := []*Node{node}
	visited[node.ID] = true
	hops := 0

	for len(queue) > 0 && hops < maxHops {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		levelSize := len(queue)
		for i := 0; i < levelSize; i++ {
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			current := queue[0]
			queue = queue[1:]
			result = append(result, current)

			neighbors, err := f.backend.GetNodeNeighbors(ctx, current.ID, relType, storage.DirectionBoth)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return nil, err
			}
			if operationError := ctx.Err(); operationError != nil {
				return nil, operationError
			}

			if err == nil {
				for _, neighbor := range neighbors {
					if operationError := ctx.Err(); operationError != nil {
						return nil, operationError
					}

					if !visited[neighbor.ID] {
						visited[neighbor.ID] = true
						queue = append(queue, neighbor)
					}
				}
			}
		}
		hops++
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return result, nil
}

// DFS performs depth-first search from a node.
//
// Example:
//
//	apoc.neighbors.dfs(node, 'KNOWS', 5) => nodes in DFS order
func (f *Functions) DFS(ctx context.Context, node *Node, relType string, maxDepth int) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	visited := make(map[int64]bool)
	result := make([]*Node, 0)

	var dfs func(*Node, int)
	dfs = func(n *Node, depth int) {
		if depth > maxDepth || visited[n.ID] {
			return
		}

		visited[n.ID] = true
		result = append(result, n)

		neighbors, err := f.backend.GetNodeNeighbors(ctx, n.ID, relType, storage.DirectionBoth)
		if err == nil {
			for _, neighbor := range neighbors {
				dfs(neighbor, depth+1)
			}
		}
	}

	dfs(node, 0)
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	return result, nil
}

// Count counts neighbors at a specific distance.
//
// Example:
//
//	apoc.neighbors.count(node, 'KNOWS', 2) => count
func (f *Functions) Count(ctx context.Context, node *Node, relType string, hops int) (int, error) {
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}

	helperValue1, helperError1 := f.AtHop(ctx, node, relType, hops)
	if helperError1 != nil {
		return 0, helperError1
	}
	neighbors := helperValue1
	if operationError := ctx.Err(); operationError != nil {
		return 0, operationError
	}
	return len(neighbors), nil
}

// Exists checks if neighbors exist at a specific distance.
//
// Example:
//
//	apoc.neighbors.exists(node, 'KNOWS', 2) => true/false
func (f *Functions) Exists(ctx context.Context, node *Node, relType string, hops int) (bool, error) {
	if operationError := ctx.Err(); operationError != nil {
		return false, operationError
	}

	helperValue2, helperError2 := f.Count(ctx, node, relType, hops)
	if helperError2 != nil {
		return false, helperError2
	}
	return helperValue2 > 0, nil
}
