// Package path provides APOC path finding functions.
//
// This package implements all apoc.path.* functions for finding
// and analyzing paths in graph queries.
package path

import "errors"

import (
	"container/list"
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

// Config holds path finding configuration.
type Config struct {
	MinLevel           int
	MaxLevel           int
	RelationshipFilter string
	LabelFilter        string
	Limit              int
	Unique             string
	BFS                bool
}

// SubgraphNodes finds all nodes in a subgraph from a start node.
//
// Example:
//
//	apoc.path.subgraphNodes(startNode, {maxLevel: 3})
func (f *Functions) SubgraphNodes(ctx context.Context, start *Node, config Config) ([]*Node, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	visited := make(map[int64]bool)
	result := make([]*Node, 0)
	queue := list.New()

	queue.PushBack(&pathState{node: start, depth: 0})
	visited[start.ID] = true

	for queue.Len() > 0 {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		element := queue.Front()
		state := element.Value.(*pathState)
		queue.Remove(element)

		if state.depth >= config.MinLevel {
			result = append(result, state.node)
		}

		if state.depth < config.MaxLevel {
			neighbors, err := f.backend.GetNodeNeighbors(ctx, state.node.ID, config.RelationshipFilter, storage.DirectionBoth)
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
						queue.PushBack(&pathState{node: neighbor, depth: state.depth + 1})
					}
				}
			}
		}

		if config.Limit > 0 && len(result) >= config.Limit {
			break
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return result, nil
}

// SubgraphAll returns nodes and relationships in the map keys "nodes" and "rels".
// Cancellation and backend failures return no partial map.
//
// Example:
//
//	apoc.path.subgraphAll(startNode, {maxLevel: 3})
func (f *Functions) SubgraphAll(ctx context.Context, start *Node, config Config) (map[string]interface{}, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	visited := make(map[int64]bool)
	nodes := make([]*Node, 0)
	rels := make([]*Relationship, 0)
	queue := list.New()
	queue.PushBack(&pathState{node: start, depth: 0})
	visited[start.ID] = true
	for queue.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		element := queue.Front()
		state := element.Value.(*pathState)
		queue.Remove(element)
		if state.depth >= config.MinLevel {
			nodes = append(nodes, state.node)
		}
		if state.depth < config.MaxLevel {
			relationships, err := f.backend.GetNodeRelationships(ctx, state.node.ID, config.RelationshipFilter, storage.DirectionBoth)
			if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
				return nil, err
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err != nil {
				continue // Ordinary misses never publish or traverse failed-read data.
			}
			for _, rel := range relationships {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				rels = append(rels, rel)
				neighborID := rel.StartNode
				if rel.StartNode == state.node.ID {
					neighborID = rel.EndNode
				}
				if !visited[neighborID] {
					neighbor, err := f.backend.GetNode(ctx, neighborID)
					if err != nil && !errors.Is(err, storage.ErrNodeNotFound) {
						return nil, err
					}
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if err == nil {
						visited[neighborID] = true
						queue.PushBack(&pathState{node: neighbor, depth: state.depth + 1})
					}
				}
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return map[string]interface{}{"nodes": nodes, "rels": rels}, nil
}

// ExpandConfig expands from a start node with configuration.
//
// Example:
//
//	apoc.path.expandConfig(startNode, {relationshipFilter: 'KNOWS>'})
func (f *Functions) ExpandConfig(ctx context.Context, start *Node, config Config) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	paths := make([]*Path, 0)
	visited := make(map[int64]bool)

	var dfs func(*Node, []*Node, []*Relationship, int)
	dfs = func(node *Node, nodes []*Node, rels []*Relationship, depth int) {
		if depth >= config.MinLevel {
			path := &Path{
				Nodes:         append([]*Node{}, nodes...),
				Relationships: append([]*Relationship{}, rels...),
				Length:        len(rels),
			}
			paths = append(paths, path)
		}

		if depth >= config.MaxLevel {
			return
		}

		if config.Limit > 0 && len(paths) >= config.Limit {
			return
		}

		relationships, err := f.backend.GetNodeRelationships(ctx, node.ID, config.RelationshipFilter, storage.DirectionBoth)
		if err == nil {
			for _, rel := range relationships {
				var neighborID int64
				if rel.StartNode == node.ID {
					neighborID = rel.EndNode
				} else {
					neighborID = rel.StartNode
				}

				if config.Unique == "NODE_GLOBAL" && visited[neighborID] {
					continue
				}

				neighbor, err := f.backend.GetNode(ctx, neighborID)
				if err != nil {
					continue
				}

				visited[neighborID] = true
				newNodes := append(nodes, neighbor)
				newRels := append(rels, rel)
				dfs(neighbor, newNodes, newRels, depth+1)

				if config.Unique == "NODE_GLOBAL" {
					visited[neighborID] = false
				}
			}
		}
	}

	visited[start.ID] = true
	dfs(start, []*Node{start}, []*Relationship{}, 0)

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return paths, nil
}

// SpanningTree finds a spanning tree from a start node.
//
// Example:
//
//	apoc.path.spanningTree(startNode, {maxLevel: 5})
func (f *Functions) SpanningTree(ctx context.Context, start *Node, config Config) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	visited := make(map[int64]bool)
	paths := make([]*Path, 0)

	var bfs func(*Node, []*Node, []*Relationship, int)
	bfs = func(node *Node, nodes []*Node, rels []*Relationship, depth int) {
		if depth > 0 {
			path := &Path{
				Nodes:         append([]*Node{}, nodes...),
				Relationships: append([]*Relationship{}, rels...),
				Length:        len(rels),
			}
			paths = append(paths, path)
		}

		if depth >= config.MaxLevel {
			return
		}

		relationships, err := f.backend.GetNodeRelationships(ctx, node.ID, config.RelationshipFilter, storage.DirectionBoth)
		if err == nil {
			for _, rel := range relationships {
				var neighborID int64
				if rel.StartNode == node.ID {
					neighborID = rel.EndNode
				} else {
					neighborID = rel.StartNode
				}

				if !visited[neighborID] {
					neighbor, err := f.backend.GetNode(ctx, neighborID)
					if err == nil {
						visited[neighborID] = true
						newNodes := append(nodes, neighbor)
						newRels := append(rels, rel)
						bfs(neighbor, newNodes, newRels, depth+1)
					}
				}
			}
		}
	}

	visited[start.ID] = true
	bfs(start, []*Node{start}, []*Relationship{}, 0)

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return paths, nil
}

// ShortestPath finds the shortest path between two nodes.
//
// Example:
//
//	apoc.path.shortestPath(start, end, 'KNOWS>', 10)
func (f *Functions) ShortestPath(ctx context.Context, start, end *Node, relFilter string, maxHops int) (*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	path, err := f.backend.FindShortestPath(ctx, start.ID, end.ID, relFilter, maxHops)
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
		return nil, nil
	}
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return path, nil
}

// AllShortestPaths finds all shortest paths between two nodes.
//
// Example:
//
//	apoc.path.allShortestPaths(start, end, 'KNOWS>', 10)
func (f *Functions) AllShortestPaths(ctx context.Context, start, end *Node, relFilter string, maxHops int) ([]*Path, error) {
	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}

	paths, err := f.backend.FindAllPaths(ctx, start.ID, end.ID, relFilter, maxHops)
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

	// Filter to only shortest paths
	if len(paths) == 0 {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}
		return paths, nil
	}

	minLength := paths[0].Length
	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if path.Length < minLength {
			minLength = path.Length
		}
	}

	shortestPaths := make([]*Path, 0)
	for _, path := range paths {
		if operationError := ctx.Err(); operationError != nil {
			return nil, operationError
		}

		if path.Length == minLength {
			shortestPaths = append(shortestPaths, path)
		}
	}

	if operationError := ctx.Err(); operationError != nil {
		return nil, operationError
	}
	return shortestPaths, nil
}

// Combine combines multiple paths into one.
//
// Example:
//
//	apoc.path.combine(path1, path2)
func Combine(paths ...*Path) *Path {
	if len(paths) == 0 {
		return &Path{}
	}

	combined := &Path{
		Nodes:         make([]*Node, 0),
		Relationships: make([]*Relationship, 0),
	}

	for _, path := range paths {
		combined.Nodes = append(combined.Nodes, path.Nodes...)
		combined.Relationships = append(combined.Relationships, path.Relationships...)
	}

	combined.Length = len(combined.Relationships)
	return combined
}

// Elements returns all elements (nodes and relationships) in a path.
//
// Example:
//
//	apoc.path.elements(path) => [node1, rel1, node2, rel2, ...]
func Elements(path *Path) []interface{} {
	elements := make([]interface{}, 0)

	for i := 0; i < len(path.Nodes); i++ {
		elements = append(elements, path.Nodes[i])
		if i < len(path.Relationships) {
			elements = append(elements, path.Relationships[i])
		}
	}

	return elements
}

// Slice returns a slice of a path.
//
// Example:
//
//	apoc.path.slice(path, 1, 3) => path from index 1 to 3
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

// Helper types and functions

type pathState struct {
	node  *Node
	depth int
}
