// Package cypher implements APOC community detection algorithms.
package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/orneryd/nornicdb/pkg/louvain"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// apoc.algo.louvain - Louvain Community Detection
// =============================================================================

// callApocAlgoLouvain implements the Louvain community detection algorithm.
// Syntax: CALL apoc.algo.louvain(['Label'], {write: false, weightProperty: 'weight'}) YIELD node, community
//
// The Louvain method is a greedy optimization method for community detection that
// maximizes modularity. It works in two phases:
// 1. Local optimization: Each node is moved to the community that yields the best modularity gain
// 2. Network aggregation: Communities are aggregated into super-nodes
// These phases repeat until no further improvement is possible.
func (e *StorageExecutor) callApocAlgoLouvain(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph, nodes, config, err := e.readAlgorithmGraph(ctx, "apoc.algo.louvain", args)
	if err != nil {
		return nil, err
	}
	// The config map may name the relationship weight property.
	weightProp, _ := config["weightProperty"].(string)

	// Run Louvain algorithm
	communities, err := graph.computeLouvain(ctx, nodes, weightProp)
	if err != nil {
		return nil, err
	}

	// Build result
	rows := algorithmRows(ctx, nodes, communities)

	return &ExecuteResult{
		Columns: []string{"node", "community"},
		Rows:    rows,
	}, nil
}

// computeLouvain is Louvain community detection (#1184), as GDS's
// gds.louvain computes it over an undirected graph: the relationships
// between the given nodes, preserving parallel arcs (weightProp's
// value, 1 without it). Each level moves nodes, in the given order, to the
// neighbouring community with the best modularity gain until no move
// helps, then merges each community into one node; levels repeat until a
// level moves nothing (louvain.Run, louvain.DefaultOptions). Communities are
// numbered by their first node.
// The run observes the statement (louvain's Stop), and a canceled one
// returns the statement's error.
func (e *StorageExecutor) computeLouvain(ctx context.Context, nodes []*storage.Node, weightProp string) (map[storage.NodeID]int, error) {
	if len(nodes) == 0 {
		return map[storage.NodeID]int{}, nil
	}
	work := bulk.NewCheckpoint(ctx)
	index := make(map[storage.NodeID]int, len(nodes))
	unique := make([]*storage.Node, 0, len(nodes))
	for _, node := range observedItems(&work, nodes) {
		if _, listed := index[node.ID]; !listed {
			index[node.ID] = len(unique)
			unique = append(unique, node)
		}
	}
	var relationships []*storage.Edge
	for _, node := range observedItems(&work, unique) {
		// Enumerate physical relationships once. Undirected expansion and
		// degree/weight accounting belong to louvain.RunUndirected;
		// outgoing+incoming incidence lists repeat a physical self-loop.
		edges, err := adjacentEdges(&work, e.storage, node.ID, "outgoing")
		if err != nil {
			edges = func(func(*storage.Edge) bool) {}
		}
		for edge := range edges {
			relationships = append(relationships, edge)
		}
	}
	options := louvain.DefaultOptions()
	options.Stop = func() bool { return ctx.Err() != nil }
	run := louvain.RunUndirected(len(unique), len(relationships), func(i int) (int, int, float64, bool) {
		edge := relationships[i]
		source, sourceFound := index[edge.StartNode]
		target, targetFound := index[edge.EndNode]
		weight := 1.0
		if weightProp != "" {
			if w, ok := toFloat64(edge.Properties[weightProp]); ok {
				weight = w
			}
		}
		return source, target, weight, sourceFound && targetFound
	}, options)
	if err := ctx.Err(); err != nil {
		recordExpressionFailure(ctx, err)
		return nil, err
	}
	membership := louvain.Final(run)
	result := make(map[storage.NodeID]int, len(unique))
	pass := bulk.NewCheckpoint(ctx)
	for i, node := range unique {
		if err := pass.Observe(); err != nil {
			return nil, err
		}
		result[node.ID] = membership[i]
	}
	return result, nil
}

// =============================================================================
// apoc.algo.labelPropagation - Label Propagation Community Detection
// =============================================================================

// callApocAlgoLabelPropagation implements label propagation community detection.
// Syntax: CALL apoc.algo.labelPropagation(['Label']) YIELD node, community
//
// Label propagation is a simpler community detection algorithm where each node
// adopts the label that most of its neighbors have.
func (e *StorageExecutor) callApocAlgoLabelPropagation(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph, nodes, _, err := e.readAlgorithmGraph(ctx, "apoc.algo.labelPropagation", args)
	if err != nil {
		return nil, err
	}

	communities, err := graph.computeLabelPropagation(ctx, nodes)
	if err != nil {
		return nil, err
	}

	rows := algorithmRows(ctx, nodes, communities)

	return &ExecuteResult{
		Columns: []string{"node", "community"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) computeLabelPropagation(ctx context.Context, nodes []*storage.Node) (map[storage.NodeID]int, error) {

	if len(nodes) == 0 {
		return map[storage.NodeID]int{}, nil
	}
	work := bulk.NewCheckpoint(ctx)

	// Initialize: each node gets its own label
	labels := make(map[storage.NodeID]int)
	pass2 := bulk.NewCheckpoint(ctx)
	for i, node := range nodes {
		if err := pass2.Observe(); err != nil {
			return nil, err
		}
		labels[node.ID] = i
	}

	// Iterate until convergence
	maxIterations := 20
	for iter := 0; iter < maxIterations; iter++ {
		changed := false

		pass4 := bulk.NewCheckpoint(ctx)
		for _, node := range nodes {
			if err := pass4.Observe(); err != nil {
				return nil, err
			}
			// Count neighbor labels
			labelCount := make(map[int]int)
			for edge := range e.nodeEdges(&work, node.ID) {
				if err := work.Observe(); err != nil {
					return nil, err
				}
				neighbor := edge.EndNode
				if edge.StartNode != node.ID {
					neighbor = edge.StartNode
				}
				if neighborLabel, ok := labels[neighbor]; ok {
					labelCount[neighborLabel]++
				}
			}

			if len(labelCount) == 0 {
				continue
			}

			// Find most common label
			maxCount := 0
			bestLabel := labels[node.ID]
			for l, count := range labelCount {
				if count > maxCount || (count == maxCount && l < bestLabel) {
					maxCount = count
					bestLabel = l
				}
			}

			if bestLabel != labels[node.ID] {
				labels[node.ID] = bestLabel
				changed = true
			}
		}

		if !changed {
			break
		}
	}

	// Communities numbered by their first node in the given order (#1183).
	return renumberInNodeOrder(nodes, labels), nil
}

// =============================================================================
// apoc.algo.wcc - Weakly Connected Components
// =============================================================================

// callApocAlgoWCC implements Weakly Connected Components detection.
// Syntax: CALL apoc.algo.wcc(['Label']) YIELD node, componentId
func (e *StorageExecutor) callApocAlgoWCC(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	graph, nodes, _, err := e.readAlgorithmGraph(ctx, "apoc.algo.wcc", args)
	if err != nil {
		return nil, err
	}

	components, err := graph.computeWCC(ctx, nodes)
	if err != nil {
		return nil, err
	}

	rows := algorithmRows(ctx, nodes, components)

	return &ExecuteResult{
		Columns: []string{"node", "componentId"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) computeWCC(ctx context.Context, nodes []*storage.Node) (map[storage.NodeID]int, error) {

	if len(nodes) == 0 {
		return map[storage.NodeID]int{}, nil
	}
	work := bulk.NewCheckpoint(ctx)

	// Union-Find structure
	parent := make(map[storage.NodeID]storage.NodeID)
	rank := make(map[storage.NodeID]int)

	pass2 := bulk.NewCheckpoint(ctx)
	for _, node := range nodes {
		if err := pass2.Observe(); err != nil {
			return nil, err
		}
		parent[node.ID] = node.ID
		rank[node.ID] = 0
	}

	// Find with path compression
	var find func(x storage.NodeID) storage.NodeID
	find = func(x storage.NodeID) storage.NodeID {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}

	// Union by rank
	union := func(x, y storage.NodeID) {
		px, py := find(x), find(y)
		if px == py {
			return
		}
		if rank[px] < rank[py] {
			parent[px] = py
		} else if rank[px] > rank[py] {
			parent[py] = px
		} else {
			parent[py] = px
			rank[px]++
		}
	}

	// Process all edges
	pass4 := bulk.NewCheckpoint(ctx)
	for _, node := range nodes {
		if err := pass4.Observe(); err != nil {
			return nil, err
		}
		for edge := range e.nodeEdges(&work, node.ID) {
			if err := work.Observe(); err != nil {
				return nil, err
			}
			neighbor := edge.EndNode
			if edge.StartNode != node.ID {
				neighbor = edge.StartNode
			}
			if _, exists := parent[neighbor]; exists {
				union(node.ID, neighbor)
			}
		}
	}

	// Assign component IDs
	componentMap := make(map[storage.NodeID]int)
	nextID := 0
	result := make(map[storage.NodeID]int)

	pass6 := bulk.NewCheckpoint(ctx)
	for _, node := range nodes {
		if err := pass6.Observe(); err != nil {
			return nil, err
		}
		root := find(node.ID)
		if id, exists := componentMap[root]; exists {
			result[node.ID] = id
		} else {
			componentMap[root] = nextID
			result[node.ID] = nextID
			nextID++
		}
	}

	return result, nil
}
