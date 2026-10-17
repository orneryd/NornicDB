// Package cypher implements APOC community detection algorithms.
package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/louvain"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/wcc"
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
	nodes, err := e.algorithmNodes("apoc.algo.louvain", args)
	if err != nil {
		return nil, err
	}
	// The config map may name the relationship weight property.
	weightProp, _ := algorithmConfig(args)["weightProperty"].(string)

	// Run Louvain algorithm
	communities := e.computeLouvain(nodes, weightProp)

	// Build result
	rows := algorithmRows(nodes, communities)

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
func (e *StorageExecutor) computeLouvain(nodes []*storage.Node, weightProp string) map[storage.NodeID]int {
	if len(nodes) == 0 {
		return map[storage.NodeID]int{}
	}
	index := make(map[storage.NodeID]int, len(nodes))
	unique := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if _, listed := index[node.ID]; !listed {
			index[node.ID] = len(unique)
			unique = append(unique, node)
		}
	}
	graph := louvain.NewGraph(len(unique))
	for i, node := range unique {
		for _, edge := range e.getNodeEdges(node.ID) {
			// Each relationship once: from its start node.
			if edge.StartNode != node.ID {
				continue
			}
			j, ok := index[edge.EndNode]
			if !ok {
				continue
			}
			weight := 1.0
			if weightProp != "" {
				if w, ok := toFloat64(edge.Properties[weightProp]); ok {
					weight = w
				}
			}
			graph.Link(i, j, weight)
		}
	}

	membership := louvain.Final(louvain.Run(graph, louvain.DefaultOptions()))
	result := make(map[storage.NodeID]int, len(unique))
	for i, node := range unique {
		result[node.ID] = membership[i]
	}
	return result
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
	nodes, err := e.algorithmNodes("apoc.algo.labelPropagation", args)
	if err != nil {
		return nil, err
	}

	communities := e.computeLabelPropagation(nodes)

	rows := algorithmRows(nodes, communities)

	return &ExecuteResult{
		Columns: []string{"node", "community"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) computeLabelPropagation(nodes []*storage.Node) map[storage.NodeID]int {

	if len(nodes) == 0 {
		return map[storage.NodeID]int{}
	}

	// Initialize: each node gets its own label
	labels := make(map[storage.NodeID]int)
	for i, node := range nodes {
		labels[node.ID] = i
	}

	// Iterate until convergence
	maxIterations := 20
	for iter := 0; iter < maxIterations; iter++ {
		changed := false

		for _, node := range nodes {
			// Count neighbor labels
			labelCount := make(map[int]int)
			edges := e.getNodeEdges(node.ID)

			for _, edge := range edges {
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
	return renumberInNodeOrder(nodes, labels)
}

// =============================================================================
// apoc.algo.wcc - Weakly Connected Components
// =============================================================================

// callApocAlgoWCC implements Weakly Connected Components detection.
// Syntax: CALL apoc.algo.wcc(['Label']) YIELD node, componentId
func (e *StorageExecutor) callApocAlgoWCC(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	nodes, err := e.algorithmNodes("apoc.algo.wcc", args)
	if err != nil {
		return nil, err
	}

	components := e.computeWCC(nodes)

	rows := algorithmRows(nodes, components)

	return &ExecuteResult{
		Columns: []string{"node", "componentId"},
		Rows:    rows,
	}, nil
}

// computeWCC is the stored graph's weakly connected components over nodes:
// every stored relationship between two of them, either way, joins them in
// the shared union-find. Components are numbered 0, 1, … by first node in the
// given order; a node listed twice is one node.
func (e *StorageExecutor) computeWCC(nodes []*storage.Node) map[storage.NodeID]int {
	index := make(map[storage.NodeID]int, len(nodes))
	for i, node := range nodes {
		if _, listed := index[node.ID]; !listed {
			index[node.ID] = i
		}
	}
	forest := wcc.New(len(nodes))
	for i, node := range nodes {
		if index[node.ID] != i {
			continue
		}
		for _, edge := range e.getNodeEdges(node.ID) {
			neighbor := edge.EndNode
			if edge.StartNode != node.ID {
				neighbor = edge.StartNode
			}
			if j, listed := index[neighbor]; listed {
				forest.Union(i, j)
			}
		}
	}
	lowest := forest.Lowest()
	components := make(map[storage.NodeID]int, len(index))
	for id, i := range index {
		components[id] = lowest[i]
	}
	return renumberInNodeOrder(nodes, components)
}
