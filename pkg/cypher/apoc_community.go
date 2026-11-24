// Package cypher implements APOC community detection algorithms.
package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: result headers come from procedure_result_columns.go.

import (
	"context"

	"github.com/orneryd/nornicdb/internal/queryprofile"
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
//
// The statement context reaches the adjacency enumeration, both relationship
// passes and the engine's own passes; a cancelled call returns ctx's error and
// no rows, rather than a successful partial partition.
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
	rows := graphAlgorithmRows(nodes, communities)

	return &ExecuteResult{
		Columns: columnNames(nodeCommunityColumns),
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
//
// Each adjacency read, relationship and output entry checks ctx, so a
// cancelled call stops reading adjacency and returns ctx's error instead of a
// partition. A failed adjacency read is returned as the call's error, not read
// as an empty incidence.
func (e *StorageExecutor) computeLouvain(ctx context.Context, nodes []*storage.Node, weightProp string) (map[storage.NodeID]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return map[storage.NodeID]int{}, nil
	}
	index := make(map[storage.NodeID]int, len(nodes))
	unique := make([]*storage.Node, 0, len(nodes))
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, listed := index[node.ID]; !listed {
			index[node.ID] = len(unique)
			unique = append(unique, node)
		}
	}
	var relationships []*storage.Edge
	for _, node := range unique {
		// A cancelled call must not keep enumerating adjacency.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Enumerate physical relationships once. Undirected expansion and
		// degree/weight accounting belong to louvain.RunUndirected;
		// outgoing+incoming incidence lists repeat a physical self-loop.
		edges, err := e.storage.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: node.ID})
		if err != nil {
			return nil, err
		}
		relationships = append(relationships, edges...)
	}
	run, err := louvain.RunUndirected(ctx, len(unique), len(relationships), func(i int) (int, int, float64, bool) {
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
	}, louvain.DefaultOptions())
	if err != nil {
		return nil, err
	}
	membership := louvain.Final(run)
	result := make(map[storage.NodeID]int, len(unique))
	for i, node := range unique {
		if err := ctx.Err(); err != nil {
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	rows := graphAlgorithmRows(nodes, communities)

	return &ExecuteResult{
		Columns: columnNames(nodeCommunityColumns),
		Rows:    rows,
	}, nil
}

// computeLabelPropagation settles each node on the label most of its neighbours
// carry. The caller's context is observed per propagation round, per adjacency
// read and every pipelineMaterializedCancelCheck units of a sweep (an initialized
// label, an incident entry, a counted label or a numbered community), so a
// canceled read retires the remaining rounds and the community numbering instead
// of finishing them.
func (e *StorageExecutor) computeLabelPropagation(ctx context.Context, nodes []*storage.Node) (map[storage.NodeID]int, error) {

	if len(nodes) == 0 {
		return map[storage.NodeID]int{}, nil
	}

	work := algorithmWorkFor(ctx)

	// Initialize: each node gets its own label
	labels := make(map[storage.NodeID]int)
	for i, node := range nodes {
		if err := work.charge(); err != nil {
			return nil, err
		}
		labels[node.ID] = i
	}

	// Iterate until convergence
	maxIterations := 20
	for iter := 0; iter < maxIterations; iter++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		changed := false

		for _, node := range nodes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// Count neighbor labels
			labelCount := make(map[int]int)
			edges := e.getNodeEdges(ctx, node.ID)

			for _, edge := range edges {
				if err := work.charge(); err != nil {
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
				if err := work.charge(); err != nil {
					return nil, err
				}
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

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Communities numbered by their first node in the given order (#1183).
	return renumberInNodeOrder(ctx, nodes, labels)
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	rows := graphAlgorithmRows(nodes, components)

	return &ExecuteResult{
		Columns: columnNames(apocAlgoWccColumns),
		Rows:    rows,
	}, nil
}

// computeWCC groups the given nodes into weakly connected components. The
// complete stored graph can use one edge scan. An induced subgraph can use
// adjacency to avoid scanning unrelated edges. Both honor the caller's context
// during the edge pass and component numbering.
func (e *StorageExecutor) computeWCC(ctx context.Context, nodes []*storage.Node) (map[storage.NodeID]int, error) {

	if len(nodes) == 0 {
		return map[storage.NodeID]int{}, nil
	}

	work := algorithmWorkFor(ctx)

	// Union-Find structure
	parent := make(map[storage.NodeID]storage.NodeID, len(nodes))
	rank := make(map[storage.NodeID]int, len(nodes))

	for _, node := range nodes {
		if err := work.charge(); err != nil {
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

	view, stream, err := e.wccEdgeStream(ctx, len(nodes))
	if err != nil {
		return nil, err
	}
	join := func(edge *storage.Edge) error {
		if err := work.charge(); err != nil {
			return err
		}
		_, hasStart := parent[edge.StartNode]
		_, hasEnd := parent[edge.EndNode]
		if hasStart && hasEnd {
			union(edge.StartNode, edge.EndNode)
		}
		return nil
	}
	if stream != nil {
		visit := queryprofile.EntityVisitor(ctx, profileKindAllRelationshipsScan, "", func(edge *storage.Edge) error {
			return join(view.edge(edge))
		})
		if err := stream.StreamEdgesByType(ctx, "", visit); err != nil {
			return nil, err
		}
	} else {
		for _, node := range nodes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for _, edge := range e.getNodeEdges(ctx, node.ID) {
				if err := join(edge); err != nil {
					return nil, err
				}
			}
		}
	}

	// Assign component IDs
	componentMap := make(map[storage.NodeID]int)
	nextID := 0
	result := make(map[storage.NodeID]int, len(nodes))

	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
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

// wccEdgeStream uses the count and edge stream of the same storage view. The
// existing adjacency route handles relationship-filtered and transaction-local
// views and storage engines without a native scoped stream.
func (e *StorageExecutor) wccEdgeStream(ctx context.Context, selected int) (*graphIdentityStorage, storage.EdgeTypeStreamer, error) {
	view, ok := e.storage.(*graphIdentityStorage)
	if !ok {
		return nil, nil, nil
	}
	underlying := view.Engine
	if scoped, ok := underlying.(*storage.NamespacedEngine); ok {
		underlying = scoped.GetInnerEngine()
	}
	switch underlying.(type) {
	case *storage.BadgerEngine, *storage.WALEngine:
	default:
		return nil, nil, nil
	}
	stream, ok := view.Engine.(storage.EdgeTypeStreamer)
	if !ok {
		return nil, nil, nil
	}
	count, err := view.Engine.NodeCount(ctx)
	if err != nil || int64(selected) != count {
		return nil, nil, err
	}
	return view, stream, nil
}
