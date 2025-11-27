// Package cypher implements APOC community detection algorithms.
package cypher

import (
	"context"
	"strings"

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
func (e *StorageExecutor) callApocAlgoLouvain(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse optional label filter
	label := e.extractLabelFromAlgoCall(cypher, "LOUVAIN")

	// Parse config
	weightProp := ""
	if strings.Contains(cypher, "weightProperty") {
		// Extract weight property name (simplified parsing)
		if idx := strings.Index(cypher, "weightProperty"); idx > 0 {
			remainder := cypher[idx:]
			if colonIdx := strings.Index(remainder, ":"); colonIdx > 0 {
				afterColon := remainder[colonIdx+1:]
				afterColon = strings.TrimSpace(afterColon)
				// Find the end of the property name
				endIdx := strings.IndexAny(afterColon, ",}")
				if endIdx > 0 {
					weightProp = strings.Trim(strings.TrimSpace(afterColon[:endIdx]), "'\"")
				}
			}
		}
	}

	// Run Louvain algorithm
	communities := e.computeLouvain(label, weightProp)

	// Build result
	rows := make([][]interface{}, 0, len(communities))
	for nodeID, communityID := range communities {
		node, err := e.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		rows = append(rows, []interface{}{e.nodeToMap(node), communityID})
	}

	return &ExecuteResult{
		Columns: []string{"node", "community"},
		Rows:    rows,
	}, nil
}

// computeLouvain implements the Louvain community detection algorithm.
func (e *StorageExecutor) computeLouvain(label, weightProp string) map[storage.NodeID]int {
	// Get all nodes
	var nodes []*storage.Node
	if label != "" {
		nodes, _ = e.storage.GetNodesByLabel(label)
	} else {
		nodes = e.storage.GetAllNodes()
	}

	if len(nodes) == 0 {
		return map[storage.NodeID]int{}
	}

	// Initialize: each node in its own community
	community := make(map[storage.NodeID]int)
	nodeIndex := make(map[storage.NodeID]int)
	for i, node := range nodes {
		community[node.ID] = i
		nodeIndex[node.ID] = i
	}

	// Build adjacency with weights
	type edge struct {
		target storage.NodeID
		weight float64
	}
	adj := make(map[storage.NodeID][]edge)
	totalWeight := 0.0

	for _, node := range nodes {
		edges := e.getNodeEdges(node.ID)
		for _, e := range edges {
			neighbor := e.EndNode
			if e.StartNode != node.ID {
				neighbor = e.StartNode
			}

			// Skip self-loops and non-existent neighbors
			if neighbor == node.ID {
				continue
			}
			if _, exists := nodeIndex[neighbor]; !exists {
				continue
			}

			weight := 1.0
			if weightProp != "" {
				if w, ok := e.Properties[weightProp]; ok {
					if wf, ok := toFloat64(w); ok {
						weight = wf
					}
				}
			}

			adj[node.ID] = append(adj[node.ID], edge{target: neighbor, weight: weight})
			totalWeight += weight
		}
	}

	if totalWeight == 0 {
		// No edges, each node is its own community
		return community
	}

	// Compute initial degree sums per community
	communityWeightSum := make(map[int]float64)
	for _, node := range nodes {
		comm := community[node.ID]
		for _, e := range adj[node.ID] {
			communityWeightSum[comm] += e.weight
		}
	}

	// Louvain Phase 1: Local optimization
	improved := true
	maxIterations := 10 // Prevent infinite loops

	for iter := 0; improved && iter < maxIterations; iter++ {
		improved = false

		for _, node := range nodes {
			currentComm := community[node.ID]

			// Calculate current node's connections to each community
			connToComm := make(map[int]float64)
			nodeDegree := 0.0
			for _, e := range adj[node.ID] {
				targetComm := community[e.target]
				connToComm[targetComm] += e.weight
				nodeDegree += e.weight
			}

			// Try moving to each neighbor's community
			bestComm := currentComm
			bestGain := 0.0

			// Remove node from current community for calculation
			communityWeightSum[currentComm] -= nodeDegree

			for _, e := range adj[node.ID] {
				targetComm := community[e.target]
				if targetComm == currentComm {
					continue
				}

				// Calculate modularity gain
				// ΔQ = [sum_in + k_i,in] / m - [(sum_tot + k_i) / 2m]^2
				//    - [sum_in / m - (sum_tot / 2m)^2 - (k_i / 2m)^2]
				// Simplified: focus on the key terms
				ki_in := connToComm[targetComm]
				sum_tot := communityWeightSum[targetComm]

				gain := ki_in - (sum_tot * nodeDegree / totalWeight)

				if gain > bestGain {
					bestGain = gain
					bestComm = targetComm
				}
			}

			// Restore and possibly update
			communityWeightSum[currentComm] += nodeDegree

			if bestComm != currentComm && bestGain > 0 {
				// Move node to best community
				communityWeightSum[currentComm] -= nodeDegree
				communityWeightSum[bestComm] += nodeDegree
				community[node.ID] = bestComm
				improved = true
			}
		}
	}

	// Renumber communities to be consecutive starting from 0
	communityMap := make(map[int]int)
	nextID := 0
	result := make(map[storage.NodeID]int)

	for nodeID, comm := range community {
		if newID, exists := communityMap[comm]; exists {
			result[nodeID] = newID
		} else {
			communityMap[comm] = nextID
			result[nodeID] = nextID
			nextID++
		}
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
func (e *StorageExecutor) callApocAlgoLabelPropagation(ctx context.Context, cypher string) (*ExecuteResult, error) {
	label := e.extractLabelFromAlgoCall(cypher, "LABELPROPAGATION")

	communities := e.computeLabelPropagation(label)

	rows := make([][]interface{}, 0, len(communities))
	for nodeID, communityID := range communities {
		node, err := e.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		rows = append(rows, []interface{}{e.nodeToMap(node), communityID})
	}

	return &ExecuteResult{
		Columns: []string{"node", "community"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) computeLabelPropagation(label string) map[storage.NodeID]int {
	var nodes []*storage.Node
	if label != "" {
		nodes, _ = e.storage.GetNodesByLabel(label)
	} else {
		nodes = e.storage.GetAllNodes()
	}

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

	// Renumber to consecutive IDs
	labelMap := make(map[int]int)
	nextID := 0
	result := make(map[storage.NodeID]int)

	for nodeID, lbl := range labels {
		if newID, exists := labelMap[lbl]; exists {
			result[nodeID] = newID
		} else {
			labelMap[lbl] = nextID
			result[nodeID] = nextID
			nextID++
		}
	}

	return result
}

// =============================================================================
// apoc.algo.wcc - Weakly Connected Components
// =============================================================================

// callApocAlgoWCC implements Weakly Connected Components detection.
// Syntax: CALL apoc.algo.wcc(['Label']) YIELD node, componentId
func (e *StorageExecutor) callApocAlgoWCC(ctx context.Context, cypher string) (*ExecuteResult, error) {
	label := e.extractLabelFromAlgoCall(cypher, "WCC")

	components := e.computeWCC(label)

	rows := make([][]interface{}, 0, len(components))
	for nodeID, componentID := range components {
		node, err := e.storage.GetNode(nodeID)
		if err != nil {
			continue
		}
		rows = append(rows, []interface{}{e.nodeToMap(node), componentID})
	}

	return &ExecuteResult{
		Columns: []string{"node", "componentId"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) computeWCC(label string) map[storage.NodeID]int {
	var nodes []*storage.Node
	if label != "" {
		nodes, _ = e.storage.GetNodesByLabel(label)
	} else {
		nodes = e.storage.GetAllNodes()
	}

	if len(nodes) == 0 {
		return map[storage.NodeID]int{}
	}

	// Union-Find structure
	parent := make(map[storage.NodeID]storage.NodeID)
	rank := make(map[storage.NodeID]int)

	for _, node := range nodes {
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
	for _, node := range nodes {
		edges := e.getNodeEdges(node.ID)
		for _, edge := range edges {
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

	for _, node := range nodes {
		root := find(node.ID)
		if id, exists := componentMap[root]; exists {
			result[node.ID] = id
		} else {
			componentMap[root] = nextID
			result[node.ID] = nextID
			nextID++
		}
	}

	return result
}
