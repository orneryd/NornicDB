// Package cypher implements APOC community detection algorithms.
package cypher

import (
	"context"
	"sort"
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
	graph, nodes, config, err := e.readAlgorithmGraph("apoc.algo.louvain", args)
	if err != nil {
		return nil, err
	}
	// The config map may name the relationship weight property.
	weightProp, _ := config["weightProperty"].(string)

	// Run Louvain algorithm
	communities := graph.computeLouvain(nodes, weightProp)

	// Build result
	rows := graphAlgorithmRows(nodes, communities)

	return &ExecuteResult{
		Columns: []string{"node", "community"},
		Rows:    rows,
	}, nil
}

// computeLouvain is Louvain community detection (#1184), as GDS's
// gds.louvain computes it over an undirected graph: the relationships
// between the given nodes, parallel ones adding their weights (weightProp's
// value, 1 without it). Each level moves nodes, in the given order, to the
// neighbouring community with the best modularity gain until no move
// helps (at most louvainMaxIterations passes), then merges each community
// into one node; levels repeat until a level moves nothing (at most
// louvainMaxLevels). Communities are numbered by their first node.
func (e *algorithmGraphExecutor) computeLouvain(nodes []algorithmGraphNode, weightProp string) map[graphEntityKey]int {
	if len(nodes) == 0 {
		return map[graphEntityKey]int{}
	}
	index := make(map[graphEntityKey]int, len(nodes))
	unique := make([]algorithmGraphNode, 0, len(nodes))
	for _, node := range nodes {
		if _, listed := index[node.ID]; !listed {
			index[node.ID] = len(unique)
			unique = append(unique, node)
		}
	}
	graph := newLouvainGraph(len(unique))
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
			graph.link(i, j, weight)
		}
	}

	// membership[i] is node i's community at the current level.
	membership := make([]int, len(unique))
	for i := range membership {
		membership[i] = i
	}
	for level := 0; level < louvainMaxLevels; level++ {
		communities, moved := graph.moveNodes()
		if !moved {
			break
		}
		count := renumberLouvainCommunities(communities)
		for i := range membership {
			membership[i] = communities[membership[i]]
		}
		graph = graph.aggregate(communities, count)
	}

	result := make(map[graphEntityKey]int, len(unique))
	for i, node := range unique {
		result[node.ID] = membership[i]
	}
	return renumberInNodeOrder(unique, result)
}

const (
	louvainMaxLevels     = 10 // GDS's maxLevels default
	louvainMaxIterations = 10 // GDS's maxIterations default
)

// louvainGraph is one Louvain level's undirected weighted graph. weights[i]
// maps each neighbour j to the weight between i and j; a self-loop is
// counted twice (weights[i][i] = 2w), so degree[i] is the sum of weights[i]
// and total, the sum of the degrees, is twice the graph's weight.
type louvainGraph struct {
	weights []map[int]float64
	degree  []float64
	total   float64
}

func newLouvainGraph(size int) *louvainGraph {
	g := &louvainGraph{weights: make([]map[int]float64, size), degree: make([]float64, size)}
	for i := range g.weights {
		g.weights[i] = make(map[int]float64)
	}
	return g
}

// link adds a relationship of weight w between i and j.
func (g *louvainGraph) link(i, j int, w float64) {
	if i == j {
		g.weights[i][i] += 2 * w
		g.degree[i] += 2 * w
	} else {
		g.weights[i][j] += w
		g.weights[j][i] += w
		g.degree[i] += w
		g.degree[j] += w
	}
	g.total += 2 * w
}

// moveNodes is a level's local moving phase: each node in turn joins the
// neighbouring community with the largest modularity gain, kept by its own
// community on a tie and by the smaller community between others, until a
// pass moves nothing. It reports each node's community and whether any
// node moved.
func (g *louvainGraph) moveNodes() ([]int, bool) {
	size := len(g.weights)
	community := make([]int, size)
	communityDegree := make([]float64, size)
	for i := range community {
		community[i] = i
		communityDegree[i] = g.degree[i]
	}
	if g.total == 0 {
		return community, false
	}
	neighbours := g.neighbours()
	moved := false
	for iteration := 0; iteration < louvainMaxIterations; iteration++ {
		moves := 0
		for i := 0; i < size; i++ {
			current := community[i]
			communityDegree[current] -= g.degree[i]
			// The weight from i to each neighbouring community.
			links := make(map[int]float64)
			for _, j := range neighbours[i] {
				if j != i {
					links[community[j]] += g.weights[i][j]
				}
			}
			candidates := make([]int, 0, len(links))
			for c := range links {
				candidates = append(candidates, c)
			}
			sort.Ints(candidates)
			// The gain of i joining c, up to a constant factor:
			// k_i,in(c) - Σtot(c)·k_i / 2m.
			gain := func(c int) float64 {
				return links[c] - communityDegree[c]*g.degree[i]/g.total
			}
			best, bestGain := current, gain(current)
			for _, c := range candidates {
				if candidateGain := gain(c); candidateGain > bestGain {
					best, bestGain = c, candidateGain
				}
			}
			communityDegree[best] += g.degree[i]
			if best != current {
				community[i] = best
				moves++
			}
		}
		if moves == 0 {
			break
		}
		moved = true
	}
	return community, moved
}

// neighbours lists each node's neighbours in order, so sums over them don't
// depend on map order.
func (g *louvainGraph) neighbours() [][]int {
	lists := make([][]int, len(g.weights))
	for i, weights := range g.weights {
		lists[i] = make([]int, 0, len(weights))
		for j := range weights {
			lists[i] = append(lists[i], j)
		}
		sort.Ints(lists[i])
	}
	return lists
}

// renumberLouvainCommunities numbers communities 0, 1, … by their first
// node, in place, and returns how many there are.
func renumberLouvainCommunities(community []int) int {
	numbers := make(map[int]int)
	for i, c := range community {
		number, ok := numbers[c]
		if !ok {
			number = len(numbers)
			numbers[c] = number
		}
		community[i] = number
	}
	return len(numbers)
}

// aggregate is the next level's graph: one node per community, the weights
// between communities summed, and a community's inner weight its
// self-loop.
func (g *louvainGraph) aggregate(community []int, count int) *louvainGraph {
	next := newLouvainGraph(count)
	for i, neighbours := range g.neighbours() {
		for _, j := range neighbours {
			next.weights[community[i]][community[j]] += g.weights[i][j]
		}
		next.degree[community[i]] += g.degree[i]
	}
	next.total = g.total
	return next
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
	graph, nodes, _, err := e.readAlgorithmGraph("apoc.algo.labelPropagation", args)
	if err != nil {
		return nil, err
	}

	communities := graph.computeLabelPropagation(nodes)

	rows := graphAlgorithmRows(nodes, communities)

	return &ExecuteResult{
		Columns: []string{"node", "community"},
		Rows:    rows,
	}, nil
}

func (e *algorithmGraphExecutor) computeLabelPropagation(nodes []algorithmGraphNode) map[graphEntityKey]int {

	if len(nodes) == 0 {
		return map[graphEntityKey]int{}
	}

	// Initialize: each node gets its own label
	labels := make(map[graphEntityKey]int)
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
	graph, nodes, _, err := e.readAlgorithmGraph("apoc.algo.wcc", args)
	if err != nil {
		return nil, err
	}

	components := graph.computeWCC(nodes)

	rows := graphAlgorithmRows(nodes, components)

	return &ExecuteResult{
		Columns: []string{"node", "componentId"},
		Rows:    rows,
	}, nil
}

func (e *algorithmGraphExecutor) computeWCC(nodes []algorithmGraphNode) map[graphEntityKey]int {

	if len(nodes) == 0 {
		return map[graphEntityKey]int{}
	}

	// Union-Find structure
	parent := make(map[graphEntityKey]graphEntityKey)
	rank := make(map[graphEntityKey]int)

	for _, node := range nodes {
		parent[node.ID] = node.ID
		rank[node.ID] = 0
	}

	// Find with path compression
	var find func(x graphEntityKey) graphEntityKey
	find = func(x graphEntityKey) graphEntityKey {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}

	// Union by rank
	union := func(x, y graphEntityKey) {
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
	componentMap := make(map[graphEntityKey]int)
	nextID := 0
	result := make(map[graphEntityKey]int)

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
