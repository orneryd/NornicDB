// Package cypher implements APOC community detection algorithms.
package cypher

import (
	"context"
	"math"
	"sort"

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
// between the given nodes, parallel ones adding their weights (weightProp's
// value, 1 without it). Each level moves nodes, in the given order, to the
// neighbouring community with the best modularity gain until no move
// helps, then merges each community into one node; levels repeat until a
// level moves nothing (runLouvain, apocLouvainOptions). Communities are
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

	membership := louvainFinal(runLouvain(graph, apocLouvainOptions))
	result := make(map[storage.NodeID]int, len(unique))
	for i, node := range unique {
		result[node.ID] = membership[i]
	}
	return renumberInNodeOrder(unique, result)
}

// louvainOptions are a Louvain run's limits, GDS's maxLevels, maxIterations
// and tolerance (a level or pass gaining less modularity ends the run),
// and the nodes' initial communities (seed; nil: each its own).
type louvainOptions struct {
	maxLevels, maxIterations int
	tolerance                float64
	seed                     []int
}

// apocLouvainOptions are apoc.algo.louvain's: GDS's defaults (#1184).
var apocLouvainOptions = louvainOptions{maxLevels: 10, maxIterations: 10, tolerance: 0.0001}

// louvainRun is a Louvain run's result: each level's community of every
// node, numbered by first node; each level's representative of each
// community, the node whose community it grew from (GDS names a community
// by it); and the modularity after each level.
type louvainRun struct {
	levels          [][]int
	representatives [][]int
	modularities    []float64
}

// runLouvain runs Louvain over graph as GDS runs it: each level moves
// nodes (the tolerance ending its passes), then merges each community into
// one node. A level that leaves as many communities as nodes, or a single
// one, or gains no more modularity than the tolerance over the level
// before, ends the run and, unless it is the first, is dropped; at most
// maxLevels levels run.
func runLouvain(graph *louvainGraph, options louvainOptions) louvainRun {
	var run louvainRun
	size := len(graph.weights)
	membership := identityCommunities(size)
	representative := identityCommunities(size)
	initial := options.seed
	previous := 0.0
	for level := 0; level < options.maxLevels; level++ {
		communities := graph.moveNodes(options.maxIterations, options.tolerance, initial)
		initial = nil
		modularity := graph.modularity(communities)
		var representatives []int
		numbered := make(map[int]bool)
		for _, c := range communities {
			if !numbered[c] {
				numbered[c] = true
				representatives = append(representatives, representative[c])
			}
		}
		count := renumberLouvainCommunities(communities)
		for i := range membership {
			membership[i] = communities[membership[i]]
		}
		run.levels = append(run.levels, append([]int(nil), membership...))
		run.representatives = append(run.representatives, representatives)
		run.modularities = append(run.modularities, modularity)
		converged := level > 0 && !(modularity > previous && modularity-previous > options.tolerance)
		if count == len(graph.weights) || count == 1 || converged {
			if level > 0 {
				last := len(run.levels) - 1
				run.levels, run.representatives, run.modularities = run.levels[:last], run.representatives[:last], run.modularities[:last]
			}
			break
		}
		previous = modularity
		graph = graph.aggregate(communities, count)
		representative = representatives
	}
	return run
}

// louvainFinal is each node's community after the run's last level (a run
// always has one).
func louvainFinal(run louvainRun) []int {
	return run.levels[len(run.levels)-1]
}

func identityCommunities(size int) []int {
	communities := make([]int, size)
	for i := range communities {
		communities[i] = i
	}
	return communities
}

// modularity is the graph's modularity for communities: Σ over communities
// of in/2m − (tot/2m)², a self-loop counted twice.
func (g *louvainGraph) modularity(community []int) float64 {
	if g.total == 0 {
		return 0
	}
	inside := make(map[int]float64)
	degree := make(map[int]float64)
	for i, weights := range g.weights {
		degree[community[i]] += g.degree[i]
		for j, w := range weights {
			if community[i] == community[j] {
				inside[community[i]] += w
			}
		}
	}
	q := 0.0
	for c, d := range degree {
		q += inside[c]/g.total - (d/g.total)*(d/g.total)
	}
	return q
}

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

// arc adds a relationship of weight w from i to j only, as a directed
// projection holds it (an undirected one adds both ways).
func (g *louvainGraph) arc(i, j int, w float64) {
	g.weights[i][j] += w
	g.degree[i] += w
	g.total += w
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

// moveNodes is a level's local moving phase, as GDS's modularity
// optimization runs it (#1198): serial K1 colouring determines node order.
// Each pass takes the colour classes in turn, every node of a class joining
// the neighbouring community
// with the largest modularity gain over the communities as they stood
// before the class (its own kept on a tie, else the smaller community).
// Passes repeat until one moves nothing, a pass after the first gains no
// more modularity than the tolerance, or maxIterations have run. Nodes
// start in initial's communities (nil: each its own). It reports each
// node's community.
func (g *louvainGraph) moveNodes(maxIterations int, tolerance float64, initial []int) []int {
	size := len(g.weights)
	community := make([]int, size)
	communityDegree := make([]float64, size)
	for i := range community {
		community[i] = i
		if initial != nil {
			community[i] = initial[i]
		}
	}
	for i := range community {
		communityDegree[community[i]] += g.degree[i]
	}
	if g.total == 0 {
		return community
	}
	neighbours := g.neighbours()
	classes := g.colourClasses(neighbours)
	before := 0.0
	next := make([]int, size)
	for iteration := 0; iteration < maxIterations; iteration++ {
		moves := 0
		for _, class := range classes {
			for _, i := range class {
				current := community[i]
				// The weight from i to each neighbouring community (its own
				// self-loops apart).
				influence := make(map[int]float64)
				self := 0.0
				for _, j := range neighbours[i] {
					if j == i {
						self += g.weights[i][j]
					}
					influence[community[j]] += g.weights[i][j]
				}
				candidates := make([]int, 0, len(influence))
				for c := range influence {
					candidates = append(candidates, c)
				}
				sort.Ints(candidates)
				// GDS's modularity gain of i leaving its community for c,
				// computed as GDS computes it: a node moves only for a
				// positive gain, to the smaller community on a tie.
				ki := g.degree[i]
				eix := influence[current] - self
				ax := communityDegree[current] - ki
				best, maxGain := current, 0.0
				for _, c := range candidates {
					if c == current {
						continue
					}
					gain := (influence[c]-eix)/(g.total/2) + (2*ki*ax-2*ki*communityDegree[c])/math.Pow(g.total, 2)
					if gain > maxGain || (gain == maxGain && gain != 0 && best > c) {
						best, maxGain = c, gain
					}
				}
				next[i] = best
			}
			for _, i := range class {
				if best := next[i]; best != community[i] {
					communityDegree[community[i]] -= g.degree[i]
					communityDegree[best] += g.degree[i]
					community[i] = best
					moves++
				}
			}
		}
		if moves == 0 {
			break
		}
		// The tolerance compares each pass with the one before; the first
		// pass is always followed by a second, as in GDS.
		after := g.modularity(community)
		if iteration > 0 && !(after > before && after-before > tolerance) {
			break
		}
		before = after
	}
	return community
}

// colourClasses follows GDS's serial K1 colouring: colour the pending nodes,
// validate outgoing conflicts, and recolour their sources. Modularity
// optimization caps this at five rounds independently of maxIterations.
// Self-loops never forbid a colour. Classes and their nodes are in order.
func (g *louvainGraph) colourClasses(neighbours [][]int) [][]int {
	colour := make([]int, len(g.weights))
	pending := make([]bool, len(colour))
	next := make([]bool, len(colour))
	// GDS's initial colour also forbids 1000 before a neighbour is visited.
	forbidden := make([]int, max(len(colour)+1, 1001))
	stamp := 0
	for i := range colour {
		colour[i], pending[i] = 1000, true
	}
	for round := 0; round < 5; round++ {
		for i := range colour {
			if !pending[i] {
				continue
			}
			stamp++
			for _, j := range neighbours[i] {
				if j != i {
					forbidden[colour[j]] = stamp
				}
			}
			c := 0
			for forbidden[c] == stamp {
				c++
			}
			colour[i] = c
		}
		clear(next)
		conflicts := false
		for i := range colour {
			if !pending[i] {
				continue
			}
			for _, j := range neighbours[i] {
				if i != j && colour[i] == colour[j] && !next[j] {
					next[i], conflicts = true, true
					break
				}
			}
		}
		if !conflicts {
			break
		}
		pending, next = next, pending
	}
	var classes [][]int
	for i, c := range colour {
		for c >= len(classes) {
			classes = append(classes, nil)
		}
		classes[c] = append(classes[c], i)
	}
	return classes
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

func (e *StorageExecutor) computeWCC(nodes []*storage.Node) map[storage.NodeID]int {

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
