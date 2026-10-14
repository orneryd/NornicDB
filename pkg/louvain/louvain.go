// Package louvain provides the shared weighted Louvain engine used by GDS and APOC.
package louvain

import (
	"math"
	"sort"
)

// Options specifies a run's limits, GDS's maxLevels, maxIterations
// and tolerance (a level or pass gaining less modularity ends the run),
// and the nodes' initial communities (Seed; nil: each its own). Limits must be positive.
type Options struct {
	MaxLevels, MaxIterations int
	Tolerance                float64
	Seed                     []int
	// Resolution scales observed community weight; zero uses one.
	Resolution float64
}

// DefaultOptions returns the GDS and apoc.algo.louvain defaults.
func DefaultOptions() Options {
	return Options{MaxLevels: 10, MaxIterations: 10, Tolerance: 0.0001, Resolution: 1}
}

// Result is a Louvain run's result: each level's community of every
// node, numbered by first node; each level's representative of each
// community, the node whose community it grew from (GDS names a community
// by it); and the modularity after each level.
type Result struct {
	Levels          [][]int
	Representatives [][]int
	Modularities    []float64
}

// Run runs Louvain over graph as GDS runs it: each level moves
// nodes (the tolerance ending its passes), then merges each community into
// one node. A level that leaves as many communities as nodes, or a single
// one, or gains no more modularity than the tolerance over the level
// before, ends the run and, unless it is the first, is dropped; at most
// MaxLevels levels run.
func Run(graph *Graph, options Options) Result {
	if options.Resolution == 0 {
		options.Resolution = 1
	}
	var run Result
	size := len(graph.weights)
	membership := identityCommunities(size)
	representative := identityCommunities(size)
	initial := options.Seed
	previous := 0.0
	for level := 0; level < options.MaxLevels; level++ {
		communities := graph.moveNodes(options.MaxIterations, options.Tolerance, initial, options.Resolution)
		initial = nil
		modularity := graph.modularity(communities, options.Resolution)
		var representatives []int
		numbered := make(map[int]bool)
		for _, c := range communities {
			if !numbered[c] {
				numbered[c] = true
				representatives = append(representatives, representative[c])
			}
		}
		count := Renumber(communities)
		for i := range membership {
			membership[i] = communities[membership[i]]
		}
		run.Levels = append(run.Levels, append([]int(nil), membership...))
		run.Representatives = append(run.Representatives, representatives)
		run.Modularities = append(run.Modularities, modularity)
		converged := level > 0 && !(modularity > previous && modularity-previous > options.Tolerance)
		if count == len(graph.weights) || count == 1 || converged {
			if level > 0 {
				last := len(run.Levels) - 1
				run.Levels, run.Representatives, run.Modularities = run.Levels[:last], run.Representatives[:last], run.Modularities[:last]
			}
			break
		}
		previous = modularity
		graph = graph.aggregate(communities, count)
		representative = representatives
	}
	return run
}

// Final is each node's community after the run's last level (a run
// always has one).
func Final(run Result) []int {
	return run.Levels[len(run.Levels)-1]
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
func (g *Graph) modularity(community []int, resolution float64) float64 {
	inside := make([]float64, len(g.weights))
	degree := make([]float64, len(g.weights))
	for i, weights := range g.weights {
		degree[community[i]] += g.degree[i]
		for j, w := range weights {
			if community[i] == community[j] {
				inside[community[i]] += w
			}
		}
	}
	q := 0.0
	inverseTotal := 1.0 / g.total
	for c, d := range degree {
		observed := inside[c]
		if resolution != 1 {
			observed *= resolution
		}
		// Keep GDS's multiplication order, including underflow and overflow.
		q += observed - d*d*inverseTotal
	}
	return q * inverseTotal
}

// Graph holds one level's weighted adjacency. Arc preserves a projected
// direction; Link adds both directions and counts a self-loop twice. A
// node's degree is its outgoing weight, and total is the sum of degrees.
type Graph struct {
	weights []map[int]float64
	degree  []float64
	total   float64
}

// NewGraph creates a graph with size nodes and no relationships.
func NewGraph(size int) *Graph {
	g := &Graph{weights: make([]map[int]float64, size), degree: make([]float64, size)}
	for i := range g.weights {
		g.weights[i] = make(map[int]float64)
	}
	return g
}

// Arc adds a relationship of weight w from i to j only, as a directed
// projection holds it (an undirected one adds both ways).
func (g *Graph) Arc(i, j int, w float64) {
	g.weights[i][j] += w
	g.degree[i] += w
	g.total += w
}

// Link adds a relationship of weight w between i and j.
func (g *Graph) Link(i, j int, w float64) {
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
// more modularity than the Tolerance, or MaxIterations have run. Nodes
// start in initial's communities (nil: each its own). It reports each
// node's community.
func (g *Graph) moveNodes(maxIterations int, tolerance float64, initial []int, resolution float64) []int {
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
		// Zero total weight can contain nonzero signed degrees. Its IEEE
		// gains may be positive infinity, so those graphs still run. When
		// every degree is zero, every gain is NaN and no node can move.
		allZero := true
		for _, degree := range g.degree {
			if degree != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			return community
		}
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
					observed := influence[c] - eix
					if resolution != 1 {
						observed *= resolution
					}
					gain := observed/(g.total/2) + (2*ki*ax-2*ki*communityDegree[c])/math.Pow(g.total, 2)
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
		after := g.modularity(community, resolution)
		if iteration > 0 && !(after > before && after-before > tolerance) {
			break
		}
		before = after
	}
	return community
}

// colourClasses follows GDS's serial K1 colouring: colour the pending nodes,
// validate outgoing conflicts, and recolour their sources. Modularity
// optimization caps this at five rounds independently of MaxIterations.
// Self-loops never forbid a colour. Classes and their nodes are in order.
func (g *Graph) colourClasses(neighbours [][]int) [][]int {
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
func (g *Graph) neighbours() [][]int {
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

// Renumber numbers communities 0, 1, … by their first
// node, in place, and returns how many there are.
func Renumber(community []int) int {
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
func (g *Graph) aggregate(community []int, count int) *Graph {
	next := NewGraph(count)
	for i, neighbours := range g.neighbours() {
		for _, j := range neighbours {
			next.weights[community[i]][community[j]] += g.weights[i][j]
		}
		next.degree[community[i]] += g.degree[i]
	}
	next.total = g.total
	return next
}
