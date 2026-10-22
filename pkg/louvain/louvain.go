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
	// Stop, when set, is polled once per stopStride node visits and input
	// relationships; once it reports true the run ends early and its result
	// is incomplete (the caller's statement was canceled).
	Stop func() bool
}

// stopStride is the node visits, or input relationships, between two polls
// of Options.Stop.
const stopStride = 1024

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
	// GDS reduces outgoing degrees, rather than reducing every arc into one
	// running total. These differ under signed floating-point cancellation.
	graph.total = 0
	for _, degree := range graph.degree {
		graph.total += degree
	}
	if options.Resolution == 0 {
		options.Resolution = 1
	}
	var run Result
	size := len(graph.arcs)
	poll := &stopPoll{stop: options.Stop}
	membership := identityCommunities(size)
	representative := identityCommunities(size)
	initial := options.Seed
	previous := 0.0
	for level := 0; level < options.MaxLevels; level++ {
		communities := graph.moveNodes(options.MaxIterations, options.Tolerance, initial, options.Resolution, poll)
		if poll.stopped {
			return Result{}
		}
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
		if count == len(graph.arcs) || count == 1 || converged {
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

// stopPoll is Options.Stop, polled once per stopStride node visits
// (visits); stopped records that it reported true.
type stopPoll struct {
	stop    func() bool
	visits  int
	stopped bool
}

// halt polls stop at the stride and reports whether the run must end.
func (poll *stopPoll) halt() bool {
	if poll.stop == nil {
		return false
	}
	poll.visits++
	if poll.visits&(stopStride-1) == 0 && poll.stop() {
		poll.stopped = true
	}
	return poll.stopped
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
	inside := make([]float64, len(g.arcs))
	degree := make([]float64, len(g.arcs))
	for i, arcs := range g.arcs {
		degree[community[i]] += g.degree[i]
		for _, arc := range arcs {
			if community[i] == community[arc.target] {
				inside[community[i]] += arc.weight
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
	arcs   [][]weightedArc
	degree []float64
	total  float64
}

type weightedArc struct {
	target int
	weight float64
}

// NewGraph creates a graph with size nodes and no relationships. Optional
// per-node arc counts reserve one backing array; they affect allocation only.
// Counts must contain size nonnegative entries, including both arcs of a Link.
func NewGraph(size int, arcCounts ...[]int) *Graph {
	graph := &Graph{arcs: make([][]weightedArc, size), degree: make([]float64, size)}
	if len(arcCounts) > 0 {
		total := 0
		for _, count := range arcCounts[0] {
			total += count
		}
		storage := make([]weightedArc, total)
		offset := 0
		for source, count := range arcCounts[0] {
			graph.arcs[source] = storage[offset : offset : offset+count]
			offset += count
		}
	}
	return graph
}

// Arc adds a relationship of weight w from i to j only, as a directed
// projection holds it (an undirected one adds both ways).
func (g *Graph) Arc(i, j int, w float64) {
	// Keep individual arcs in traversal order. Summing parallel targets here
	// changes floating-point community influence when seeded targets coincide.
	g.arcs[i] = append(g.arcs[i], weightedArc{target: j, weight: w})
	g.degree[i] += w
}

// Link adds a relationship of weight w between i and j.
func (g *Graph) Link(i, j int, w float64) {
	g.Arc(i, j, w)
	g.Arc(j, i, w)
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
func (g *Graph) moveNodes(maxIterations int, tolerance float64, initial []int, resolution float64, poll *stopPoll) []int {
	size := len(g.arcs)
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
	// Reuse influence storage across nodes and passes. Candidate scores are
	// independent; the explicit positive-gain/minimum-ID rule resolves ties.
	influence := make(map[int]float64)
	for iteration := 0; iteration < maxIterations; iteration++ {
		moves := 0
		for _, class := range classes {
			for _, i := range class {
				if poll.halt() {
					return community
				}
				current := community[i]
				// The weight from i to each neighbouring community (its own
				// self-loops apart).
				clear(influence)
				self := 0.0
				for _, arc := range g.arcs[i] {
					if arc.target == i {
						self += arc.weight
					}
					influence[community[arc.target]] += arc.weight
				}
				// GDS's modularity gain of i leaving its community for c,
				// computed as GDS computes it: a node moves only for a
				// positive gain, to the smaller community on a tie.
				ki := g.degree[i]
				eix := influence[current] - self
				ax := communityDegree[current] - ki
				best, maxGain := current, 0.0
				for c, weight := range influence {
					if c == current {
						continue
					}
					observed := weight - eix
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
	colour := make([]int, len(g.arcs))
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

// neighbours lists distinct targets in node order for colouring. Numeric
// reductions consume the original ordered arcs, including parallel arcs.
func (g *Graph) neighbours() [][]int {
	lists := make([][]int, len(g.arcs))
	for i, arcs := range g.arcs {
		list := make([]int, len(arcs))
		for j, arc := range arcs {
			list[j] = arc.target
		}
		sort.Ints(list)
		for _, target := range list {
			if len(lists[i]) == 0 || lists[i][len(lists[i])-1] != target {
				list[len(lists[i])] = target
				lists[i] = list[:len(lists[i])+1]
			}
		}
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
	weights := make([]map[int]float64, count)
	for i := range weights {
		weights[i] = make(map[int]float64)
	}
	for i, arcs := range g.arcs {
		for _, arc := range arcs {
			weights[community[i]][community[arc.target]] += arc.weight
		}
		next.degree[community[i]] += g.degree[i]
	}
	for source, targets := range weights {
		ordered := make([]int, 0, len(targets))
		for target := range targets {
			ordered = append(ordered, target)
		}
		sort.Ints(ordered)
		next.arcs[source] = make([]weightedArc, len(ordered))
		for i, target := range ordered {
			next.arcs[source][i] = weightedArc{target: target, weight: targets[target]}
		}
	}
	next.total = g.total
	return next
}
