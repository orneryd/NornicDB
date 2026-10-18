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

// RunUndirected is the shared APOC input adapter and engine. Each indexed
// relationship is one physical relationship, including parallel relationships
// and self-loops. The callback supplies stable endpoints in [0,size), weight,
// and whether both endpoints belong to the selected graph. It is read twice
// to reserve one adjacency buffer, without copying an adapter's input graph.
// Only Graph.Link expands a relationship into its two degree contributions.
func RunUndirected(size, relationshipCount int, relationship func(int) (int, int, float64, bool), options Options) Result {
	counts := make([]int, size)
	for i := 0; i < relationshipCount; i++ {
		source, target, _, included := relationship(i)
		if included {
			counts[source]++
			counts[target]++
		}
	}
	graph := NewGraph(size, counts)
	for i := 0; i < relationshipCount; i++ {
		source, target, weight, included := relationship(i)
		if included {
			graph.Link(source, target, weight)
		}
	}
	return Run(graph, options)
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
	scratch := newWorkspace(size)
	membership := identityCommunities(size)
	representative := identityCommunities(size)
	initial := options.Seed
	previous := 0.0
	for level := 0; level < options.MaxLevels; level++ {
		communities := graph.moveNodes(options.MaxIterations, options.Tolerance, initial, options.Resolution, scratch)
		initial = nil
		modularity := graph.modularityScratch(communities, options.Resolution, scratch.inside, scratch.totals)
		var representatives []int
		numbered := scratch.numbers[:len(graph.arcs)]
		clear(numbered)
		for _, c := range communities {
			if numbered[c] == 0 {
				numbered[c] = 1
				representatives = append(representatives, representative[c])
			}
		}
		count := renumberDense(communities, numbered)
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
		graph = graph.aggregate(communities, count, scratch)
		representative = representatives
	}
	return run
}

// Dense node/community IDs allow one run-local workspace, reused as levels
// shrink. No buffers are shared between runs or retained on the input graph.
type workspace struct {
	community, next, stamps, candidates, heads, links, counts, numbers []int
	communityDegree, influence, inside, totals, weights                []float64
}

func newWorkspace(size int) *workspace {
	integers := make([]int, 8*size)
	floats := make([]float64, 5*size)
	return &workspace{
		community: integers[:size], next: integers[size : 2*size], stamps: integers[2*size : 3*size], candidates: integers[3*size : 4*size],
		heads: integers[4*size : 5*size], links: integers[5*size : 6*size], counts: integers[6*size : 7*size], numbers: integers[7*size:],
		communityDegree: floats[:size], influence: floats[size : 2*size], inside: floats[2*size : 3*size], totals: floats[3*size : 4*size], weights: floats[4*size:],
	}
}

func renumberDense(community, numbers []int) int {
	for i := range numbers {
		numbers[i] = -1
	}
	count := 0
	for i, c := range community {
		if numbers[c] == -1 {
			numbers[c] = count
			count++
		}
		community[i] = numbers[c]
	}
	return count
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
	return g.modularityScratch(community, resolution, inside, degree)
}

func (g *Graph) modularityScratch(community []int, resolution float64, inside, degree []float64) float64 {
	inside, degree = inside[:len(g.arcs)], degree[:len(g.arcs)]
	clear(inside)
	clear(degree)
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
func (g *Graph) moveNodes(maxIterations int, tolerance float64, initial []int, resolution float64, scratch *workspace) []int {
	size := len(g.arcs)
	community := scratch.community[:size]
	communityDegree := scratch.communityDegree[:size]
	clear(communityDegree)
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
	next := scratch.next[:size]
	// Reuse influence storage across nodes and passes. Candidate scores are
	// independent; the explicit positive-gain/minimum-ID rule resolves ties.
	influence := scratch.influence[:size]
	stamps := scratch.stamps[:size]
	clear(stamps)
	stamp := 0
	totalSquared := math.Pow(g.total, 2)
	for iteration := 0; iteration < maxIterations; iteration++ {
		moves := 0
		for _, class := range classes {
			for _, i := range class {
				current := community[i]
				// The weight from i to each neighbouring community (its own
				// self-loops apart).
				stamp++
				candidates := scratch.candidates[:0]
				self := 0.0
				for _, arc := range g.arcs[i] {
					if arc.target == i {
						self += arc.weight
					}
					c := community[arc.target]
					if stamps[c] != stamp {
						stamps[c], influence[c] = stamp, 0
						candidates = append(candidates, c)
					}
					influence[c] += arc.weight
				}
				// GDS's modularity gain of i leaving its community for c,
				// computed as GDS computes it: a node moves only for a
				// positive gain, to the smaller community on a tie.
				ki := g.degree[i]
				eix := 0.0 - self
				if stamps[current] == stamp {
					eix = influence[current] - self
				}
				ax := communityDegree[current] - ki
				best, maxGain := current, 0.0
				for _, c := range candidates {
					weight := influence[c]
					if c == current {
						continue
					}
					observed := weight - eix
					if resolution != 1 {
						observed *= resolution
					}
					gain := observed/(g.total/2) + (2*ki*ax-2*ki*communityDegree[c])/totalSquared
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
		after := g.modularityScratch(community, resolution, scratch.inside, scratch.totals)
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
	forbidden := make([]int, len(colour)+1)
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
				// On small graphs 1000 cannot be selected as a colour, so
				// its sentinel needs no slot in the forbidden-colour array.
				if j != i && colour[j] < len(forbidden) {
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
func (g *Graph) aggregate(community []int, count int, scratch *workspace) *Graph {
	heads, links := scratch.heads[:count], scratch.links[:len(g.arcs)]
	for i := range heads {
		heads[i] = -1
	}
	// Group source nodes without reordering the reductions of their arcs.
	for i := len(g.arcs) - 1; i >= 0; i-- {
		c := community[i]
		links[i], heads[c] = heads[c], i
	}
	stamps, counts := scratch.stamps[:count], scratch.counts[:count]
	clear(stamps)
	clear(counts)
	for source := range heads {
		for node := heads[source]; node >= 0; node = links[node] {
			for _, arc := range g.arcs[node] {
				target := community[arc.target]
				if stamps[target] != source+1 {
					stamps[target] = source + 1
					counts[source]++
				}
			}
		}
	}
	next := NewGraph(count, counts)
	clear(stamps)
	weights := scratch.weights[:count]
	for source := range heads {
		ordered := scratch.candidates[:0]
		for node := heads[source]; node >= 0; node = links[node] {
			for _, arc := range g.arcs[node] {
				target := community[arc.target]
				if stamps[target] != source+1 {
					stamps[target], weights[target] = source+1, 0
					ordered = append(ordered, target)
				}
				weights[target] += arc.weight
			}
			next.degree[source] += g.degree[node]
		}
		sort.Ints(ordered)
		for _, target := range ordered {
			next.arcs[source] = append(next.arcs[source], weightedArc{target: target, weight: weights[target]})
		}
	}
	next.total = g.total
	return next
}
