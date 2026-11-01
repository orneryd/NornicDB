// Package louvain provides the shared weighted Louvain engine used by GDS and APOC.
package louvain

import (
	"context"
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

// checkStride bounds the steps between context checks inside a scan whose
// length the graph controls: a node's arcs, a high-degree node's candidate
// communities, a node's neighbours. A stopped run finishes at most that many
// steps of the scan in flight. It must be a power of two.
const checkStride = 1024

// strideCheck reports ctx's error on every checkStride-th step of such a scan.
// It reads nothing the run computes, so it cannot change a result.
func strideCheck(ctx context.Context, step int) error {
	if step&(checkStride-1) != 0 {
		return nil
	}
	return ctx.Err()
}

// RunUndirected is the shared APOC input adapter and engine. Each indexed
// relationship is one physical relationship, including parallel relationships
// and self-loops. The callback supplies stable endpoints in [0,size), weight,
// and whether both endpoints belong to the selected graph. It is read twice
// to reserve one adjacency buffer, without copying an adapter's input graph.
// Only Graph.Link expands a relationship into its two degree contributions.
// Both passes check ctx before each relationship, and the adapter checks it
// again before reserving the adjacency buffer, so a stopped run performs no
// further adaptation or reservation and returns the zero Result with ctx's
// error.
func RunUndirected(ctx context.Context, size, relationshipCount int, relationship func(int) (int, int, float64, bool), options Options) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	counts := make([]int, size)
	for i := 0; i < relationshipCount; i++ {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		source, target, _, included := relationship(i)
		if included {
			counts[source]++
			counts[target]++
		}
	}
	// The caller can stop the run from inside the last relationship of the
	// counting pass; reserving the adjacency buffer first would do the whole
	// graph's allocation work after the stop.
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	graph := NewGraph(size, counts)
	for i := 0; i < relationshipCount; i++ {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		source, target, weight, included := relationship(i)
		if included {
			graph.Link(source, target, weight)
		}
	}
	return Run(ctx, graph, options)
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
// MaxLevels levels run. Every level, pass, colour class, node sweep and output
// boundary checks ctx, and every scan whose length the graph controls (a
// node's arcs, a high-degree node's candidates) checks it once per stride, so
// a stopped run returns within bounded work with the zero Result and ctx's
// error; no partial partition is ever returned.
func Run(ctx context.Context, graph *Graph, options Options) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	// GDS reduces outgoing degrees, rather than reducing every arc into one
	// running total. These differ under signed floating-point cancellation.
	graph.total = 0
	for _, degree := range graph.degree {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		graph.total += degree
	}
	if options.Resolution == 0 {
		options.Resolution = 1
	}
	var run Result
	size := len(graph.arcs)
	scratch := newWorkspace(size)
	membership, err := identityCommunities(ctx, size)
	if err != nil {
		return Result{}, err
	}
	representative, err := identityCommunities(ctx, size)
	if err != nil {
		return Result{}, err
	}
	initial := options.Seed
	previous := 0.0
	for level := 0; level < options.MaxLevels; level++ {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		communities, err := graph.moveNodes(ctx, options.MaxIterations, options.Tolerance, initial, options.Resolution, scratch)
		if err != nil {
			return Result{}, err
		}
		initial = nil
		modularity, err := graph.modularityScratch(ctx, communities, options.Resolution, scratch.inside, scratch.totals)
		if err != nil {
			return Result{}, err
		}
		var representatives []int
		numbered := scratch.numbers[:len(graph.arcs)]
		clear(numbered)
		for _, c := range communities {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
			if numbered[c] == 0 {
				numbered[c] = 1
				representatives = append(representatives, representative[c])
			}
		}
		count, err := renumberDense(ctx, communities, numbered)
		if err != nil {
			return Result{}, err
		}
		// The level's membership and the level it publishes carry the same
		// values, so one bounded, checked pass writes both; the published copy
		// keeps the previous level's slice and the empty graph's nil level.
		var published []int
		if nodes := len(membership); nodes > 0 {
			published = make([]int, nodes)
		}
		for i := range membership {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
			membership[i] = communities[membership[i]]
			published[i] = membership[i]
		}
		run.Levels = append(run.Levels, published)
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
		graph, err = graph.aggregate(ctx, communities, count, scratch)
		if err != nil {
			return Result{}, err
		}
		representative = representatives
	}
	return run, nil
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

func renumberDense(ctx context.Context, community, numbers []int) (int, error) {
	for i := range numbers {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		numbers[i] = -1
	}
	count := 0
	for i, c := range community {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if numbers[c] == -1 {
			numbers[c] = count
			count++
		}
		community[i] = numbers[c]
	}
	return count, nil
}

// Final is each node's community after the run's last level (a run
// always has one).
func Final(run Result) []int {
	return run.Levels[len(run.Levels)-1]
}

func identityCommunities(ctx context.Context, size int) ([]int, error) {
	communities := make([]int, size)
	for i := range communities {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		communities[i] = i
	}
	return communities, nil
}

// modularityScratch computes the graph's modularity for communities: Σ over communities
// of in/2m − (tot/2m)², a self-loop counted twice.
func (g *Graph) modularityScratch(ctx context.Context, community []int, resolution float64, inside, degree []float64) (float64, error) {
	inside, degree = inside[:len(g.arcs)], degree[:len(g.arcs)]
	clear(inside)
	clear(degree)
	for i, arcs := range g.arcs {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		degree[community[i]] += g.degree[i]
		for ai, arc := range arcs {
			if err := strideCheck(ctx, ai); err != nil {
				return 0, err
			}
			if community[i] == community[arc.target] {
				inside[community[i]] += arc.weight
			}
		}
	}
	q := 0.0
	inverseTotal := 1.0 / g.total
	for c, d := range degree {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		observed := inside[c]
		if resolution != 1 {
			observed *= resolution
		}
		// Keep GDS's multiplication order, including underflow and overflow.
		q += observed - d*d*inverseTotal
	}
	return q * inverseTotal, nil
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
// node's community. Each class, node and pass checks ctx, a node's arcs and
// candidates check it once per stride, and a stopped pass returns ctx's error
// instead of a community.
func (g *Graph) moveNodes(ctx context.Context, maxIterations int, tolerance float64, initial []int, resolution float64, scratch *workspace) ([]int, error) {
	size := len(g.arcs)
	community := scratch.community[:size]
	communityDegree := scratch.communityDegree[:size]
	clear(communityDegree)
	for i := range community {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		community[i] = i
		if initial != nil {
			community[i] = initial[i]
		}
	}
	for i := range community {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		communityDegree[community[i]] += g.degree[i]
	}
	if g.total == 0 {
		// Zero total weight can contain nonzero signed degrees. Its IEEE
		// gains may be positive infinity, so those graphs still run. When
		// every degree is zero, every gain is NaN and no node can move.
		allZero := true
		for _, degree := range g.degree {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if degree != 0 {
				allZero = false
				break
			}
		}
		if allZero {
			return community, nil
		}
	}
	neighbours, err := g.neighbours(ctx)
	if err != nil {
		return nil, err
	}
	classes, err := g.colourClasses(ctx, neighbours)
	if err != nil {
		return nil, err
	}
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		moves := 0
		for _, class := range classes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for _, i := range class {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				current := community[i]
				// The weight from i to each neighbouring community (its own
				// self-loops apart).
				stamp++
				candidates := scratch.candidates[:0]
				self := 0.0
				for ai, arc := range g.arcs[i] {
					if err := strideCheck(ctx, ai); err != nil {
						return nil, err
					}
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
				for ci, c := range candidates {
					if err := strideCheck(ctx, ci); err != nil {
						return nil, err
					}
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
				if err := ctx.Err(); err != nil {
					return nil, err
				}
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
		after, err := g.modularityScratch(ctx, community, resolution, scratch.inside, scratch.totals)
		if err != nil {
			return nil, err
		}
		if iteration > 0 && !(after > before && after-before > tolerance) {
			break
		}
		before = after
	}
	return community, nil
}

// colourClasses follows GDS's serial K1 colouring: colour the pending nodes,
// validate outgoing conflicts, and recolour their sources. Modularity
// optimization caps this at five rounds independently of MaxIterations.
// Self-loops never forbid a colour. Classes and their nodes are in order.
// Each pending node and round checks ctx, and a node's neighbours once per
// stride.
func (g *Graph) colourClasses(ctx context.Context, neighbours [][]int) ([][]int, error) {
	colour := make([]int, len(g.arcs))
	pending := make([]bool, len(colour))
	next := make([]bool, len(colour))
	// GDS's initial colour also forbids 1000 before a neighbour is visited.
	forbidden := make([]int, len(colour)+1)
	stamp := 0
	for i := range colour {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		colour[i], pending[i] = 1000, true
	}
	for round := 0; round < 5; round++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for i := range colour {
			if !pending[i] {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			stamp++
			for ni, j := range neighbours[i] {
				if err := strideCheck(ctx, ni); err != nil {
					return nil, err
				}
				// On small graphs 1000 cannot be selected as a colour, so
				// its sentinel needs no slot in the forbidden-colour array.
				if j != i && colour[j] < len(forbidden) {
					forbidden[colour[j]] = stamp
				}
			}
			c := 0
			for forbidden[c] == stamp {
				if err := strideCheck(ctx, c); err != nil {
					return nil, err
				}
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
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for ni, j := range neighbours[i] {
				if err := strideCheck(ctx, ni); err != nil {
					return nil, err
				}
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for c >= len(classes) {
			classes = append(classes, nil)
		}
		classes[c] = append(classes[c], i)
	}
	return classes, nil
}

// neighbours lists distinct targets in node order for colouring. Numeric
// reductions consume the original ordered arcs, including parallel arcs.
// Each node and each stride of its arcs checks ctx.
func (g *Graph) neighbours(ctx context.Context) ([][]int, error) {
	lists := make([][]int, len(g.arcs))
	for i, arcs := range g.arcs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		list := make([]int, len(arcs))
		for ai, arc := range arcs {
			if err := strideCheck(ctx, ai); err != nil {
				return nil, err
			}
			list[ai] = arc.target
		}
		sort.Ints(list)
		for ai, target := range list {
			if err := strideCheck(ctx, ai); err != nil {
				return nil, err
			}
			if len(lists[i]) == 0 || lists[i][len(lists[i])-1] != target {
				list[len(lists[i])] = target
				lists[i] = list[:len(lists[i])+1]
			}
		}
	}
	return lists, nil
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
// self-loop. Each grouped node and each stride of its arcs checks ctx.
func (g *Graph) aggregate(ctx context.Context, community []int, count int, scratch *workspace) (*Graph, error) {
	heads, links := scratch.heads[:count], scratch.links[:len(g.arcs)]
	for i := range heads {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		heads[i] = -1
	}
	// Group source nodes without reordering the reductions of their arcs.
	for i := len(g.arcs) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c := community[i]
		links[i], heads[c] = heads[c], i
	}
	stamps, counts := scratch.stamps[:count], scratch.counts[:count]
	clear(stamps)
	clear(counts)
	for source := range heads {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for node := heads[source]; node >= 0; node = links[node] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for ai, arc := range g.arcs[node] {
				if err := strideCheck(ctx, ai); err != nil {
					return nil, err
				}
				target := community[arc.target]
				if stamps[target] != source+1 {
					stamps[target] = source + 1
					counts[source]++
				}
			}
		}
	}
	// A stop the counting pass observed must not reserve the next level.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	next := NewGraph(count, counts)
	clear(stamps)
	weights := scratch.weights[:count]
	for source := range heads {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ordered := scratch.candidates[:0]
		for node := heads[source]; node >= 0; node = links[node] {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for ai, arc := range g.arcs[node] {
				if err := strideCheck(ctx, ai); err != nil {
					return nil, err
				}
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
		for oi, target := range ordered {
			if err := strideCheck(ctx, oi); err != nil {
				return nil, err
			}
			next.arcs[source] = append(next.arcs[source], weightedArc{target: target, weight: weights[target]})
		}
	}
	next.total = g.total
	return next, nil
}
