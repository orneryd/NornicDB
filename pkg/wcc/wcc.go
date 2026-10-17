// Package wcc provides the shared union-find behind NornicDB's weakly
// connected components: apoc.algo.wcc, gds.wcc.stream and gds.wcc.stats, and
// node similarity's useComponents.
package wcc

// Forest is a disjoint-set forest over the nodes 0 … n-1: union by size with
// path halving, so any sequence of unions and finds is near-linear. Callers
// build their own graph input (a projection's selected relationships, or the
// stored relationships between given nodes) and union its relationships'
// ends, which reads every relationship as undirected.
type Forest struct {
	parent []int
	size   []int
}

// New returns a forest of n nodes, each its own component.
func New(n int) *Forest {
	f := &Forest{parent: make([]int, n), size: make([]int, n)}
	for i := range f.parent {
		f.parent[i], f.size[i] = i, 1
	}
	return f
}

// Find is the root of node's component.
func (f *Forest) Find(node int) int {
	for f.parent[node] != node {
		f.parent[node] = f.parent[f.parent[node]]
		node = f.parent[node]
	}
	return node
}

// Union joins the components of a and b.
func (f *Forest) Union(a, b int) {
	a, b = f.Find(a), f.Find(b)
	if a == b {
		return
	}
	if f.size[a] < f.size[b] {
		a, b = b, a
	}
	f.parent[b] = a
	f.size[a] += f.size[b]
}

// Size is the number of nodes in node's component.
func (f *Forest) Size(node int) int {
	return f.size[f.Find(node)]
}

// Lowest names every node's component by its lowest node, as GDS names a
// component. The names depend only on the components, not on the order of
// the unions that formed them.
func (f *Forest) Lowest() []int {
	first := make([]int, len(f.parent)) // root -> its component's lowest node, plus one
	lowest := make([]int, len(f.parent))
	for node := range lowest {
		root := f.Find(node)
		if first[root] == 0 {
			first[root] = node + 1
		}
		lowest[node] = first[root] - 1
	}
	return lowest
}
