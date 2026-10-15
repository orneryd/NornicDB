package louvain

import (
	"reflect"
	"testing"
)

// GDS 2.13.12's directed and undirected seeded projections differ because
// parallel arcs are reduced per community and total volume reduces degrees.
func TestRunParallelInfluenceAndDegreeVolume(t *testing.T) {
	for _, undirected := range []bool{false, true} {
		for _, reserved := range []bool{false, true} {
			counts := []int{4, 1, 0, 0}
			want := []int{0, 1, 1, 1}
			if undirected {
				counts = []int{4, 3, 2, 1}
				want = []int{0, 1, 0, 1}
			}
			var graph *Graph
			if reserved {
				graph = NewGraph(4, counts)
			} else {
				graph = NewGraph(4)
			}
			for _, edge := range [][3]float64{{0, 1, 1e16}, {0, 2, 1}, {0, 2, 1}, {0, 3, -1e16}, {1, 1, 1}} {
				i, j := int(edge[0]), int(edge[1])
				if undirected {
					graph.Link(i, j, edge[2])
				} else {
					graph.Arc(i, j, edge[2])
				}
			}
			options := DefaultOptions()
			options.Seed = []int{0, 1, 1, 1}
			if got := Final(Run(graph, options)); !reflect.DeepEqual(want, got) {
				t.Fatalf("undirected=%v reserved=%v: got %v, GDS %v", undirected, reserved, got, want)
			}
		}
	}
}
