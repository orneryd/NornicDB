package louvain

import (
	"reflect"
	"testing"
)

func TestRunUndirectedSelfLoopsParallelAndExcludedEndpoints(t *testing.T) {
	edges := [][3]int{{0, 1, 1}, {0, 1, 1}, {0, 3, 2}, {1, 2, 2}, {2, 2, 1}, {2, 2, 1}, {2, 99, 10}}
	input := func(i int) (int, int, float64, bool) {
		edge := edges[i]
		return edge[0], edge[1], float64(edge[2]), edge[1] < 4
	}
	result := RunUndirected(4, len(edges), input, DefaultOptions())
	if want, got := []int{0, 1, 1, 0}, Final(result); !reflect.DeepEqual(want, got) {
		t.Fatalf("got %v, want %v", got, want)
	}
	graph := NewGraph(1)
	graph.Link(0, 0, 2)
	graph.Link(0, 0, 3)
	if graph.degree[0] != 10 || graph.total != 0 || len(graph.arcs[0]) != 4 {
		t.Fatalf("self-loop degree/arc contributions: %+v", graph)
	}
}

func TestColourClassesSentinelBoundary(t *testing.T) {
	for _, size := range []int{8, 1002} {
		neighbours := make([][]int, size)
		for i := range neighbours {
			for j := 0; j < size; j++ {
				if i != j {
					neighbours[i] = append(neighbours[i], j)
				}
			}
		}
		classes := NewGraph(size).colourClasses(neighbours)
		if len(classes) != size {
			t.Fatalf("%d nodes: %d classes", size, len(classes))
		}
		for _, class := range classes {
			if len(class) != 1 {
				t.Fatalf("clique colour collision: %v", class)
			}
		}
		if size == 1002 && (classes[1000][0] != 1001 || classes[1001][0] != 1000) {
			t.Fatal("GDS sentinel colouring changed")
		}
	}
}
