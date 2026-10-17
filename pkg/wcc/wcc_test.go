package wcc

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestForestNamesComponentsByLowestNode(t *testing.T) {
	// 0-3-5, 1-4, 2 alone, 6-7 joined twice, 8 with a self-loop.
	f := New(9)
	for _, edge := range [][2]int{{5, 3}, {3, 0}, {4, 1}, {7, 6}, {6, 7}, {8, 8}} {
		f.Union(edge[0], edge[1])
	}
	if got, want := f.Lowest(), []int{0, 1, 2, 0, 1, 0, 6, 6, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lowest() = %v, want %v", got, want)
	}
	for node, want := range []int{3, 2, 1, 3, 2, 3, 2, 2, 1} {
		if got := f.Size(node); got != want {
			t.Fatalf("Size(%d) = %d, want %d", node, got, want)
		}
	}
}

func TestForestEmpty(t *testing.T) {
	if got := New(0).Lowest(); len(got) != 0 {
		t.Fatalf("Lowest() = %v, want none", got)
	}
}

// The names are the same whatever order the unions run in.
func TestForestLowestIndependentOfUnionOrder(t *testing.T) {
	const n = 2000
	random := rand.New(rand.NewSource(7))
	edges := make([][2]int, 1500)
	for i := range edges {
		edges[i] = [2]int{random.Intn(n), random.Intn(n)}
	}
	var want []int
	for round := 0; round < 5; round++ {
		random.Shuffle(len(edges), func(i, j int) { edges[i], edges[j] = edges[j], edges[i] })
		f := New(n)
		for _, edge := range edges {
			if round%2 == 0 {
				f.Union(edge[0], edge[1])
			} else {
				f.Union(edge[1], edge[0])
			}
		}
		got := f.Lowest()
		if want == nil {
			want = got
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: names differ", round)
		}
	}
	// Every name is its component's lowest member.
	for node, name := range want {
		if name > node || want[name] != name {
			t.Fatalf("node %d named %d", node, name)
		}
	}
}

// A long chain neither recurses nor degrades: one component of every node.
func TestForestLongChain(t *testing.T) {
	const n = 1 << 20
	f := New(n)
	for i := n - 1; i > 0; i-- {
		f.Union(i, i-1)
	}
	lowest := f.Lowest()
	if lowest[n-1] != 0 || f.Size(n/2) != n {
		t.Fatalf("chain: lowest %d, size %d", lowest[n-1], f.Size(n/2))
	}
}
